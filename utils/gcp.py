from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage
from googleapiclient.discovery import build
import pandas as pd
import general_utils
import os
import socket

class Gcp:
    """
    gcp class contains the connections for gcp and includes the upload method
    for crawler data
    """
    
    def __init__(self, gcp_config_path, bucket_name):
        self.gcp_config_path = gcp_config_path
        self.bucket_name = bucket_name
        self.credentials = service_account.Credentials.from_service_account_file(gcp_config_path)
        self.bqclient = self.get_bq_client()
        self.storage_client = self.get_storage_client()
        self.bucket = self.get_bucket()
        self.gsheets = self.get_sheets_service()
        self.gdrive = self.get_drive_service()


    def get_storage_client(self):
        #Hack to overcome the timeout error 
        storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024* 1024  # 5 MB
        storage.blob._MAX_MULTIPART_SIZE = 5 * 1024* 1024  # 5 MB
        storage_client = storage.Client(credentials=self.credentials,
                                        project=self.credentials.project_id)
        return storage_client


    def get_bq_client(self):
        bqclient = bigquery.Client(credentials=self.credentials,
                                   project=self.credentials.project_id)
        print(f'connected to BigQuery, project: {bqclient.project} on version {bigquery.__version__}')
        return bqclient


    def get_bucket(self):
        bucket = self.storage_client.get_bucket(self.bucket_name)
        print('connected to Google Storage, bucket:', bucket.name)
        return bucket

    def get_drive_service(self):
        service = build('drive', 'v3', credentials=self.credentials)
        print('connected to gdrive')
        return service
    
    def get_sheets_service(self):
        socket.setdefaulttimeout(300)
        service = build('sheets', 'v4', credentials=self.credentials)
        sheet = service.spreadsheets()
        print('connected to sheets')
        return sheet


    def df_to_gs(self, df, bucket, dest_file_path: str, verbose=True):
        """
        exports and uploads DataFrame as specified file-format to Google Cloud Storage

        TODO: Remove all os stuff and use tempfile.NamedTemporaryFile
        """
        if os.name == 'nt':
            os.makedirs('C:/tmp_gs/', exist_ok=True)
            tmp_path = 'C:/tmp_gs/'
        elif os.name == 'posix':
            home = os.path.expanduser('~')
            os.makedirs(os.path.abspath(os.path.join(home, 'tmp_gs')), exist_ok=True)
            tmp_path = os.path.abspath(os.path.join(home, 'tmp_gs'))

        tmp_file = dest_file_path.split('/')[-1]
        dest_path = dest_file_path.replace(tmp_file, '')
        tmp_path_file = os.path.join(tmp_path, tmp_file)

        if verbose: print('saving DataFrame, shape', str(df.shape).rjust(15), 'to file', end='... ')

        if tmp_path_file.endswith('.csv.gz'):
            df.to_csv(tmp_path_file, float_format='%.12g', index=False, compression='gzip')
        elif tmp_path_file.endswith('.csv'):
            df.to_csv(tmp_path_file, float_format='%.12g', index=False)
        elif tmp_path_file.endswith('.json'):
            df.to_json(tmp_path_file, date_format='iso')
        elif tmp_path_file.endswith('.pkl'):
            df.to_pickle(tmp_path_file, protocol=4)
        elif tmp_path_file.endswith('.parquet'):
            try:
                df.to_parquet(tmp_path_file, index=False, skipna=False)
            except:
                df.to_parquet(tmp_path_file, index=False)
            if verbose: print('no index', end=', ')
        else:
            raise Exception('no valid file type (.csv, .csv.gz, .json, .pkl, .parquet)')
        print('done.', end='\t')

        self.upload_gs_blob(bucket, local_file_path=tmp_path_file, dest_path=dest_path)
        os.remove(tmp_path_file)


    def upload_to_gcs(self,
                      df: pd.DataFrame,
                      path_: str,
                      prefix: str,
                      extension: str,
                      destination: str='',
                      timestamp: bool=True):
        
        if destination:
            path_ = path_ + destination + '/'
        else:
            path_ = path_

        if timestamp:
            ts_ = general_utils.get_datetime_str()
            file_name = prefix + '_' + ts_ + '.' + extension
        else:
            file_name = prefix + '.' + extension

        self.df_to_gs(df = df, bucket = self.bucket, dest_file_path = path_+file_name)


    def get_latest_file_path_in_bucket(self,
                                       path: str,
                                       extension: str,
                                       source: str='',
                                       file_start: str='')->str:
        """
        get path of last uploaded file in bucket. Requires {name}{ts}.{extension} fromat
        of bucket files
        """
        if source:
            prefix = path+source+'/'
        else:
            prefix = path
        blobs = self.bucket.list_blobs(prefix=prefix + file_start,
                                    fields='items(name)')
        bucket_parquets = [blob.name for blob in blobs if blob.name.endswith(f'.{extension}')]
        latest_raw = max(bucket_parquets)

        print(f'latest raw {extension} in source:{latest_raw}')
        return latest_raw


    def get_latest_file_contents_in_bucket(self,
                                           path: str,
                                           extension: str,
                                           source: str,
                                           file_start: str = '')->pd.DataFrame:
        """
        import latest competitor data as data frame 
        """
        path_ = self.get_latest_file_path_in_bucket(path = path,
                                        extension = extension,
                                        source = source,
                                        file_start = file_start)

        df = self.gs_to_df(bucket = self.bucket,
                     source_file_path = path_)
        return df


    def gs_to_df(self, bucket, source_file_path: str, verbose=True):
        """
        Downloads DataFrame as specified file-format from Google Cloud Storage
        """
        if os.name == 'nt':
            os.makedirs('C:/tmp_gs/', exist_ok=True)
            tmp_path = 'C:/tmp_gs/'
        else:
            home = os.path.expanduser('~')
            os.makedirs(os.path.abspath(os.path.join(home, 'tmp_gs')), exist_ok=True)
            tmp_path = os.path.abspath(os.path.join(home, 'tmp_gs'))

        tmp_file = source_file_path.split('/')[-1]
        tmp_path_file = os.path.join(tmp_path, tmp_file)

        self.download_gs_blob(bucket, local_path=tmp_path, blob_file_path=source_file_path)

        if tmp_path_file.endswith('.csv.gz'):
            df = pd.read_csv(tmp_path_file, compression='gzip')
            if verbose: print('Created DataFrame from {} file with shape: {}'.format('csv gz', df.shape))
        elif tmp_path_file.endswith('.csv'):
            df = pd.read_csv(tmp_path_file)
            if verbose: print('Created DataFrame from {} file with shape: {}'.format('csv', df.shape))
        elif tmp_path_file.endswith('.json'):
            df = pd.read_json(tmp_path_file)
            if verbose: print('Created DataFrame from {} file with shape: {}'.format('json', df.shape))
        elif tmp_path_file.endswith('.pkl'):
            df = pd.read_pickle(tmp_path_file)
            if verbose: print('Created DataFrame from {} file with shape: {}'.format('pkl', df.shape))
        elif tmp_path_file.endswith('.parquet'):
            df = pd.read_parquet(tmp_path_file)
            if verbose: print('Created DataFrame from {} file with shape: {}'.format('parquet', df.shape))
        else:
            raise Exception("Error: incorrect file format")

        if verbose: print('done.', end='\t')

        os.remove(tmp_path_file)
        return df


    def download_gs_blob(self, bucket, local_path:str, blob_file_path:str, verbose=True):
        """
        Downloads a blob from the bucket to a local path
        """

        # add '/' to path if not given, create if not existing
        if not local_path.endswith('/'):
            local_path += '/'
        os.makedirs(local_path, exist_ok=True)

        # create local file path
        blob_file_name  = blob_file_path.split('/')[-1]
        local_file_path = os.path.join(local_path, blob_file_name)

        blob = bucket.get_blob(blob_file_path)
        blob.download_to_filename(local_file_path)

        if verbose:
            print('Blob downloaded to:', local_file_path,
                  '| size:', general_utils.convert_bytes(blob.size),
                  '| last modified:', str(blob.updated)[:19]
                  )


    def upload_gs_blob(self, bucket, local_file_path:str, dest_path:str, verbose=True):
        """
        Uploads a file to the bucket.
        """
        if not dest_path.endswith('/'):
            dest_path += '/'

        local_file_name = local_file_path.split('/')[-1]
        destination_file_path = dest_path + local_file_name

        blob = bucket.blob(destination_file_path)
        if verbose:
            print(f'Uploading {local_file_path} ({general_utils.get_file_size(local_file_path)}) to {dest_path}', end='... ')
        blob.upload_from_filename(local_file_path)
        if verbose:
            print('done.')


    def model_to_gs(self, model, bucket, dest_file_path: str):
        """
        exports and uploads model objects as specified file-format to Google Cloud Storage
        """
        if os.name == 'nt':
            os.makedirs('C:/tmp_gs/', exist_ok=True)
            tmp_path = 'C:/tmp_gs/'
        elif os.name == 'posix':
            home = os.path.expanduser('~')
            os.makedirs(os.path.abspath(os.path.join(home, 'tmp_gs')), exist_ok=True)
            tmp_path = os.path.abspath(os.path.join(home, 'tmp_gs'))
        tmp_file = dest_file_path.split('/')[-1]
        dest_path = dest_file_path.replace(tmp_file, '')
        tmp_path_file = os.path.join(tmp_path, tmp_file)
        joblib.dump(model, tmp_path_file)
        self.upload_gs_blob(bucket, local_file_path=tmp_path_file, dest_path=dest_path)
        os.remove(tmp_path_file)



    def get_model_from_gs(self, bucket, source_file_path: str):
        """
        gets model objects from google buckets
        """
        if os.name == 'nt':
            os.makedirs('C:/tmp_gs/', exist_ok=True)
            tmp_path = 'C:/tmp_gs/'
        else:
            home = os.path.expanduser('~')
            os.makedirs(os.path.abspath(os.path.join(home, 'tmp_gs')), exist_ok=True)
            tmp_path = os.path.abspath(os.path.join(home, 'tmp_gs'))
        tmp_file = source_file_path.split('/')[-1]
        tmp_path_file = os.path.join(tmp_path, tmp_file)
        self.download_gs_blob(bucket, local_path=tmp_path, blob_file_path=source_file_path)
        model = joblib.load(tmp_path_file)
        os.remove(tmp_path_file)
        return model
    

