import pandas as pd
import numpy as np
import datetime
import os
import sqlalchemy

from tqdm.auto import tqdm

###############################################################################
##### general functions #######################################################
###############################################################################


def get_datetime_str(up_to='second'):
    """
    up_to: second, minute, hour, day
    """
    if up_to == 'second':
        s = str(datetime.datetime.now())[0:19]
    elif up_to == 'minute':
        s = str(datetime.datetime.now())[0:16]
    elif up_to == 'hour':
        s = str(datetime.datetime.now())[0:13]
    elif up_to == 'day':
        s = str(datetime.datetime.now())[0:10]
    else:
        raise Exception('no valid value')
    s = s.replace('-', '').replace(' ', '_').replace(':', '')
    return s


def is_notebook():
    "check if running inside a notebook"
    # try:
    #     from google import colab
    #     return True
    # except:
    #     pass
    try:
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True   # Jupyter notebook, Spyder or qtconsole
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


def convert_bytes(num) -> str:
    "return human-readable string with MB, GB, etc"
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.2f %s" % (num, x)
        num /= 1024.0


def get_file_size(file_path) -> str:
    "return the size of a local file as formatted string"
    if os.path.isfile(file_path):
        file_info = os.stat(file_path)
        return convert_bytes(file_info.st_size)


###############################################################################
##### pandas functions ########################################################
###############################################################################
def set_pd_options():
    options = {
        'display': {
            'max_columns': None,
            'max_colwidth': 35,
            'expand_frame_repr': False,  # Don't wrap to multiple pages
            'max_rows': 200,
            'max_seq_items': 50,         # Max length of printed sequence
            'precision': 6,
            'show_dimensions': False
        },
        # 'mode': {
        #     'chained_assignment': None   # Controls SettingWithCopyWarning
        # }
    }

    for category, option in options.items():
        for op, value in option.items():
            pd.set_option(f'{category}.{op}', value)  # Python 3.6+
    print('pandas options updated')


def display_df(df, level=1):
    """
    display dataframe with rotated column names
    """
    from IPython.core.display import display, HTML

    style = """
    <style>
    th.rotate {height: 140px; white-space: nowrap;
    }

    th.rotate > div {transform: translate(25px, 51px) rotate(315deg); width: 30px;
    }

    th.rotate > div > span {border-bottom: 1px solid #ccc;  padding: 5px 10px;
    }
    </style>
    """
    dfhtml = style + df.to_html()

    try:
        colnames = df.columns.get_level_values(level).values
    except IndexError:
        colnames = df.columns.values

    for name in colnames:
        dfhtml = dfhtml.replace(
            f'<th>{name}</th>', f'<th class="rotate"><div><span>{name}</span></div></th>')

    display(HTML(dfhtml))


def df_mem_usage(df):
    "return string with memory usage of dataframe in human readable format (KB, MB, GB, ...)"
    mem_usage_str = convert_bytes(df.memory_usage().sum())
    return mem_usage_str


def memory_usage(df_or_series) -> str:
    """
    Returns the size of a DataFrame or Series in readable format.
    """
    if type(df_or_series) == pd.core.frame.DataFrame:
        size = round(df_or_series.memory_usage(index=True, deep=True).sum(), 2)
    elif type(df_or_series) == pd.core.frame.Series:
        size = round(df_or_series.memory_usage(index=True, deep=True), 2)

    return convert_bytes(size)


def downcast_numeric_columns(df, columns=[], verbose=False) -> pd.DataFrame:
    """
    Downcast all passed columns to most efficient numeric type.
    """
    numeric_columns = df.loc[:, columns].select_dtypes(
        'number').columns.tolist()
    int_columns = df.loc[:, columns].select_dtypes('int').columns.tolist()
    float_columns = df.loc[:, columns].select_dtypes('float').columns.tolist()

    max_string_length = max([len(col) for col in numeric_columns])+2

    for col in numeric_columns:
        if verbose:
            print("downcasting:", col.ljust(max_string_length),
                  'from', memory_usage(df[col]).rjust(8), end=' ')

        if col in int_columns:
            df[col] = pd.to_numeric(df[col], downcast="integer")
        elif col in float_columns:
            df[col] = pd.to_numeric(df[col], downcast="float")

        if verbose:
            print(memory_usage(df[col]).rjust(8))

    return df


def df_shape(df) -> str:
    "return string of shape with cleaner format, e.g. (10,000 x 24)"
    shape_str = f'({df.shape[0]:,} x {df.shape[1]})'
    return shape_str


def df_info(df, nunique=True, incl_min=True, incl_max=True, mem_usage=True, incl_total=False) -> pd.DataFrame:
    "show general info about df, more detailed than df.info()"
    info_details = pd.DataFrame(index=df.columns)

    info_details['dtype'] = df.dtypes
    if mem_usage:
        info_details['memory_mb'] = np.round(
            df.memory_usage(deep=True)/1_000_000, 2)

    if nunique:
        info_details['nunique'] = df.nunique()

    info_details['notnull'] = df.notnull().sum()
    info_details['isnull'] = df.isnull().sum()
    info_details['isnull_%'] = np.round(df.isnull().sum()*100 / len(df), 2)

    if incl_min:
        info_details['min'] = df.min()
    if incl_max:
        info_details['max'] = df.max()

    info_details = info_details.reset_index(
        drop=False).rename(columns={'index': 'column'})

    if incl_total and mem_usage == True:
        info_details = info_details.append(pd.DataFrame(data={'column': 'TOTAL',
                                                              'memory_mb': info_details['memory_mb'].sum()
                                                              },
                                                        index=[len(info_details)])
                                           )

    return info_details


def display_value_counts(ser, head=20) -> pd.DataFrame:
    "display value counts as formatted dataframe"
    vc = ser.value_counts(dropna=False)
    vs_df = pd.DataFrame(vc)
    vs_df['%'] = np.round(vc/vc.sum(), 4)*100

    vs_df = (vs_df
             .reset_index()
             .rename(columns={ser.name: 'count',
                              'index': ser.name
                              })
             .head(head)
             )

    s = vs_df.style.format({'count': '{:,}',
                            '%':    '{:,.2%}'})

    return vs_df


def info_catg(row, top=10):
    null = row.isnull().sum()
    v_counts = row.value_counts()
    n_unique = row.nunique()
    appears_only_once = (v_counts == 1).sum()

    print(f"'{row.name}':")
    print('null:     ', null)
    print('n_unique: ', n_unique)
    print('only once:', appears_only_once)

    if top > 0:
        print('\nvalue counts top 10:')
        print(str(v_counts.head(top)).split('\nName')[0])

###############################################################################
##### AWS input/output functions ##############################################
###############################################################################


def sql_query_from_placeholders(query: str, placeholders: dict) -> str:
    if query.endswith('.sql'):
        query_str = open(query, 'r').read()  # .replace('%', '%%')
    else:
        query_str = query

    query_str = query_str.format(**placeholders)

    return query_str


def load_sql(query: str, conn, verbose=True, parse_dates: list = None) -> pd.DataFrame:
    if query.endswith('.sql'):
        query_string = open(query, 'r').read().replace('%', '%%')
    else:
        query_string = query

    if verbose:
        print(f'loading {query[:50]}...', end='')
    start = datetime.datetime.now()

    df = pd.read_sql(query_string, conn, parse_dates=parse_dates)

    end = datetime.datetime.now()
    dur_s = (end-start).seconds + (end-start).microseconds/1_000_000
    if verbose:
        print(
            f'done with shape: {df_shape(df)} in {dur_s:.2f}s with size {df_mem_usage(df)}')

    return df


def df_to_s3(df, bucket, bucket_file_path: str, verbose=True):
    "exports and uploads DataFrame as specified file-format to S3"
    # create temp folder
    if os.name == 'nt':
        os.makedirs('C:/tmp_s3/', exist_ok=True)
        dir_tmp_s3 = 'C:/tmp_s3/'
    elif os.name == 'posix':
        home = os.path.expanduser("~")
        os.makedirs(os.path.abspath(
            os.path.join(home, 'tmp_s3')), exist_ok=True)
        dir_tmp_s3 = os.path.abspath(os.path.join(home, 'tmp_s3'))

    file_name = bucket_file_path.split('/')[-1]
    tmp_folder_file = os.path.join(dir_tmp_s3, file_name)

    # save local file
    if verbose:
        print(f'saving DataFrame {df_shape(df)} to file:', end='... ')
    if file_name.endswith(".csv"):
        df.to_csv(tmp_folder_file, float_format="%.12g", index=False)
    elif file_name.endswith(".csv.gz"):
        df.to_csv(tmp_folder_file, float_format="%.12g",
                  index=False, compression="gzip")
    elif file_name.endswith(".pkl"):
        df.to_pickle(tmp_folder_file)
    elif file_name.endswith(".parquet"):
        df.to_parquet(tmp_folder_file, index=False)
    elif file_name.endswith(".json"):
        df.to_json(tmp_folder_file, date_format='iso')
    else:
        raise Exception("no valid file type")

    if verbose:
        print(f'done with size: {get_file_size(tmp_folder_file)}')

    # upload progress bar
    if verbose:
        pbar = tqdm(total=os.stat(tmp_folder_file).st_size, desc='uploading',
                    bar_format="{desc}: {percentage:.2f}%|{bar}| [{elapsed}<{remaining}]")

    def load_progress(chunk):
        if verbose:
            pbar.update(chunk)
        else:
            pass

    # local -> S3
    bucket.upload_file(Filename=tmp_folder_file,
                       Key=bucket_file_path, Callback=load_progress)
    if verbose:
        pbar.close()

    # remove local file
    os.remove(tmp_folder_file)


def s3_to_df(bucket, bucket_file_path, verbose=True):
    "returns DataFrame after downloading file from S3"
    # create temp folder
    if os.name == 'nt':
        os.makedirs('C:/tmp_s3/', exist_ok=True)
        dir_tmp_s3 = 'C:/tmp_s3/'
    elif os.name == 'posix':
        home = os.path.expanduser("~")
        os.makedirs(os.path.abspath(
            os.path.join(home, 'tmp_s3')), exist_ok=True)
        dir_tmp_s3 = os.path.abspath(os.path.join(home, 'tmp_s3'))

    file_name = bucket_file_path.split('/')[-1]
    tmp_folder_file = os.path.join(dir_tmp_s3, file_name)

    # download and status
    size = bucket.Object(bucket_file_path).content_length
    if verbose:
        if size > 15_000_000:
            pbar = tqdm(total=size, desc=f'downloading {convert_bytes(size)}',
                        bar_format="{desc}: {percentage:.2f}%|{bar}| [{elapsed}<{remaining}]")
        else:
            print(f'downloading {convert_bytes(size)}...', end='')

    def load_progress(chunk):
        if verbose:
            pbar.update(chunk)
        else:
            pass

    # only show if >15 Mb
    cb = load_progress if size > 15_000_000 else None

    bucket.download_file(Key=bucket_file_path,
                         Filename=tmp_folder_file, Callback=cb)
    if verbose:
        print(f'loading as df...', end='')

    # load as dataframe
    if file_name.endswith('.csv'):
        df = pd.read_csv(tmp_folder_file)
    elif file_name.endswith('.csv.gz'):
        df = pd.read_csv(tmp_folder_file, compression='gzip')
    elif file_name.endswith('.pkl'):
        df = pd.read_pickle(tmp_folder_file)
    elif file_name.endswith('.parquet'):
        df = pd.read_parquet(tmp_folder_file)
    else:
        raise Exception('incorrect file format')

    if verbose:
        print(' shape:', df_shape(df))

    # remove local file
    os.remove(tmp_folder_file)

    return df


def s3_to_rs(bucket, s3_filepath: str, schema_table: str, conn, access_key: str, secret_key: str, print_sql=True):
    "copy data from S3 CSV/PARQUET file to a redshift table"
    # placeholder
    sql = f"""
    COPY {schema_table}
    FROM 's3://{bucket.name}/{s3_filepath}'
    """

    # file type config
    if s3_filepath.endswith('.csv'):
        sql += """
        FORMAT AS CSV
        IGNOREHEADER AS 1
        TRUNCATECOLUMNS
        EMPTYASNULL
        DELIMITER ','
        """

    if s3_filepath.endswith('.parquet'):
        sql += """
        FORMAT AS PARQUET
        """

    # credentials
    sql += f"""
    ACCESS_KEY_ID '{access_key}'
    SECRET_ACCESS_KEY '{secret_key}';
    ;
    """

    # pretty for print out
    sql = sql.replace('    ', '')

    if print_sql:
        print(sql, '\n')

    print('loading data from S3 to Redshift... ', end='')
    conn.execute(sql)
    print('done')


def rs_to_s3(query: str, bucket, s3_filepath: str, format_as: str, access_key: str, secret_key: str, conn, partition_by: str = None,
             verbose=False) -> str:
    "execute query, unload result to file in S3 bucket"
    # UNLOAD automatically creates a file ending, replace to avoid duplicate ending
    s3_filepath = s3_filepath.replace('.csv', '').replace('.parquet', '')

    if query.endswith('.sql'):
        query_string = open(query, 'r').read().replace('%', '%%')
    else:
        query_string = query
    query_string = '    ' + \
        query_string.replace("'", "\'").replace('\n', '\n        ')

    sql = f"""
    UNLOAD
    ($$
    {query_string}
    $$)
    TO 's3://{bucket.name}/{s3_filepath}'
    CREDENTIALS 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
    PARALLEL OFF --creates just one file
    FORMAT AS {format_as}
    ALLOWOVERWRITE
    """  # .replace('        ', '')

    if partition_by is not None:
        sql += f'PARTITION BY ({partition_by}) INCLUDE'

    if verbose:
        print(sql, '\n\n')

    # new path pattern
    new_path = f"{s3_filepath}000{'.parquet' if format_as=='PARQUET' else '.csv'}"
    print(f'run and unload to S3: /{new_path}', end='... ')
    if len(new_path) > 50:
        print('')

    # execute query
    start = datetime.datetime.now()
    result = conn.execute(sql)
    end = datetime.datetime.now()
    dur_s = (end-start).seconds + (end-start).microseconds/1_000_000
    if verbose:
        print(f'done in {dur_s:.2f}s', end=' ')

    # get file size.. seems to be quite slow sometimes
    # size = client.head_object(Bucket=bucket.name, Key='test_folder/file.sh')['ContentLength']
    size = bucket.Object(new_path).content_length
    if verbose:
        print(f'with size {convert_bytes(size)}')

    return new_path


def run_sql_rs(query: str, conn, verbose=False):
    "execute query on Redshift"
    print(query)
    if query.endswith('.sql'):
        query_string = open(query, 'r').read().replace('%', '%%')
    else:
        query_string = query
    query_string = '    ' + \
        query_string.replace("'", "\'").replace('\n', '\n        ')

    if verbose:
        print(query_string, '\n\n')

    # execute query
    start = datetime.datetime.now()
    result = conn.execute(query_string)
    end = datetime.datetime.now()
    dur_s = (end-start).seconds + (end-start).microseconds/1_000_000
    print(f'done in {dur_s:.2f}s', end=' ')


###############################################################################
##### GCP input/output functions ##############################################
###############################################################################
def bq_schema_to_dict(bq_schema: list) -> dict:
    "convert bq schema to dict with col_name:dtype key-value pairs"
    type_dict = {'INT64': 'int64',
                 'INTEGER': 'int64',
                 'FLOAT64': 'float64',
                 'FLOAT': 'float64',
                 'NUMERIC': 'float64',
                 'BIGNUMERIC': 'float64',
                 'STRING': 'object',
                 'DATE': 'datetime64'
                 }

    schema_dict = {}
    for col in r.schema:
        schema_dict[col.name] = type_dict[col.field_type]

    return schema_dict


def read_bigquery(query: str, bqclient, parse_dates: list = None, location: str = "US", verbose=False, query_params: dict = None) -> pd.DataFrame:
    """
    Load a query from BigQuery.
    query: query-string or file path
    """

    if query.endswith(".sql"):
        query_string = open(query, "r").read().replace("%", "%%")
    else:
        query_string = query

    if verbose:
        print("running query...", end=" ")
        progress_bar_type = "tqdm_notebook"
    else:
        progress_bar_type = None

    if query_params != None:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    param_name, param_type, param_value)
                for param_name, param_type, param_value in zip(
                    query_params["param_name"],
                    query_params["param_type"],
                    query_params["param_value"],
                )
            ]
        )
        job = bqclient.query(
            query_string, location=location, job_config=job_config)
    else:
        job = bqclient.query(query_string, location=location)

    if verbose:
        print("job done, downloading...", end=" ")
    result = job.result()
    df = result.to_dataframe(progress_bar_type=progress_bar_type)
    if verbose:
        print("done with shape", df.shape)

    if parse_dates is not None:
        for date_col in parse_dates:
            try:
                df[date_col] = pd.to_datetime(
                    df[date_col], errors="raise"
                ).dt.tz_localize(None)
            except:
                print("ERROR converting to datetime for column:", date_col)

    return df


def download_gs_blob(bucket, local_path: str, blob_file_path: str, verbose=True):
    """
    Downloads a blob from the bucket to a local path
    """

    # add '/' to path if not given, cbte if not existing
    if local_path.endswith('/') == False:
        local_path += '/'
    os.makedirs(local_path, exist_ok=True)

    # create local file path
    blob_file_name = blob_file_path.split('/')[-1]
    local_file_path = os.path.join(local_path, blob_file_name)

    blob = bucket.get_blob(blob_file_path)
    blob.download_to_filename(local_file_path)

    if verbose:
        print('Blob downloaded to:', local_file_path,
              '| size:', convert_bytes(blob.size),
              '| last modified:', str(blob.updated)[:19]
              )


def upload_gs_blob(bucket, local_file_path: str, dest_path: str, verbose=True):
    """
    Uploads a file to the bucket.
    """
    if dest_path.endswith('/') == False:
        dest_path += '/'

    local_file_name = local_file_path.split('/')[-1].split('\\')[-1]
    destination_file_path = dest_path + local_file_name

    blob = bucket.blob(destination_file_path)
    if verbose:
        print('uploading')
        print(f'-{local_file_path} ({get_file_size(local_file_path)})')
        print(f'-{dest_path}', end='... ')
    blob.upload_from_filename(local_file_path)
    if verbose:
        print('done.')


def df_to_gs(df, bucket, dest_file_path: str, verbose=True):
    """
    exports and uploads DataFrame as specified file-format to Google Cloud Storage
    """
    if os.name == 'nt':
        os.makedirs('C:/tmp_gs/', exist_ok=True)
        tmp_path = 'C:/tmp_gs/'
    elif os.name == 'posix':
        home = os.path.expanduser('~')
        os.makedirs(os.path.abspath(
            os.path.join(home, 'tmp_gs')), exist_ok=True)
        tmp_path = os.path.abspath(os.path.join(home, 'tmp_gs'))

    tmp_file = dest_file_path.split('/')[-1]
    dest_path = dest_file_path.replace(tmp_file, '')
    tmp_path_file = os.path.join(tmp_path, tmp_file)

    if verbose:
        print('saving DataFrame, shape', df_shape(
            df).rjust(15), 'to file', end='... ')

    if tmp_path_file.endswith('.csv.gz'):
        df.to_csv(tmp_path_file, float_format='%.12g',
                  index=False, compression='gzip')
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
        if verbose:
            print('no index', end=', ')
    else:
        raise Exception(
            'no valid file type (.csv, .csv.gz, .json, .pkl, .parquet)')
    print('done.', end='\t')

    upload_gs_blob(bucket, local_file_path=tmp_path_file, dest_path=dest_path)
    os.remove(tmp_path_file)


def gs_to_df(bucket, source_file_path: str, verbose=True):
    """
    Downloads DataFrame as specified file-format from Google Cloud Storage
    """
    if os.name == 'nt':
        os.makedirs('C:/tmp_gs/', exist_ok=True)
        tmp_path = 'C:/tmp_gs/'
    else:
        home = os.path.expanduser('~')
        os.makedirs(os.path.abspath(
            os.path.join(home, 'tmp_gs')), exist_ok=True)
        tmp_path = os.path.abspath(os.path.join(home, 'tmp_gs'))

    tmp_file = source_file_path.split('/')[-1]
    tmp_path_file = os.path.join(tmp_path, tmp_file)

    download_gs_blob(bucket, local_path=tmp_path,
                     blob_file_path=source_file_path)

    if tmp_path_file.endswith('.csv.gz'):
        df = pd.read_csv(tmp_path_file, compression='gzip')
        if verbose:
            print('Created DataFrame from {} file with shape: {}'.format(
                'csv gz', df.shape))
    elif tmp_path_file.endswith('.csv'):
        df = pd.read_csv(tmp_path_file)
        if verbose:
            print('Created DataFrame from {} file with shape: {}'.format(
                'csv', df.shape))
    elif tmp_path_file.endswith('.json'):
        df = pd.read_json(tmp_path_file)
        if verbose:
            print('Created DataFrame from {} file with shape: {}'.format(
                'json', df.shape))
    elif tmp_path_file.endswith('.pkl'):
        df = pd.read_pickle(tmp_path_file)
        if verbose:
            print('Created DataFrame from {} file with shape: {}'.format(
                'pkl', df.shape))
    elif tmp_path_file.endswith('.parquet'):
        df = pd.read_parquet(tmp_path_file)
        if verbose:
            print('Created DataFrame from {} file with shape: {}'.format(
                'parquet', df.shape))
    else:
        raise Exception("Error: incorrect file format")

    if verbose:
        print('done.', end='\t')

    os.remove(tmp_path_file)
    return df


###############################################################################
##### geo functions ###########################################################
###############################################################################

def lat_lon_distance(lat1, lon1, lat2, lon2):
    """
    Calculates the distance in km between two lat-lon points
    """
    try:
        p = 0.017453292
        a = 0.5 - np.cos((lat2 - lat1) * p)/2 + np.cos(lat1 * p) * \
            np.cos(lat2 * p) * (1 - np.cos((lon2 - lon1) * p)) / 2
        return round(12742 * np.arcsin(np.sqrt(a)), 1)
    except:
        print(f'error with:, {lat1}, {lon1} - {lat2}, {lon2}')


def get_redshift_connection(cfg: dict) -> sqlalchemy.engine.Engine:
    """
    returns a Redshift engine. Requires that cfg has the entries:
        cfg['redshift']['user']
        cfg['redshift']["pw"]
        cfg['redshift']['host']
        cfg['redshift']['port']
        cfg['redshift']['db']
    """
    return sqlalchemy.create_engine("postgresql://"
                                    + cfg['redshift']['user']+":" +
                                    cfg['redshift']["pw"]+"@"
                                    + cfg['redshift']['host']+":" +
                                    cfg['redshift']['port'] +
                                    "/"+cfg['redshift']['db'],
                                    isolation_level="AUTOCOMMIT")
