import pandas as pd
import datetime
import os
from tqdm.auto import tqdm


def load_sql(query:str, conn, verbose=True, parse_dates:list=None) -> pd.DataFrame:
    if query.endswith('.sql'):  query_string = open(query, 'r').read().replace('%', '%%')
    else:                       query_string = query

    if verbose: print(f'loading {query[:50]}...', end='')
    start = datetime.datetime.now()

    df = pd.read_sql(query_string, conn, parse_dates=parse_dates)

    end = datetime.datetime.now()
    dur_s = (end-start).seconds + (end-start).microseconds/1_000_000
    if verbose: print(f'done with shape: {df_shape(df)} in {dur_s:.2f}s with size {df_mem_usage(df)}')

    return df


def df_to_s3(df, bucket, bucket_file_path:str, verbose=True):
    "exports and uploads DataFrame as specified file-format to S3"
    # create temp folder
    if os.name == 'nt':
        os.makedirs('C:/tmp_s3/', exist_ok=True)
        dir_tmp_s3 = 'C:/tmp_s3/'
    elif os.name == 'posix':
        home = os.path.expanduser("~")
        os.makedirs(os.path.abspath(os.path.join(home, 'tmp_s3')), exist_ok=True)
        dir_tmp_s3 = os.path.abspath(os.path.join(home, 'tmp_s3'))

    file_name       = bucket_file_path.split('/')[-1]
    tmp_folder_file = os.path.join(dir_tmp_s3, file_name)

    # save local file
    if verbose: print(f'saving DataFrame {df_shape(df)} to file:', end='... ')
    if   file_name.endswith(".csv"):     df.to_csv(    tmp_folder_file, float_format="%.12g", index=False)
    elif file_name.endswith(".csv.gz"):  df.to_csv(    tmp_folder_file, float_format="%.12g", index=False, compression="gzip")
    elif file_name.endswith(".pkl"):     df.to_pickle( tmp_folder_file)
    elif file_name.endswith(".parquet"): df.to_parquet(tmp_folder_file, index=False)
    elif file_name.endswith(".json"):    df.to_json(   tmp_folder_file, date_format='iso')
    else: raise Exception("no valid file type")

    if verbose: print(f'done with size: {get_file_size(tmp_folder_file)}')

    # upload progress bar
    if verbose: pbar = tqdm(total=os.stat(tmp_folder_file).st_size, desc='uploading', bar_format="{desc}: {percentage:.2f}%|{bar}| [{elapsed}<{remaining}]")
    def load_progress(chunk):
        if verbose: pbar.update(chunk)
        else: pass

    # local -> S3
    bucket.upload_file(Filename=tmp_folder_file, Key=bucket_file_path, Callback=load_progress)
    if verbose: pbar.close()

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
        os.makedirs(os.path.abspath(os.path.join(home, 'tmp_s3')), exist_ok=True)
        dir_tmp_s3 = os.path.abspath(os.path.join(home, 'tmp_s3'))

    file_name       = bucket_file_path.split('/')[-1]
    tmp_folder_file = os.path.join(dir_tmp_s3, file_name)

    # download and status
    size = bucket.Object(bucket_file_path).content_length
    if verbose: pbar = tqdm(total=size, desc=f'downloading {convert_bytes(size)}',
                            bar_format="{desc}: {percentage:.2f}%|{bar}| [{elapsed}<{remaining}]")
    def load_progress(chunk):
        if verbose: pbar.update(chunk)
        else: pass
    
    bucket.download_file(Key=bucket_file_path, Filename=tmp_folder_file, Callback=load_progress)
    if verbose: print(f'loading as df...', end='')

    # load as dataframe
    if   file_name.endswith('.csv'):     df = pd.read_csv(tmp_folder_file)
    elif file_name.endswith('.csv.gz'):  df = pd.read_csv(tmp_folder_file, compression='gzip')
    elif file_name.endswith('.pkl'):     df = pd.read_pickle(tmp_folder_file)
    elif file_name.endswith('.parquet'): df = pd.read_parquet(tmp_folder_file)
    else: raise Exception('incorrect file format')

    if verbose: print(' shape:', df_shape(df))

    # remove local file
    os.remove(tmp_folder_file)

    return df


def s3_to_rs(bucket, s3_filepath:str, schema_table:str, conn, access_key:str, secret_key:str, print_sql=True):
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

    if print_sql: print(sql, '\n')

    print('loading data from S3 to Redshift... ', end='')
    conn.execute(sql)
    print('done')


def rs_to_s3(query:str, bucket, s3_filepath:str, format_as:str, access_key:str, secret_key:str, conn, verbose=False) -> str:
    "execute query, unload result to file in S3 bucket"
    # UNLOAD automatically creates a file ending, replace to avoid duplicate ending
    s3_filepath = s3_filepath.replace('.csv', '').replace('.parquet', '')

    if query.endswith('.sql'):  query_string = open(query, 'r').read().replace('%', '%%')
    else:                       query_string = query
    query_string = '    '+query_string.replace("'", "\'").replace('\n', '\n        ')
    
    sql = f"""
    UNLOAD
    ($$
    {query_string}
    $$)
    TO 's3://{bucket.name}/{s3_filepath}'
    CREDENTIALS 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
    PARALLEL OFF --creates just one file
    FORMAT AS {format_as}
    ALLOWOVERWRITE;
    """#.replace('        ', '')

    if verbose: print(sql, '\n\n')

    # new path pattern
    new_path = f"{s3_filepath}000{'.parquet' if format_as=='PARQUET' else '.csv'}"
    print(f'executing and unloading to S3: {bucket.name}/{new_path}', end='... ')

    # execute query
    start = datetime.datetime.now()
    result = conn.execute(sql)
    end = datetime.datetime.now()
    dur_s = (end-start).seconds + (end-start).microseconds/1_000_000
    print(f'done in {dur_s:.2f}s', end=' ')

    # get file size.. seems to be quite slow sometimes
    # size = client.head_object(Bucket=bucket.name, Key='test_folder/file.sh')['ContentLength']
    size = bucket.Object(new_path).content_length
    print(f'with size {convert_bytes(size)}')

    return new_path


def run_sql_rs(query:str, conn, verbose=False):
    "execute query on Redshift"

    if query.endswith('.sql'):  query_string = open(query, 'r').read().replace('%', '%%')
    else:                       query_string = query
    query_string = '    '+query_string.replace("'", "\'").replace('\n', '\n        ')
    
    if verbose: print(query_string, '\n\n')

    # execute query
    start = datetime.datetime.now()
    result = conn.execute(query_string)
    end = datetime.datetime.now()
    dur_s = (end-start).seconds + (end-start).microseconds/1_000_000
    print(f'done in {dur_s:.2f}s', end=' ')
