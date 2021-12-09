import pandas as pd
import sys

sys.path.insert(1, '../')
from utils import utils


def bq_table_info(bqclient, project, dataset, table):
    "print table details from BQ API"
    t = bqclient.get_table(f'{project}.{dataset}.{table}')

    print( 'type:          ', t.table_type)
    print(f'row count:      {t.num_rows:,}')
    print( 'column count:  ', len(t.schema))
    print( 'size:          ', utils.convert_bytes(t.num_bytes))
    print( 'table created: ', str(t.created)[:19])
    print( 'last modified: ', str(t.modified)[:19])
    print( 'description:   ', t.description)
    print( 'labels:        ', t.labels)
    print( 'partitioned by:', t.partitioning_type)
    print( 'GCP location:  ', t.location)

def bq_schema_df(bqclient, project, dataset, table) -> pd.DataFrame:
    "return dataframe with schema details"
    table_obj = bqclient.get_table(f'{project}.{dataset}.{table}')
    schema = table_obj.schema
    
    df = pd.DataFrame(index=range(len(schema)),
                      data = {'name': [col.name       for col in schema],
                              'type': [col.field_type for col in schema],
                              'mode': [col.mode       for col in schema],
                              'description': [col.description for col in schema]
                             })
    
    return df