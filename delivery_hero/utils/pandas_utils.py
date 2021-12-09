import pandas as pd
import numpy as np


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
        dfhtml = dfhtml.replace(f'<th>{name}</th>', f'<th class="rotate"><div><span>{name}</span></div></th>')

    display(HTML(dfhtml))


def df_mem_usage(df):
    "return string with memory usage of dataframe in human readable format (KB, MB, GB, ...)"
    mem_usage_str = convert_bytes(df.memory_usage().sum())
    return mem_usage_str


def df_shape(df) -> str:
    "return string of shape with cleaner format, e.g. (10,000 x 24)"
    shape_str = f'({df.shape[0]:,} x {df.shape[1]})'
    return shape_str


def df_info(df, nunique=True, incl_min=True, incl_max=True, mem_usage=True) -> pd.DataFrame:
    "show general info about df, more detailed than df.info()"
    info_details = pd.DataFrame(index=df.columns)

    info_details['dtype']    = df.dtypes
    if mem_usage: info_details['memory_mb'] = np.round(df.memory_usage(deep=True ) /1_000_000, 2)

    if nunique: info_details['nunique']  = df.nunique()

    info_details['notnull']  = df.notnull().sum()
    info_details['isnull']   = df.isnull().sum()
    info_details['isnull_%'] = np.round(df.isnull().sum( ) *100 / len(df), 2)

    if incl_min:
        info_details['min'] = df.min()
    if incl_max:
        info_details['max'] = df.max()

    # info_details['mode']     = df.mode()

    return info_details.reset_index(drop=False).rename(columns={'index' :'column'})


def info_catg(row, top=10):
    null = row.isnull().sum()
    v_counts = row.value_counts()
    n_unique = row.nunique()
    appears_only_once = (v_counts==1).sum()

    print(f"'{row.name}':")
    print('null:     ', null)
    print('n_unique: ', n_unique)
    print('only once:', appears_only_once)

    if top>0:
        print('\nvalue counts top 10:')
        print(str(v_counts.head(top)).split('\nName')[0])
