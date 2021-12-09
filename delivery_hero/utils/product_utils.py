
import pandas as pd


def get_product_discount_pct(df_price_col: pd.core.series.Series,
                             df_sale_price_col: pd.core.series.Series) -> pd.core.series.Series:
    return round((df_price_col - df_sale_price_col) / df_price_col * 100, 2)


def get_product_name_short(df: pd.DataFrame, df_name_col_name: str, df_brand_col_name: str) -> pd.core.series.Series:
    return df.apply(lambda x: remove_brand_name(x[df_name_col_name], x[df_brand_col_name]), axis=1)


def get_product_name_detailed(df, df_name_col_name: str, df_brand_col_name: str):
    return df.apply(lambda x: append_brand_name(x[df_name_col_name], x[df_brand_col_name]), axis=1)


def append_brand_name(product_name: str, brand_name: str) -> str:
    if brand_name is not None and brand_name not in product_name:
        return product_name + " " + brand_name
    return product_name.strip()


def remove_brand_name(full_str: str, brand_name: str) -> str:
    if brand_name is not None:
        full_str = full_str.replace(brand_name, "")
    return full_str.strip()


