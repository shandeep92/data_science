import pandas as pd
import re
import pint


def get_num_pieces_after_pattern() -> re.Pattern:
    """
    Returns the regular expression pattern where the number of pieces is like "<keywords> <number of pieces>"
    Returns:

    """
    num_pieces_after_keywords = {
        "pack of",
        "x",
        "X",
        "×"
    }

    num_pieces_after_keywords_str = "|".join(num_pieces_after_keywords)
    num_pieces_after_pattern = re.compile(rf'((?:{num_pieces_after_keywords_str})\s?)(\d+)', re.UNICODE)

    return num_pieces_after_pattern


def get_num_pieces_before_pattern() -> re.Pattern:
    """
    Returns the regular expression pattern where the number of pieces is like "<number of pieces> <keywords>"

    Returns:

    """
    num_pieces_before_keywords = {
        # "x", --> Worth including based on the data set
        "-pieces",
        "pieces",
        "piece",
        "-piece",
        "unit",
        "units",
        "per pack",
        "-in-1",
        "in 1",
        "in-1",
        "pack",
        "packs",
        "pc",
        "pcs",
        "-p",
        "st",
        "-st",
        "'s",
        "'S",
        # "s",
        # "S",
        # "//",
        # "p"

    }
    num_pieces_before_keywords_str = "|".join(num_pieces_before_keywords)
    num_pieces_before_pattern = re.compile(rf'(\d+)(\s?(?:{num_pieces_before_keywords_str}))', re.UNICODE)

    return num_pieces_before_pattern


def get_star_pieces_pattern() -> re.Pattern:
    star_pieces_pattern = re.compile(r'(\d+)(\s?[\*x]+\s?)(\d+)')
    return star_pieces_pattern


def get_product_content_unit_base(df_product_content_unit_col: pd.core.series.Series) -> pd.core.series.Series:
    unit_base_conversion_dict = {
        "miligram": "gram",
        "mg": "gram",
        "kilogram": "gram",
        "kg": "gram",
        "pound": "gram",
        "lb": "gram",
        "ounce": "gram",
        "oz": "gram",
        "mililiter": "liter",
        "ml": "liter",
        "deciliter": "liter",
        "dl": "liter",
        "centiliter": "liter",
        "cl": "liter",
        "gram": "gram",
        "g": "gram",
        "liter": "liter",
        "l": "liter",
        "st": "st"
    }

    return df_product_content_unit_col.replace(unit_base_conversion_dict)


def get_product_package_piece_count(df_name_col: pd.core.series.Series) -> pd.core.series.Series:
    return df_name_col.apply(extract_num_pieces)


def extract_num_pieces(product_name: str) -> str:
    "extract number of pieces based on regex and keyword dicts"
    if product_name is None:
        return None
    else:
        results_list = re.findall(get_num_pieces_after_pattern(), product_name.lower())
        if len(results_list) > 0:
            return results_list[0][1]
        elif len(re.findall(get_num_pieces_before_pattern(), product_name.lower())) > 0:
            return re.findall(get_num_pieces_before_pattern(), product_name.lower())[0][0]
        else:
            results_list = re.findall(get_star_pieces_pattern(), product_name.lower())
            if len(results_list) > 0:
                return results_list[0][0]
        return None


def get_unit_size_type_pattern() -> re.Pattern:
    product_content_units = ["miligram", "kilogram", "gram", "mg", "kg", "k","g","gr",
                             "pound", "ounce", "lb", "oz",
                             "mililiter", "deciliter", "liter" "centiliter", "cl", "dl", "ml", "l",
                             "servings", "box",
                             "cm3", "cm2", "m2", "lt",
                             "sheets", "sheet", "tablets", "tablet", "sachets", "sachet", "capsules",
                             "capsule", "bags", "bag", "packets", "packet", "bunches", "bunch",
                             "bouquets", "bouquet", "rolls", "roll", "boxes",
                             ]

    product_content_units_str = "|".join(product_content_units)
    unit_size_type_pattern = re.compile(
        rf'(\d+\s{{0,1}}|\d+\.\d+\s{{0,1}})+(({product_content_units_str})($|\s))',
        re.UNICODE)

    return unit_size_type_pattern


def remove_unit_value_pieces(name: str) -> str:
    """
    remove, unit, value, and pieces information from string
    """

    if name is None:
        return None
    else:
        clean = re.sub(get_unit_size_type_pattern(), '', name.lower())
        clean = re.sub(get_num_pieces_after_pattern(), '', clean.lower())
        clean = re.sub(get_num_pieces_before_pattern(), '', clean.lower())

        return clean.replace('  ', '').strip()


def convert_to_base_unit(content_value: float, content_unit: str,
                         volume_base: str = 'liter', weight_base: str = 'gram',
                         ureg=pint.UnitRegistry) -> list:
    """
    Gets base value and units using pint. Deals with None and non-standard unit types.
    If used in pandas.apply, value column has Nan for missing, must be converted to None for matching.
    Assumes definition in environment of a pint.UnitRegistry() object
    """

    base_value_and_unit = [content_value, content_unit]

    if content_value is not None:
        if content_unit in ureg:
            # pint object for transformation
            pint_unit_registry = content_value * ureg(content_unit)

            if pint_unit_registry.is_compatible_with(volume_base):
                pint_unit_registry = pint_unit_registry.to(volume_base)
            elif pint_unit_registry.is_compatible_with(weight_base):
                pint_unit_registry = pint_unit_registry.to(weight_base)
            base_value_and_unit = [round(float(pint_unit_registry.magnitude), 4), str(pint_unit_registry.units)]

    return base_value_and_unit


def extract_num_pieces(product_name: str) -> str:
    "extract number of pieces based on regex and keyword dicts"
    if product_name is None:
        return None
    else:
        results_list = re.findall(get_num_pieces_after_pattern(), product_name.lower())
        if len(results_list) > 0:
            return results_list[0][1]
        elif len(re.findall(get_num_pieces_before_pattern(), product_name.lower())) > 0:
            return re.findall(get_num_pieces_before_pattern(), product_name.lower())[0][0]
        else:
            results_list = re.findall(get_star_pieces_pattern(), product_name.lower())
            if len(results_list) > 0:
                return results_list[0][0]
        return None


def unify_unit_type(s: str) -> str:
    replacement_dict = {
        "kilogram": "kg",
        "mililiter": "ml",
        "deciliter": "dl",
        "gram": "g",
        "liter": "l",
        "ounce": "oz"
    }
    for old_str, new_str in replacement_dict.items():
        s = s.replace(old_str, new_str)
    return s


def extract_unit_size_type(s: str) -> (str, float):
    "find all matching string for size and type"
    if s is None:
        return None, None
    else:
        results_list = re.findall(get_unit_size_type_pattern(), s.lower())
        if len(results_list) > 0:
            unit_size = float(results_list[0][0])
            unit_type = unify_unit_type(results_list[0][1])
            return unit_type, unit_size
        else:
            return None, None

def remove_unit_value_pieces(name: str) -> str:
    """
    remove, unit, value, and pieces information from string
    """
    if name is None:
        return None
    else:
        clean = re.sub(get_unit_size_type_pattern(), '', name.lower())
        clean = re.sub(get_num_pieces_after_pattern(), '', clean.lower())
        clean = re.sub(get_num_pieces_before_pattern(), '', clean.lower())

        return clean.replace('  ', '').strip()


def convert_to_base_unit(content_value: float, content_unit: str,
                         volume_base: str = 'liter', weight_base: str = 'gram',
                         ureg=pint.UnitRegistry) -> list:
    """
    Gets base value and units using pint. Deals with None and non-standard unit types.
    If used in pandas.apply, value column has Nan for missing, must be converted to None for matching.
    Assumes definition in environment of a pint.UnitRegistry() object
    """

    base_value_and_unit = [content_value, content_unit]

    if content_value is not None:
        if content_unit in ureg:
            # pint object for transformation
            pint_unit_registry = content_value * ureg(content_unit)

            if pint_unit_registry.is_compatible_with(volume_base):
                pint_unit_registry = pint_unit_registry.to(volume_base)
            elif pint_unit_registry.is_compatible_with(weight_base):
                pint_unit_registry = pint_unit_registry.to(weight_base)
            base_value_and_unit = [round(float(pint_unit_registry.magnitude), 4), str(pint_unit_registry.units)]

    return base_value_and_unit