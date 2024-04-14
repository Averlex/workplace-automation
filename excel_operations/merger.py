"""
A module contains merging functions for excel functions
"""
import warnings
from datetime import timedelta
import pandas as pd
from typing import Literal
from excel_operations.excel_utils import transform_date
from utils.utils import validate_arg_type
from settings.defaults import GlobalDefaults, SystemDefaults, ResourcesDefaults


def concat_tables(tables: list[pd.DataFrame],
                  axis: Literal["h", "v"] = "v",
                  drop_indices: bool = False
                  ) -> pd.DataFrame:
    """
    A simple wrapper for tables concatenation
    :param tables: list of the source tables to concat
    :param axis: the axis along which the merging is needed. Vertical or horizontal
    :param drop_indices: a flag indicates whether to drop indices or not. Not recommended for named indices
    :return: merged pd.DataFrame
    """
    print("Initializing tables concatenation...")
    if tables is None or len(tables) == 0:
        print("At least one of the tables does not exist or is empty")
        return pd.DataFrame()

    if len(tables) == 1:
        print("Only one table is stored, no concatenation is needed")
        return tables[0]

    if drop_indices:
        for table in tables:
            table.reset_index(inplace=True, drop=True)

    if axis == "v":
        res = pd.concat(tables, axis=0, join="outer")
    else:
        res = pd.concat(tables, axis=1, join="outer")

    print("Tables successfully concatenated")
    return res


def _parse_keys(col_names: list[str] | str,
                source_header: list,
                target_header: list,
                source_key_names: list,
                target_key_names: list,
                source_date_name: str = "",
                target_date_name: str = "",
                use_dates: bool = False) -> list[str]:
    """
    Parses columns of 2 given tables to provide a safe merging

    :param col_names: list of column names to add. Single column name is also viable. Passing 'all' will trigger merging with all columns
    :param source_header: the source table's header
    :param target_header: the target's table header
    :param source_key_names: name of the key field(s) in the source table
    :param target_key_names: name of the key field(s) in the target table
    :param use_dates: flag indicating whether to use date comparison or not
    :param source_date_name: name of the date field in the source table. Might be ignored (depends on 'use_dates' param)
    :param target_date_name: name of the date field in the target table. Might be ignored (depends on 'use_dates' param)

    :return: header to work with further, empty list if no actions needed

    :raises ValueError: on NoneType argument
    :raises AttributeError: invalid parameters, requires user actions and/or changes in the source data
    """
    column_names = col_names
    target_set = set(target_header)
    source_set = set(source_header)
    source_key_set = set(source_key_names)
    target_key_set = set(target_key_names)

    # Processing arguments
    if "all" in column_names:
        column_names.extend(target_header)
        column_names = list(set(column_names))
        column_names.remove("all")

    # Adding key field to the lookup table
    if not target_key_set <= set(column_names):
        column_names.extend(target_key_names)

    # If nothing to add (we don't want to add a key column since it should already be in the source table)
    if set(column_names) == {}:
        warnings.warn(f"Nothing to add: source table already has {target_key_names}", category=UserWarning)
        return []

    # If not a subset (semi-intersections excluded) of target table
    if not set(column_names) <= target_set:
        difference = set(column_names) - target_set
        warnings.warn(f"Not all the column names are represented in the target table. "
                      f"Missing columns: {difference}", category=UserWarning)
        column_names = list(set(column_names) - difference)

    # Source key column names validation
    if not source_key_set <= source_set:
        raise AttributeError(f"Not all of the merging keys are represented in the source table: "
                             f"{source_key_set - source_set}. No additions will be done")

    # Target key column names validation
    if not target_key_set <= target_set:
        raise AttributeError(f"Not all of the merging keys are represented in the target table: "
                             f"{target_key_set - target_set}. No additions will be done")

    # Keys length comparison
    if len(source_key_set) != len(target_key_set):
        raise AttributeError(f"Keys count doesn't match. Source: {len(source_key_set)}, target: {len(target_key_set)}. "
                             f"No additions will be done")

    if use_dates:
        # Same for the dates
        if source_date_name not in source_header:
            raise AttributeError(f"There is no '{source_date_name}' column in the source table. "
                                 f"No additions will be done")
        if target_date_name not in target_header:
            raise AttributeError(f"There is no '{target_date_name}' column in the target table. "
                                 f"No additions will be done")
        # Adding date name to the lookup names if needed
        if target_date_name not in column_names:
            column_names.append(target_date_name)

    return list(set(column_names))


def merge_with_table(col_names: list[str] | str,
                     source_table: pd.DataFrame,
                     target_table: pd.DataFrame,
                     source_key_names: str | list = None,
                     target_key_names: str | list = None,
                     is_numeric: bool = False,
                     use_dates: bool = False,
                     source_date_name: str = None,
                     target_date_name: str = None,
                     default_timedelta: timedelta = timedelta(weeks=5200),
                     add_suffix: bool = True,
                     suffix: str = " (доп.)"
                     ) -> pd.DataFrame:
    """
    Forms a DataFrame using the given one by merging it with columns with another one. \n
    Merged columns are always in alphabetical order. Merging is performed by one column as well as a list of columns. \n
    It's possible to apply a numeric conversion to keys using 'is_numeric' parameter - e.g., '012345' -> '12345' so numeric key strings will match each other better. Conversion is applied only wherever it's possible. \n
    Additional date params allows to perform check if the records are within the given time interval. The record with the minimal timedelta is always preferred over other matches. Any records outside of the given timedelta will be considered as the ones with no matches.

    :param col_names: list of column names to add. Single column name is also viable. Passing 'all' will trigger merging with all columns
    :param source_table: the source table
    :param target_table: the target table
    :param source_key_names: name of the key field(s) in the source table
    :param target_key_names: name of the key field(s) in the target table
    :param is_numeric: flag indicating if the key is a numeric value. Numerics are stripped of 0's at the beginning as well as trash values will be ignored. Applies to each of the key columns, so if applying is needed only for part of them then preprocess necessary columns and disable this parameter
    :param use_dates: flag indicating whether to use date comparison or not
    :param source_date_name: name of the date field in the source table. Might be ignored (depends on 'use_dates' param)
    :param target_date_name: name of the date field in the target table. Might be ignored (depends on 'use_dates' param)
    :param default_timedelta: default value for datetimes comparison. Difference exceeding this one is considered as no match found. Default is 100 years
    :param add_suffix: flag defining whether to modify column names of the formed DataFrame or not. Default is True
    :param suffix: string value for suffix to add. Default is ''

    :return: merged table on success, the source table on a failed attempt
    """
    print("Initializing tables merging...")

    if source_table is None or target_table is None:
        print(f"One of the tables doesn't exist. Source type: {type(source_table)}, target type: {type(target_table)}")
        return source_table

    if source_table.empty:
        print(f"The source table is empty. No additions could be done")
        return source_table

    if target_table.empty:
        print(f"The target table is empty. No additions will be done")
        return source_table

    # Args preparation
    column_names = [i.lower() for i in validate_arg_type(col_names)]
    _column_names_unchanged = column_names.copy()  # This one if the later check, so we can drop unnecessary columns
    # Source keys
    if source_key_names is None:
        _source_key_names = str(SystemDefaults.garage_num).lower()
    else:
        _source_key_names = [i.lower() for i in validate_arg_type(source_key_names)]
    # Target keys
    if target_key_names is None:
        _target_key_names = str(ResourcesDefaults.garage_num).lower()
    else:
        _target_key_names = [i.lower() for i in validate_arg_type(target_key_names)]
    # Source date key
    if source_date_name is None:
        _source_date_name = str(SystemDefaults.creation_date).lower()
    else:
        _source_date_name = source_date_name.lower()
    # Target date key
    if target_date_name is None:
        _target_date_name = str(ResourcesDefaults.date).lower()
    else:
        _target_date_name = target_date_name.lower()

    source_columns = source_table.columns.tolist()
    force_suffix = False

    # Checking if we need to force a suffix
    lookup_diff = set(_column_names_unchanged).intersection(set(source_table.columns.tolist()))
    if len(lookup_diff) != 0 and not add_suffix:
        force_suffix = True
        warnings.warn(f"Some of the target table columns for merging are already in the source table, forcing suffix")

    # Further columns validation
    try:
        column_names = _parse_keys(column_names, source_table.columns.tolist(), target_table.columns.tolist(),
                                   _source_key_names, _target_key_names, _source_date_name, _target_date_name, use_dates)
        if len(column_names) == 0:
            return source_table
    except (AttributeError, ValueError) as err:
        warnings.warn(err.__str__(), category=UserWarning)
        return source_table

    # Leaving only columns we actually need
    tmp_target = target_table[column_names].copy(deep=True)
    tmp_source = source_table.copy(deep=True)

    # Filtering target table, so we will look up for values which are in the source table
    if is_numeric:
        # Satisfies the is_numeric logic and stripping from non-significant symbols
        tmp_target.loc[:, _target_key_names] = tmp_target.loc[:, _target_key_names].apply(
            lambda x: str(int(x)) if str(x).isnumeric() else x, axis=0)
        tmp_source.loc[:, _source_key_names] = tmp_source.loc[:, _source_key_names].apply(
            lambda x: str(int(x)) if str(x).isnumeric() else x, axis=0)
        print("Numeric conversion to tables' keys applied (where it was possible)")

    # Executes only if needed: adds a suffix to the new column names for pandas method
    if add_suffix and suffix != "":
        _suffix = suffix
        suffixes = {"suffixes": (None, _suffix)}
        print("Suffix added successfully")
    else:
        _suffix = ""
        suffixes = {"suffixes": (None, None)}

    # Suffix preparation in case intersecting columns
    if force_suffix:
        if suffix != "":
            _suffix = suffix
        else:
            _suffix = merge_with_table.__annotations__[suffix].default
        suffixes = {"suffixes": (None, _suffix)}

    _default_na_val = GlobalDefaults.na_val
    _default_timedelta = abs(default_timedelta)

    # Simple lookup table mode
    if not use_dates:
        res = pd.merge(left=tmp_source, right=tmp_target, how="left", left_on=_source_key_names,
                       right_on=_target_key_names, **suffixes)  # .fillna(_default_na_val)
    # Using date comparison: only the closest one by date in a given time interval will be merged
    else:
        if GlobalDefaults.parsed_date not in tmp_source.keys():
            tmp_source[GlobalDefaults.parsed_date] = transform_date(
                tmp_source[_source_date_name].values.tolist())[GlobalDefaults.parsed_date]
        if GlobalDefaults.parsed_date not in tmp_target.keys():
            tmp_target[GlobalDefaults.parsed_date] = transform_date(
                tmp_target[_target_date_name].values.tolist())[GlobalDefaults.parsed_date]

        tmp_source[GlobalDefaults.parsed_date] = pd.to_datetime(tmp_source[GlobalDefaults.parsed_date], errors="coerce")
        tmp_target[GlobalDefaults.parsed_date] = pd.to_datetime(tmp_target[GlobalDefaults.parsed_date], errors="coerce")

        res = pd.merge_asof(left=tmp_source.sort_values(by=GlobalDefaults.parsed_date),
                            right=tmp_target.sort_values(by=GlobalDefaults.parsed_date),
                            on=GlobalDefaults.parsed_date, left_by=_source_key_names, right_by=_target_key_names,
                            **suffixes, tolerance=_default_timedelta, allow_exact_matches=True,
                            direction="nearest").fillna(_default_na_val)

    # Checking if we've succeeded
    if res is None:
        warnings.warn(f"Internal error occurred during merging: table stays without changes", category=UserWarning)
        res = source_table
        return res

    # Applying suffixes for those columns which doesn't have one (if set by params)
    res.columns = [col + _suffix if not col.endswith(_suffix) and col in _column_names_unchanged and col
                                    not in source_columns else col for col in res.columns]

    # Sorting the added part in alphabetical order (merges works a bit differently + preventing from a mess from sets)
    _column_names_unchanged.sort()
    # Stripping from any auxiliary columns (e.g., "parsed date", key columns, etc.)
    _column_names_unchanged = [i + _suffix for i in _column_names_unchanged if i not in _target_key_names]
    res = res.loc[:, source_columns + _column_names_unchanged]

    print("Table merged successfully")
    return res


if __name__ == "__main__":
    pass
