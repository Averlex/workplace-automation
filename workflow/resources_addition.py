"""
A module contains parsing methods for some resources columns with further adding in a table
"""
import warnings

import pandas as pd
from excel_operations.merger import concat_tables
from excel_operations.excel_utils import transform_date
from settings.defaults import ResourcesDefaults, GlobalDefaults


class ResourcesAddition:
    """
    Class forms a pd.DataFrame based on parametric columns
    """
    _date_col_name = ResourcesDefaults.date

    def __init__(self,
                 source: pd.DataFrame,
                 date_col_name: str = None,
                 date_pattern: str = str(ResourcesDefaults.datetime_format)):
        """
        Forms a dictionary with year and month columns in string format. The result dictionary is stored within
        the class object
        :param source: source table for columns to add
        :param date_col_name: name of the source table date column
        :param date_pattern: a date pattern. By default = ResourcesDefaults.datetime_format
        :return: nothing
        """
        # Source table emptiness check
        if source is None:
            print(f"The source table doesn't exist. No additions could be done")
            self.table = None
            return
        if source.empty:
            print(f"The source table is empty. No additions will be done")
            self.table = None
            return

        _date_col_name = date_col_name
        if _date_col_name is None:
            _date_col_name = str(ResourcesDefaults.date)
        _date_col_name = _date_col_name.lower()

        # Checking if the table has the desired date column
        contains_dates = False
        for elem in source.columns.tolist():
            if _date_col_name in elem:
                contains_dates = True
                break

        if not contains_dates:
            print("No date was found in resources table. The source table stays without any changes")
            self.table = source

        # Forming cols and adding them to the source table
        additional_table = self.form_cols(source[_date_col_name].values.tolist(), date_pattern)

        diff = set(source.columns.tolist()).intersection(additional_table.columns.tolist())
        if len(diff) != 0:
            warnings.warn(
                f"Some columns in additional table overlaps the source ones: {diff}, dropping additional ones")
            additional_table.drop(columns=list(diff), inplace=True)

        # Normalizing vehicle class
        diff = set(ResourcesDefaults.vehicle_class_list).intersection(set(source.columns.tolist())) - {ResourcesDefaults.vehicle_class}
        if len(diff) >= 1:
            # source[list(diff)].fillna("", inplace=True)
            if ResourcesDefaults.vehicle_class not in source.columns.tolist():
                source[ResourcesDefaults.vehicle_class] = GlobalDefaults.na_val
            for col in list(diff):
                source[ResourcesDefaults.vehicle_class] = source.\
                    apply(lambda row: row[ResourcesDefaults.vehicle_class] if row[col] in ('', 'N/A') else row[col], axis=1)

        self.table = concat_tables([source, additional_table], axis="h", drop_indices=True)
        return

    @staticmethod
    def form_cols(creation_date: list[str], date_pattern: str = None) -> pd.DataFrame:
        """
        Forms a pd.DataFrame with year and month columns in string format. The result dictionary is stored within
        the class object. Just a local wrapper
        :param creation_date: source date column for parsing
        :param date_pattern: a date pattern. By default = ResourcesDefaults.datetime_format
        :return:
        """
        _date_pattern = date_pattern
        if _date_pattern is None:
            _date_pattern = str(ResourcesDefaults.datetime_format)

        return transform_date(creation_date, date_pattern, year=True, month=True)

        pass
