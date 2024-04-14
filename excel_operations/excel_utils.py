"""
A module contains utility functions needed for excel parsing
"""

import calendar
import locale
from datetime import datetime, time, date

import openpyxl
import pandas as pd
from dateutil import parser
import re
from settings.defaults import GlobalDefaults


def transform_date(source_col: list[str], date_pattern: str = None,
                   **kwargs) -> pd.DataFrame:
    """
    Forms several columns based on a source column. Column with datetime object will be added regardless of **kwargs
    :param source_col: source columns which contains date in str format
    :param date_pattern: desired pattern for date conversion. Default is a config value
    :keyword datetime: a flag indicating whether the datetime column is needed or not. Default is False
    :keyword year: a flag indicating whether the year column is needed or not. Default is False
    :keyword month: a flag indicating whether the month (full name) column is needed or not. Default is False
    :keyword day: a flag indicating whether the day number column is needed or not. Default is False
    :keyword hour: a flag indicating whether the year column is needed or not. Default is False
    :keyword week: a flag indicating whether the week number column is needed or not. Default is False
    :return: pd.DataFrame, consisting of the desire columns
    """
    locale.setlocale(locale.LC_ALL, 'ru_RU')
    search_res = {GlobalDefaults.parsed_date: date_to_datetime(source_col)}
    _date_pattern = date_pattern
    if _date_pattern is None:
        _date_pattern = str(GlobalDefaults.datetime_preferred_format)

    add_year = kwargs.get("year", False)
    add_month = kwargs.get("month", False)
    add_day = kwargs.get("day", False)
    add_hour = kwargs.get("hour", False)
    add_week = kwargs.get("week", False)
    add_datetime = kwargs.get("datetime", False)

    if add_datetime:
        search_res[GlobalDefaults.date] = []
    if add_year:
        search_res[GlobalDefaults.year] = []
    if add_month:
        search_res[GlobalDefaults.month] = []
    if add_day:
        search_res[GlobalDefaults.day] = []
    if add_hour:
        search_res[GlobalDefaults.hour] = []
    if add_week:
        search_res[GlobalDefaults.week] = []

    for row_elem in search_res[GlobalDefaults.parsed_date]:
        if add_datetime:
            search_res[GlobalDefaults.date] += [row_elem.strftime(_date_pattern)]
        if add_year:
            search_res[GlobalDefaults.year] += [str(row_elem.year)]
        if add_month:
            search_res[GlobalDefaults.month] += [str(calendar.month_name[row_elem.month])]
        if add_day:
            search_res[GlobalDefaults.day] += [str(row_elem.day)]
        if add_hour:
            search_res[GlobalDefaults.hour] += [str(row_elem.hour)]
        if add_week:
            search_res[GlobalDefaults.week] += [str(row_elem.isocalendar()[1])]

    return pd.DataFrame.from_dict(search_res)


def parse_date_value(val: str) -> str:
    """
    A simple parser with a small functionality for parsing date and time parts of the string datetime value
    :param val: source value
    :return: '%d.%m.%Y %H:%M:%S.f'-like string (or its date/time part) or the source one on failed attempt
    """
    res_val = val
    # Removing some spaces
    if "  " in res_val:
        res_val = res_val.replace("  ", " ")

    # Parsing date and time parts considering it could be split by a single space delimiter
    if " " in res_val:
        # Removing more useless spaces
        if res_val[0] == " ":
            res_val = res_val[1:]
        if res_val[-1] == " ":
            res_val = res_val[:-1]

        date_val, time_val = res_val.split(" ", maxsplit=1)[0], res_val.split(" ", maxsplit=1)[1]

        # Parsing date delimiters
        if "-" in date_val:
            date_val = date_val.replace("-", ".")
        if "/" in date_val:
            date_val = date_val.replace("/", ".")
        if "," in date_val:
            date_val = date_val.replace(",", ".")

        # Parsing time delimiters
        if "-" in time_val:
            time_val = time_val.replace("-", ":")
        if "/" in time_val:
            time_val = time_val.replace("/", ":")
        if "," in time_val:
            time_val = time_val.replace(",", ".")
        res_val = date_val + " " + time_val
        
    return res_val


def date_to_datetime(source: str | list[str]) -> datetime | list[datetime]:
    """
    Converts a single string value or a list of date values to a list of datetimes.
    Incorrect dates are replaced with min datetime
    :param source: source str value or list of values containing date
    :return: converted date or list of dates
    """
    def single_conversion(source_val: str) -> datetime:
        default_datetime = datetime.min

        new_val = source_val

        # Trying to convert float or int Excel date string to datetime object
        try:
            float_val = float(new_val)
            res = openpyxl.utils.datetime.from_excel(float(float_val))
            if isinstance(res, time):
                res = datetime(year=default_datetime.year, month=default_datetime.month, day=default_datetime.day,
                               hour=res.hour, minute=res.minute, second=res.second)
            elif isinstance(res, date):
                res = datetime(year=res.year, month=res.month, day=res.day)
            return res
        except (ValueError, TypeError, OverflowError, OSError) as err:
            pass

        # Some substitutions if needed
        new_val = parse_date_value(new_val)

        # Trying some common formats
        common_formats = GlobalDefaults.datetime_formats

        for dt_format in common_formats:
            try:
                return datetime.strptime(new_val, dt_format)
            except ValueError:
                continue

        # By default, we will parse everything
        try:
            return parser.parse(new_val, dayfirst=True, fuzzy=True, default=default_datetime)
        except parser.ParserError:
            return default_datetime

    if isinstance(source, str):
        return single_conversion(source)
    elif isinstance(source, list):
        return list(map(single_conversion, source))
    else:
        raise ValueError(f"Incorrect argument, type: {type(source)}")


def get_garage_num(source: list[str] | str) -> str | list[str]:
    """
    Forms a vehicle garage code value or column based on the source
    :param source: list with source values, each value's copy is converted to string by force
    :return: str value or list of parsed code(s)
    """
    search_res = []

    def single_conversion(source_val: str) -> str:
        pattern = re.compile(str(GlobalDefaults.garage_num_pattern))
        _default_na = GlobalDefaults.na_val

        try:
            tmp_res = re.findall(pattern, source_val)
            if len(tmp_res) > 0:
                # Expecting list of tuples
                if isinstance(tmp_res[0], tuple):
                    for list_elem in tmp_res:
                        for tuple_elem in list_elem:
                            if tuple_elem != "":
                                # Any match stripped of possible 0's in the beginning
                                return str(int(tuple_elem))
                # List of strings otherwise
                else:
                    for list_elem in tmp_res:
                        if list_elem != "":
                            return str(int(list_elem))

            # No matches at all
            return GlobalDefaults.na_val

        # Doesn't contain the value at all
        except ValueError as err:
            return GlobalDefaults.na_val

    if isinstance(source, str):
        return single_conversion(source)
    elif isinstance(source, list):
        return list(map(single_conversion, source))
    else:
        raise ValueError(f"Incorrect argument, type: {type(source)}")
