"""
A module contains classes which performs basic parsing of the System table and adds necessary columns based on parsing
"""
import warnings

import pandas as pd
import re
from excel_operations.merger import concat_tables, merge_with_table
from excel_operations.excel_utils import transform_date, get_garage_num
from settings.defaults import SystemDefaults, GlobalDefaults, ClassifierDefaults, BranchesDefaults
from typing import Literal


# TODO: add None checks for methods that includes None as a default value
class SystemAddition:
    """
    Class forms a pd.DataFrame based on parametric columns and masks assuming the table's type is type
    """

    def __init__(self,
                 source: pd.DataFrame,
                 task_header: str = None,
                 observation: str = None,
                 creation_date: str = None,
                 direction: str = None,
                 place: str = None,
                 appointed_to: str = None,
                 park: str = None,
                 check_kind: str = None,
                 check_type: str = None,
                 priority: str = None,
                 stages: str = None,
                 observation_pattern: re.Pattern | str = None,
                 date_pattern: str = None,
                 enable_filter: bool = True,
                 file_type: Literal["tasks", "checks"] = "tasks",
                 include_fire_extinguishers: bool = False):
        """
        Forms a pd.DataFrame based on parsed values
        :param source: the source table
        :param task_header: tasks column name
        :param observation: prohibition column name
        :param observation_pattern: a re.Pattern which should be used to form a prohibition category
        :param creation_date: date column name
        :param direction: vehicle direction info column name
        :param place: place column name
        :param appointed_to: appointed to column name
        :param park: park column name
        :param check_kind: check type column name
        :param check_type: check type column name
        :param priority: priority column name
        :param stages: stages column name
        :param date_pattern: a date pattern. By default = SystemDefaults.datetime_format
        :param enable_filter: a flag indicating whether to filter the source table or not
        :param file_type: file type to parse
        :param include_fire_extinguishers: flag indicating whether to include the associated division or not
        :return: a new pd.DataFrame with the targeted values
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

        self._cols_to_add = {}

        # Arguments validation
        _task_header = task_header
        _observation = observation
        _creation_date = creation_date
        _direction = direction
        _place = place
        _check_kind = check_kind
        _observation_pattern = observation_pattern
        _date_pattern = date_pattern
        _appointed_to = appointed_to
        _park = park
        _check_type = check_type
        _priority = priority
        _stages = stages

        # Applying default settings if needed
        if _task_header is None:
            _task_header = SystemDefaults.task_header
        if _observation is None:
            _observation = SystemDefaults.observation
        if _creation_date is None:
            _creation_date = SystemDefaults.creation_date
        if _direction is None:
            _direction = SystemDefaults.direction
        if _place is None:
            _place = SystemDefaults.place
        if _check_kind is None:
            _check_kind = SystemDefaults.check_kind
        if _observation_pattern is None:
            _observation_pattern = re.compile(str(SystemDefaults.observation_pattern))
        if _date_pattern is None:
            _date_pattern = SystemDefaults.datetime_format
        if _appointed_to is None:
            _appointed_to = SystemDefaults.appointed_to
        if _park is None:
            _park = SystemDefaults.park
        if _check_type is None:
            _check_type = SystemDefaults.check_type
        if _priority is None:
            _priority = SystemDefaults.priority
        if _stages is None:
            _stages = SystemDefaults.stages

        # Parsing headers
        task_col, observation_col, date_col, direction_col = [], [], [], []
        for elem in source.columns.tolist():
            if _task_header in elem:
                task_col.extend(source[elem].values.tolist())
            elif _observation in elem:
                observation_col.extend(source[elem].values.tolist())
            elif _creation_date in elem:
                date_col.extend(source[elem].values.tolist())
            elif _direction in elem:
                direction_col.extend(source[elem].values.tolist())

        # Forming cols and adding them to the source table
        additional_table = self.form_cols(task_col, observation_col, _observation_pattern, date_col,
                                          direction_col, str(_date_pattern), include_fire_extinguishers)

        # Checking intersections before merging
        diff = set(source.columns.tolist()).intersection(set(additional_table.columns.tolist()))
        if len(diff) != 0:
            warnings.warn(
                f"Some columns in additional table overlaps the source ones: {diff}, dropping additional ones")
            additional_table.drop(columns=list(diff), inplace=True)
        self.table = concat_tables([source, additional_table], axis="h", drop_indices=True)

        # Additional columns from classifier
        if file_type == "tasks":
            self.table = merge_with_table(col_names=[ClassifierDefaults.new_task, ClassifierDefaults.system],
                                          source_table=self.table, target_table=ClassifierDefaults.table,
                                          source_key_names=_observation,
                                          target_key_names=ClassifierDefaults.task, add_suffix=False)

        # Additional columns from branches table
        self.table = merge_with_table(col_names=[BranchesDefaults.branch, BranchesDefaults.park_name],
                                      source_table=self.table, target_table=BranchesDefaults.table,
                                      source_key_names=_park,
                                      target_key_names=BranchesDefaults.old_park_name, add_suffix=False)

        # Filtering if necessary
        _res_direction = SystemDefaults.res_direction
        if enable_filter:
            if file_type == "tasks":
                self.table = self.filter_tasks(self.table, _observation, _place, _check_kind, _res_direction, _park,
                                               _stages, _priority, _task_header)
            elif file_type == "checks":
                self.table = self.filter_checks(self.table, _creation_date, _observation, _check_kind, _task_header,
                                                _appointed_to, _park, _res_direction, _check_type, _priority, _stages)
            else:
                print("Incorrect file type! No filters will be applied")
                pass
        else:
            pass

        return

    @staticmethod
    def _form_garage_num(source_col: list[str]) -> list[str]:
        """
        Forms a vehicle garage code column based on the source column and a pattern for regex parsing
        :param source_col: list with source values, each value's copy is converted to string by force
        :return: list of parsed codes
        """
        return get_garage_num(source_col)

    @staticmethod
    def _form_prohibition(source_col: list, pattern: re.Pattern) -> list[str]:
        """
        Forms the task categories (final version: used to avoid empty values)
        :param source_col: source task category column
        :param pattern: compiled regex pattern for matching
        :return: a column of the task categories without any empty values
        """
        search_res = []

        for indx, row_elem in enumerate(source_col):
            try:
                tmp_res = re.findall(pattern, row_elem)
            except ValueError as err:
                search_res.append(row_elem)
                continue
            else:
                if len(tmp_res) > 0:
                    search_res.append(SystemDefaults.prohibition_strict)
                else:
                    search_res.append(SystemDefaults.prohibition_all)

        return search_res

    @staticmethod
    def _form_dates(source_col: list[str], date_pattern: str = None) -> pd.DataFrame:
        """
        Forms a pd.DataFrame with year, month, calendar day and hour in string format. Mostly just a local wrapper
        :param source_col: source column for parsing
        :param date_pattern: a date pattern. By default = SystemDefaults.datetime_format
        :return: a dictionary with month and year columns
        """
        _date_pattern = date_pattern
        if _date_pattern is None:
            _date_pattern = SystemDefaults.datetime_format

        return transform_date(source_col, _date_pattern, datetime=True, year=True, month=True, day=True, hour=True,
                              week=True)

    @staticmethod
    def _form_direction(source_direction: list[str], source_hour: list[str]) -> list[str]:
        """
        Forms a column with the final version of a vehicle direction value. If the source value is empty
        then during a time period between 4:00 and 12:59 we suppose that the direction is SystemDefaults.direction_out
        :param source_direction: source direction column for parsing
        :param source_hour: source hour column for parsing
        :return: a list with the final version of the target value
        """
        res = []
        source = [str(i) for i in source_direction]

        for direction, hour in zip(source, source_hour):
            if len(direction) > 0:
                res.append(direction)
            else:
                if int(hour) in range(4, 13):
                    res.append(SystemDefaults.direction_out)
                else:
                    res.append(SystemDefaults.direction_in)

        return res

    @staticmethod
    def _form_fire_ext(prohibition):
        """
        Forms an additional column for a desired task. Parses the source column in attempt to find one of the 2 keywords
        :param prohibition: source column
        :return: additional column
        """
        res = [SystemDefaults.fire_ext_na] * len(prohibition)
        for i in range(len(prohibition)):
            if SystemDefaults.fire_ext_add_key in prohibition[i]:
                res[i] = SystemDefaults.fire_ext_add
            elif SystemDefaults.fire_ext_main_key in prohibition[i]:
                res[i] = SystemDefaults.fire_ext_main

        return res

    @staticmethod
    def filter_tasks(source, observation: str = None, place: str = None,
                     check_kind: str = None, direction: str = None, park: str = None,
                     stages: str = None, priority: str = None, task_header: str = None) -> pd.DataFrame:
        """
        Method performs filtering based on several default parameters
        :param source: source table to filter
        :param observation: observation column name
        :param place: place column name
        :param check_kind: check type column name
        :param direction: direction column name
        :param park: park column name
        :param stages: stages column name
        :param priority: priority column name
        :param task_header: task header column name
        :return: filtered pd.DataFrame
        """
        # Arguments preprocessing
        _observation = str(observation).lower()
        _place = str(place).lower()
        _check_kind = str(check_kind).lower()
        _direction = str(direction).lower()
        _park = str(park).lower()
        _stages = str(stages).lower()
        _priority = str(priority).lower()
        _task_header = str(task_header).lower()

        # Column names validation
        target_cols = {_observation, _place, _check_kind, _direction, _park, _stages, _priority, _task_header}
        source_cols = set(source.columns.tolist())
        if not target_cols <= source_cols:
            diff = target_cols - source_cols
            warnings.warn(
                f"Some of the target columns are not represented in the source table: {diff}. "
                f"No filtering for tasks table will be done")
            return source

        # Preparing variables and conditions
        observation_filter = SystemDefaults.observation_filter
        forbidden_header_vals = str(SystemDefaults.forbidden_header_vals).lower()
        allowed_direction = str(SystemDefaults.direction_out).lower()
        forbidden_parks = str(SystemDefaults.forbidden_parks).lower()
        allowed_stages = [elem.lower() for elem in SystemDefaults.allowed_stages]
        allowed_check_kind = str(SystemDefaults.allowed_check_kind).lower()
        allowed_priority = [elem.lower() for elem in SystemDefaults.tasks_allowed_priority]

        place_condition = source[_place] == ""
        check_kind_condition = ((source[_check_kind].apply(lambda x: str(x).lower()) == allowed_check_kind) |
                                (source[_check_kind] == ""))
        park_condition = ~(source[_park].apply(lambda x: str(x).lower()).str.match(forbidden_parks) |
                           (source[_park] == ""))
        header_condition = ~(source[_task_header].apply(lambda x: str(x).lower()).str.match(forbidden_header_vals))
        observation_condition = source[_observation].str.match(observation_filter)
        stages_condition = source[_stages].apply(lambda x: str(x).lower()).isin(allowed_stages)
        priority_condition = source[_priority].apply(lambda x: str(x).lower()).isin(allowed_priority)
        direction_condition = source[_direction].apply(lambda x: str(x).lower()) == allowed_direction

        # Result condition
        total_condition = \
            observation_condition & place_condition & check_kind_condition & direction_condition & header_condition & \
            park_condition & stages_condition & priority_condition

        # Filtering the table
        res = source[total_condition]

        res.reset_index(drop=True, inplace=True)

        # Dealing with NaNs
        res = res.dropna(subset=[_observation])
        res.fillna(GlobalDefaults.na_val, inplace=True)

        print("Task table filtered successfully")
        return res

    @staticmethod
    def filter_checks(source, creation_date: str = None,
                      observation: str = None, check_kind: str = None,
                      task_header: str = None, appointed_to: str = None,
                      park: str = None, direction: str = None, check_type: str = None,
                      priority: str = None, stages: str = None) -> pd.DataFrame:
        """
        Method performs filtering based on several default parameters
        :param park: park column name
        :param appointed_to: appointed_to column name
        :param task_header: task_header column name
        :param creation_date: creation_date column name
        :param source: source table to filter
        :param observation: observation column name
        :param direction: direction column name
        :param check_kind: check kind column name
        :param stages: stages column name
        :param priority: priority column name
        :param check_type: check type column name
        :return: filtered pd.DataFrame
        """
        # Arguments preprocessing
        _appointed_to = str(appointed_to).lower()
        _park = str(park).lower()
        _task_header = str(task_header).lower()
        _observation = str(observation).lower()
        _check_kind = str(check_kind).lower()
        _creation_date = str(creation_date).lower()
        _direction = str(direction).lower()
        _check_type = str(check_type).lower()
        _stages = str(stages).lower()
        _priority = str(priority).lower()

        # Column names validation
        target_cols = {_creation_date, _observation, _check_type, _task_header, _appointed_to, _park, _check_type,
                       _stages, _priority}
        source_cols = set(source.columns.tolist())
        if not target_cols <= source_cols:
            diff = target_cols - source_cols
            warnings.warn(f"Some of the target columns are not represented in the source table: {diff}. "
                          f"No filtering for checks table will be done")
            return source

        # Filter params
        appointed_to_check_vals = str(SystemDefaults.appointed_to_check_vals).lower()
        forbidden_header_vals = str(SystemDefaults.forbidden_header_vals).lower()
        forbidden_parks = str(SystemDefaults.forbidden_parks).lower()
        allowed_direction = str(SystemDefaults.direction_out).lower()
        allowed_check_kind = str(SystemDefaults.allowed_check_kind).lower()
        allowed_check_type = str(SystemDefaults.allowed_check_type).lower()
        allowed_priority = str(SystemDefaults.checks_allowed_priority).lower()
        forbidden_stages = [elem.lower() for elem in SystemDefaults.forbidden_stages]

        # Preparing conditions
        # TODO: unify tasks and checks filtering as much as possible (seems not at least in appropriate way)
        observation_condition = source[_observation].apply(lambda x: str(x).lower()) == allowed_check_kind
        check_kind_condition = (source[_check_kind].apply(lambda x: str(x).lower()) == allowed_check_kind) | \
                               (source[_check_kind] == "")
        check_type_condition = source[_check_type].apply(lambda x: str(x).lower()) == allowed_check_type
        priority_condition = source[_priority].apply(lambda x: str(x).lower()) == allowed_priority
        appointed_condition = (source[_appointed_to].apply(lambda x: str(x).lower()) == appointed_to_check_vals) | \
                              (source[_appointed_to] == "")
        header_condition = ~(source[_task_header].apply(lambda x: str(x).lower()).str.match(forbidden_header_vals))
        park_condition = ~(source[_park].apply(lambda x: str(x).lower()).str.match(forbidden_parks) |
                           (source[_park] == ""))
        direction_condition = source[_direction].apply(lambda x: str(x).lower()) == allowed_direction
        stages_condition = ~(source[_stages].apply(lambda x: str(x).lower()).isin(forbidden_stages))

        # Result condition
        total_condition = \
            observation_condition & check_type_condition & appointed_condition & header_condition & park_condition & \
            direction_condition & check_kind_condition & priority_condition & stages_condition

        # Filtering the table
        res = source[total_condition]

        # Adding datetime column
        if GlobalDefaults.parsed_date not in res.columns.tolist():
            res = concat_tables([res, transform_date(res[_creation_date].values.tolist(), SystemDefaults.datetime_format,
                                                     year=True, month=True, day=True)], axis="h", drop_indices=True)
        # Sort DataFrame by datetime column in ascending order
        res = res.sort_values(GlobalDefaults.parsed_date)

        # Drop duplicates based on 'Date' and 'task_header' columns
        res = res.drop_duplicates(subset=[GlobalDefaults.day, GlobalDefaults.month, GlobalDefaults.year, _task_header])

        # Dealing with NaNs and additional columns
        res.drop(columns=[GlobalDefaults.parsed_date, GlobalDefaults.day, GlobalDefaults.month, GlobalDefaults.year],
                 inplace=True)
        res = res.dropna(subset=[_task_header])
        res.fillna(GlobalDefaults.na_val, inplace=True)

        res.reset_index(drop=True, inplace=True)

        print("Checks table filtered successfully")
        return res

    def form_cols(self,
                  task_header: list[str],
                  observation: list[str],
                  observation_pattern: re.Pattern,
                  creation_date: list[str],
                  direction: list[str],
                  date_pattern: str = None,
                  include_fire_extinguishers: bool = False
                  ) -> pd.DataFrame:
        """
        Forms a pd.DataFrame based on parsed values
        :param task_header: source column containing task names
        :param observation: source column containing task type
        :param observation_pattern: a re.Pattern which should be used to form a prohibition category
        :param creation_date: source date column
        :param direction: source column containing a vehicle direction info
        :param date_pattern: a date pattern. By default = SystemDefaults.datetime_format
        :param include_fire_extinguishers: flag indicating whether to include the associated division or not
        :return: a new pd.DataFrame with the targeted values
        """
        _date_pattern = date_pattern
        if _date_pattern is None:
            _date_pattern = SystemDefaults.datetime_format

        # Adding base columns
        self._cols_to_add[SystemDefaults.garage_num] = self._form_garage_num(task_header)
        self._cols_to_add[SystemDefaults.prohibition_strict] = self._form_prohibition(observation, observation_pattern)
        if include_fire_extinguishers:
            self._cols_to_add[SystemDefaults.fire_ext] = self._form_fire_ext(observation)

        # Adding some dates
        dates_frame = self._form_dates(creation_date, _date_pattern)
        self._cols_to_add[SystemDefaults.res_direction] = \
            self._form_direction(direction, dates_frame[GlobalDefaults.hour].values.tolist())

        # Finalizing the addition
        res = pd.DataFrame().from_dict(self._cols_to_add)
        res = concat_tables([res, dates_frame], axis="h", drop_indices=True)

        return res
