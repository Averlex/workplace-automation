"""
A module contains parsing methods for system tables which forms ready-to-use pivots
"""
import warnings

import numpy as np

from settings.defaults import SystemDefaults, GlobalDefaults, ResourcesDefaults, BranchesDefaults, ClassifierDefaults
import pandas as pd
from excel_operations.merger import concat_tables

# TODO: the latest changes need to be added via settings.defaults


class Pivots:
    def __init__(self,
                 tasks_table: pd.DataFrame,
                 checks_table: pd.DataFrame,
                 prohibition: SystemDefaults.prohibition = None,
                 branch: ResourcesDefaults.branch = None,
                 park: str = None,
                 top_categories: list = None,
                 observation: str = None,
                 detailed_observation: str = None,
                 contract: str = None,
                 modification: str = None,
                 top_count: int = 5,
                 ):
        """
        Forms a pivot with branches indices and observations count (all and strict prohibitions separately) \n
        Counts total company summary, N/A branch numbers and unit values for each branch and the mentioned categories
        :param tasks_table: source table to calculate a pivot from
        :param checks_table: additional table needed for unit values calculation
        :param prohibition: prohibition column name
        :param branch: branch column name
        :return: pd.DataFrame on success, None otherwise
        """
        self.table = None

        # Basic emptiness check
        if tasks_table is None or checks_table is None:
            print(
                f"One of the tables doesn't exist. Source type: {type(tasks_table)}, checks type: {type(checks_table)}")
            return
        if tasks_table.empty:
            print(f"The tasks table is empty. No pivots could be done")
            return
        if checks_table.empty:
            print(f"The checks table is empty. No pivots will be done")
            return

        # Args processing
        _prohibition = prohibition
        _branch = branch
        _park = park
        _observation = observation
        _modification = modification
        _contract = contract
        _detailed_observation = detailed_observation

        if _prohibition is None:
            _prohibition = SystemDefaults.prohibition
        if _branch is None:
            _branch = BranchesDefaults.branch
        if _observation is None:
            _observation = ClassifierDefaults.new_task
        if _modification is None:
            _modification = ResourcesDefaults.modification
        if _contract is None:
            _contract = SystemDefaults.contract
        if _detailed_observation is None:
            _detailed_observation = ClassifierDefaults.task
        # A bit more advanced validation for this one
        if _park is None:
            _park = str(BranchesDefaults.park_name).lower()
            if _park not in tasks_table.columns.tolist() and _park not in checks_table.columns.tolist():
                _park = str(SystemDefaults.park).lower()

        # Lowercasing the column names
        _prohibition, _branch, _observation, _modification, _contract, _detailed_observation = (
            _prohibition.lower(), _branch.lower(), _observation.lower(), _modification.lower(), _contract.lower(),
            _detailed_observation.lower())

        # Columns validation for tasks table (modification column will be processed separately)
        column_names = [i.lower() for i in tasks_table.columns.tolist()]
        diff = {_prohibition, _branch, _observation, _contract, _park} - set(column_names)
        if len(diff) > 0:
            print(f"Not all of the columns are represented in the tasks table: {diff}. No pivots will be done")
            return

        # The same for the checks table (modification column will be processed separately)
        column_names = [i.lower() for i in checks_table.columns.tolist()]
        diff = {_branch, _contract, _park} - set(column_names)
        if len(diff) > 0:
            print(f"Not all of the columns are represented in the checks table: {diff}. No pivots will be done")
            return

        # Top-N count validation
        parks = [i.lower() for i in tasks_table[_park].unique().tolist()]
        _top_count = abs(top_count)
        if _top_count == 0 or _top_count >= len(parks):
            _top_count = self.__annotations__["top_count"].default

        # Top categories validation
        _top_categories = top_categories
        if _top_categories is None or len(_top_categories) != _top_count:
            _top_categories = (pd.pivot_table(tasks_table, index=_observation, aggfunc=len).
                               fillna(0).nlargest(3, _branch)).index.tolist()

        # Branches validation for the tasks table
        branches_config_list = GlobalDefaults.branches
        branches_table_list = [i.lower() for i in tasks_table[_branch].unique().tolist()]
        diff = set(branches_table_list) - set(branches_config_list) - {GlobalDefaults.na_val}
        if len(diff) > 0:
            print(
                f"Some of the branches from the tasks table are not present in the config: {diff}. No pivots will be done")
            return

        # Branches validation for the checks table
        branches_table_list = [i.lower() for i in checks_table[_branch].unique().tolist()]
        diff = set(branches_table_list) - set(branches_config_list) - {GlobalDefaults.na_val}
        if len(diff) > 0:
            print(
                f"Some of the branches from the checks table are not present in the config: {diff}. No pivots will be done")
            return

        parks_pivot = self._form_parks_pivot(tasks_table, checks_table, _park, _prohibition, _top_count)
        parks_single_obs_pivot = self._form_parks_pivot(tasks_table[tasks_table[_observation] == _top_categories[0]],
                                                        checks_table, _park, _prohibition, _top_count)
        tasks_pivot = self._form_tasks_pivot(tasks_table, checks_table, _prohibition, _observation,
                                             _detailed_observation, _branch, _top_count)
        main_pivot = self._form_main_pivot(tasks_table, checks_table, _prohibition, _branch)

        contract_top_pivot = self._form_contract_top(tasks_table, checks_table, _top_categories, _observation,
                                             _contract, _branch, _modification)

        self.table = {"Main pivot": main_pivot, "Top tasks": tasks_pivot, "Top parks": parks_pivot,
                      "Top contract": contract_top_pivot, "Top parks by category": parks_single_obs_pivot}

        pass

    @staticmethod
    def _form_main_pivot(source_table: pd.DataFrame,
                         checks_table: pd.DataFrame,
                         prohibition: str = None,
                         branch: str = None) -> pd.DataFrame | None:
        """
        Forms a pivot with branches indices and observations count (all and strict prohibitions separately) \n
        Counts total company summary, N/A branch numbers and unit values for each branch and the mentioned categories
        :param source_table: source table to calculate a pivot from
        :param checks_table: additional table needed for unit values calculation
        :param prohibition: prohibition column name
        :param branch: branch column name
        :return: pd.DataFrame on success, None otherwise
        """
        # Organizing pivot table's structure
        branches = GlobalDefaults.branches + [GlobalDefaults.na_val, GlobalDefaults.pivot_name]
        res = pd.DataFrame(columns=["prohibition", "tasks", "checks", "all rel", "prohib rel"], index=branches)

        # Counting the required values
        prohibition_counts = \
            source_table[source_table[prohibition].apply(lambda x: str(x).lower()) == SystemDefaults.prohibition_strict][
                branch].value_counts()
        all_counts = source_table[branch].value_counts()
        checks_counts = checks_table[branch].value_counts()

        # Lowercasing indices
        prohibition_counts.index = prohibition_counts.index.str.lower()
        all_counts.index = all_counts.index.str.lower()
        checks_counts.index = checks_counts.index.str.lower()

        # Filling the pivot table with the source data (without summaries)
        for elem in branches:
            res.loc[elem, :] = 0

            # Filling the table accordingly to the sums
            if elem in prohibition_counts.index.tolist():
                res.loc[elem, "prohibition"] += prohibition_counts.loc[elem]
            if elem in all_counts.index.tolist():
                res.loc[elem, "tasks"] += all_counts.loc[elem]

            # Filling with checks count
            if elem in checks_counts.index.tolist():
                res.loc[elem, "checks"] += checks_counts.loc[elem]

        # Filling the horizontal summary
        checks_sum = checks_counts.sum()
        prohibition_sum = prohibition_counts.sum()
        all_sum = all_counts.sum()
        res.loc[GlobalDefaults.pivot_name, "prohibition"] = prohibition_sum
        res.loc[GlobalDefaults.pivot_name, "checks"] = checks_sum
        res.loc[GlobalDefaults.pivot_name, "tasks"] = all_sum

        # Filling N/A row
        res.loc[GlobalDefaults.na_val, "prohibition"] = prohibition_sum - prohibition_counts.loc[
            GlobalDefaults.branches].sum()
        res.loc[GlobalDefaults.na_val, "checks"] = checks_sum - checks_counts.loc[GlobalDefaults.branches].sum()
        res.loc[GlobalDefaults.na_val, "tasks"] = all_sum - all_counts.loc[GlobalDefaults.branches].sum()

        # Filling the actual N/As
        res.fillna(0, inplace=True)

        # Filling the vertical summary
        for row in branches:
            if res.loc[row, "checks"] != 0:
                res.loc[row, "prohib rel"] += res.loc[row, "prohibition"] / res.loc[row, "checks"]
                res.loc[row, "all rel"] += res.loc[row, "tasks"] / res.loc[row, "checks"]

        return res

    @staticmethod
    def _form_parks_pivot(source_table: pd.DataFrame,
                          checks_table: pd.DataFrame,
                          park: str = None,
                          prohibition: str = None,
                          top_count: int = 5):
        # Organizing pivot table's structure
        parks = [i.lower() for i in source_table[park].unique().tolist()]
        tmp_res = pd.DataFrame(columns=["prohibition", "tasks", "checks", "all rel", "prohib rel"], index=parks)

        # Counting the required values
        prohibition_counts = source_table[source_table[prohibition].apply(lambda x: str(x).lower()) ==
                                          SystemDefaults.prohibition_strict][park].value_counts()
        all_counts = source_table[park].value_counts()
        checks_counts = checks_table[park].value_counts()

        # Lowercasing indices
        prohibition_counts.index = prohibition_counts.index.str.lower()
        all_counts.index = all_counts.index.str.lower()
        checks_counts.index = checks_counts.index.str.lower()

        # Filling the pivot table with the source data (without summaries)
        for elem in parks:
            tmp_res.loc[elem, :] = 0

            # Filling the table accordingly to the sums
            if elem in prohibition_counts.index.tolist():
                tmp_res.loc[elem, "prohibition"] += prohibition_counts.loc[elem]
            if elem in all_counts.index.tolist():
                tmp_res.loc[elem, "tasks"] += all_counts.loc[elem]

            # Filling with checks count
            if elem in checks_counts.index.tolist():
                tmp_res.loc[elem, "checks"] += checks_counts.loc[elem]

        # Filling the vertical summary
        for row in parks:
            if tmp_res.loc[row, "checks"] != 0:
                tmp_res.loc[row, "prohib rel"] += tmp_res.loc[row, "prohibition"] / tmp_res.loc[row, "checks"]
                tmp_res.loc[row, "all rel"] += tmp_res.loc[row, "tasks"] / tmp_res.loc[row, "checks"]

        tmp_res["all rel"] = tmp_res["all rel"].astype(float)
        tmp_res["prohib rel"] = tmp_res["prohib rel"].astype(float)

        # Organizing top-n pivot
        all_sorted = tmp_res.sort_values("all rel", ascending=False)
        strict_sorted = tmp_res.sort_values("prohib rel", ascending=False)
        all_parks = all_sorted.index.tolist()[: top_count]
        strict_parks = strict_sorted.index.tolist()[: top_count]
        new_indices = [("prohibition", elem) for elem in strict_parks] + [("all tasks", elem) for elem in all_parks]
        new_indices = pd.MultiIndex.from_tuples(new_indices, names=["prohibition", "park"])
        res = pd.DataFrame(columns=["tasks", "checks", "rel"], index=new_indices)

        res.loc["prohibition", :] = (
            tmp_res.nlargest(top_count, "prohib rel").loc[:, ["prohibition", "checks", "prohib rel"]].values)
        res.loc["all tasks", :] = (
            tmp_res.nlargest(top_count, "all rel").loc[:, ["tasks", "checks", "all rel"]].values)

        return res

    @staticmethod
    def _form_top_tasks(source_table: pd.DataFrame, checks_table: pd.DataFrame, branch: str = None,
                        observation: str = None, detailed_observation: str = None, top_count: int = 5) -> object:
        _branches = [i.lower() for i in GlobalDefaults.branches]

        # Forming a pivot from the tasks table
        filtered_source = source_table[[branch, observation]]
        pivot_source = pd.pivot_table(filtered_source, index=observation, columns=branch,
                                      aggfunc=np.count_nonzero).fillna(0)
        pivot_source.columns = pivot_source.columns.str.lower()
        indices = []
        for col in GlobalDefaults.branches:
            indices.extend([(col, elem) for elem in pivot_source.nlargest(top_count, col).index.tolist()])

        filtered_checks = checks_table[[BranchesDefaults.branch]]
        filtered_checks.loc[:, ["extra_col"]] = "checks"
        pivot_checks = pd.pivot_table(filtered_checks, index=branch, columns="extra_col",
                                      aggfunc=np.count_nonzero).fillna(0)
        pivot_checks.index = pivot_checks.index.str.lower()

        # Filling the pivot
        res = pd.DataFrame(
            index=pd.MultiIndex.from_tuples(indices, names=[branch, observation]),
            columns=[detailed_observation, "checks"])
        for group in res.index.tolist():
            res.loc[group, detailed_observation] = pivot_source.loc[group[1], group[0]]
            res.loc[group, "checks"] = pivot_checks.loc[group[0], "checks"]

        # Filling the horizontal summary
        res["rel"] = res.loc[:, detailed_observation] / res.loc[:, "checks"]

        # Adding totals to tasks pivot
        pivot_source["company"] = pivot_source.loc[:, GlobalDefaults.branches].apply(lambda row: row.sum(), axis=1)
        # Calculating checks total amount
        total_checks = pivot_checks.apply(lambda col: col.sum(), axis=0).values[0]
        # Taking top tasks overall
        overall_top = pivot_source.nlargest(top_count, "company").index.tolist()
        # Forming indices for summary addition
        indices = [("company", elem) for elem in overall_top]

        # Filling the total summary
        bottom_summary = pd.DataFrame(
            index=pd.MultiIndex.from_tuples(indices, names=[branch, observation]),
            columns=res.columns.tolist())
        for elem in overall_top:
            bottom_summary.loc[("company", elem), detailed_observation] = pivot_source.loc[elem, "company"]
            bottom_summary.loc[("company", elem), "checks"] = total_checks
        bottom_summary.loc[:, "rel"] = bottom_summary.loc[:, detailed_observation] / bottom_summary.loc[:, "checks"]

        # Adding the summary to the pivot
        res = concat_tables([bottom_summary, res], axis="v", drop_indices=False)

        return res

    def _form_tasks_pivot(self, source_table: pd.DataFrame, checks_table: pd.DataFrame, prohibition: str = None,
                          observation: str = None, detailed_observation: str = None, branch: str = None, top_count: int = 3):
        # Forming pivot with all tasks and with strict ones only
        all_pivot = self._form_top_tasks(source_table, checks_table, branch, observation, detailed_observation, top_count)
        strict_pivot = self._form_top_tasks(
            source_table[source_table[prohibition] == SystemDefaults.prohibition_strict],
            checks_table, branch, observation, detailed_observation, top_count)

        # Adds one level to index and strips the task of the first numeric part
        def custom_reidex(index_to_add: str, df: pd.DataFrame):
            new_indices = []
            for indx in df.index.tolist():
                new_indices.append((indx[0], index_to_add, indx[1][indx[1].find(" ") + 1:]))
            df.index = new_indices
            return df

        # Reindexing both pivots
        all_pivot = custom_reidex("all tasks", all_pivot)
        strict_pivot = custom_reidex("prohibition", strict_pivot)

        # Merging them into a single one
        res = concat_tables([all_pivot, strict_pivot], "v", drop_indices=False)

        # Reindexing accordingly to the table indices
        new_indices = pd.MultiIndex.from_tuples(res.index.tolist(), names=[branch, prohibition, observation])
        res.index = new_indices

        # Regrouping indices
        res = res.groupby(level=0, sort=False).apply(lambda x: x).reset_index(level=0, drop=True)

        return res

    @staticmethod
    def _form_contract_top(tasks: pd.DataFrame, checks: pd.DataFrame, top_categories: list = None, observation: str = None,
                       contract: str = None, branch: str = None, modification: str = None) -> pd.DataFrame | None:
        if top_categories is None:
            print("No top categories are set for the pivot. Skipping contract_top pivot...")
            return None

        if len(top_categories) == 0:
            print("No top categories are set for the pivot. Skipping contract_top pivot...")
            return None

        # Filtering tasks by top categories
        filtered_tasks = tasks[tasks[observation].isin(top_categories)]
        tmp_checks = checks

        # A small function for grouping
        def group_modification(contract: str, modification: str):
            tmp = modification.lower()
            contract_condition = contract != "" and contract != GlobalDefaults.na_val
            if "val_1" in tmp and contract_condition:
                res = "VAL_1"
            elif ("val_2" in tmp or "val_2_alt" in tmp) and contract_condition:
                res = "VAL_2"
            elif not contract_condition:
                res = "VAL_3"
            else:
                res = "Other"
            return res

        # Parsing columns to determine a modification column (since it could change its name)
        checks_modification = ""
        tasks_modification = ""
        for col in filtered_tasks.columns.tolist():
            if modification in col:
                tasks_modification = col
        for col in checks.columns.tolist():
            if modification in col:
                checks_modification = col

        tmp_checks.loc[:, ["group"]] = tmp_checks.apply(
            lambda row: group_modification(row[contract], row[tasks_modification]), axis=1)
        filtered_tasks.loc[:, ["group"]] = filtered_tasks.apply(
            lambda row: group_modification(row[contract], row[checks_modification]), axis=1)

        # Forming pivots by branch-groups
        tasks_pivot = (pd.pivot_table(filtered_tasks[[observation, branch, "group"]],
                                      index=[observation, branch], columns="group",
                                      aggfunc=len, margins=True, margins_name="Total").
                       fillna(0).rename_axis(index=[observation, branch]))

        # Calculate the sum for each observation category
        observation_sums = tasks_pivot.groupby(observation).sum()
        observation_sums.index = [(indx, "total") for indx in observation_sums.index.tolist()]

        # Filter out the ("total", "") row aka the original margin
        tasks_pivot = tasks_pivot[~((tasks_pivot.index.get_level_values(observation) == "Total") & (
                tasks_pivot.index.get_level_values(branch) == ""))]

        # Append the bottom margin to the pivot table
        tasks_pivot = concat_tables([tasks_pivot, observation_sums], axis="v")
        checks_pivot = (pd.pivot_table(tmp_checks[[branch, "group"]],
                                       index=branch, columns="group",
                                       aggfunc=len, margins=True, margins_name="total").
                        fillna(0).sort_values(by="total", ascending=False))

        # Saving pivots for later output
        tasks_for_writing = tasks_pivot.copy(deep=True).groupby(level=0, group_keys=False).apply(
            lambda group: group.sort_values(by="total", ascending=False))
        checks_for_writing = checks_pivot.copy(deep=True)

        # Removing unnecessary data
        if "Other" in tasks_pivot.columns.tolist():
            tasks_pivot = tasks_pivot.drop(columns="Other")
        if "Other" in checks_pivot.columns.tolist():
            checks_pivot = checks_pivot.drop(columns="Other")

        # Branches and modification groups validation
        tasks_indices_diff = set(tasks_pivot.index.unique(level=1).tolist()) - set(checks_pivot.index.tolist())
        tasks_columns_diff = set(tasks_pivot.columns.tolist()) - set(checks_pivot.columns.tolist())
        with_errors = False
        if len(tasks_indices_diff) != 0:
            with_errors = True
        if len(tasks_columns_diff) != 0:
            with_errors = True
        # The skip itself, though we return both pivots
        if with_errors:
            warnings.warn(f"Branches or modification groups mismatch for tasks and checks tables: "
                          f"{tasks_indices_diff.union(tasks_columns_diff)}. Skipping contract top pivot...")
            return {"tasks": tasks_for_writing, "checks": checks_for_writing}

        # Creating the result pivot
        res_pivot = pd.DataFrame(index=tasks_pivot.index.unique(level=0).tolist(),
                                 columns=tasks_pivot.columns.tolist()).fillna(0)

        # Filling the result
        for row in res_pivot.index.tolist():
            for col in res_pivot.columns.tolist():
                if checks_pivot.loc["total", col] != 0:
                    res_pivot.loc[row, col] = tasks_pivot.loc[(row, "total"), col] / checks_pivot.loc["total", col]
                else:
                    res_pivot.loc[row, col] = 0

        # Some column reordering and reindexing
        res_pivot = res_pivot[["VAL_3", "VAL_1", "VAL_2", "total"]]
        reindex_list = []
        for indx in res_pivot.index.tolist():
            if " " in indx:
                reindex_list.append(indx[indx.find(" ") + 1:])
            else:
                reindex_list.append(indx)
        res_pivot.index = reindex_list

        result = {"Pivot": res_pivot, "tasks": tasks_for_writing, "checks": checks_for_writing}

        return result


# Some code here
if __name__ == "__main__":
    pass
