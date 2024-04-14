"""
A module contains parsing methods for production files and forming a report based on parsing
"""

import warnings
import pandas as pd
import excel_operations.io as io
from settings.defaults import _load_production, _load_global, ProductionDefaults, GlobalDefaults


class ProductionDict:
    """ Contains keywords and parsing parameters """
    def __init__(self):
        self.branches = GlobalDefaults.branches
        self.tram_branches = ProductionDefaults.tram_branches
        self.transport_type = ProductionDefaults.transport_type
        self.col_names = ProductionDefaults.col_names
        self.stop_name = ProductionDefaults.stop_name
        self.totals_name = ProductionDefaults.totals_name
        self.prod_name = ProductionDefaults.prod_name
        self.round_name = ProductionDefaults.round_name
        self.cols_to_search = ProductionDefaults.cols_to_search
        self.tram_name = ProductionDefaults.tram_name
        self.pivot_name = GlobalDefaults.pivot_name

        # For selecting the information needed
        self.transport = dict(zip(self.transport_type, [[] for i in range(len(self.transport_type))]))
        self.header_size = 0  # Each mini-table header size
        self.start_col = 0  # Empty columns counter (from the left side)


class Production(ProductionDict):
    """ Performs production reading, parsing and writing to a target file """
    def __init__(self, source_path: str = "", fname: str = "", target_path: str = ""):
        # For containing result
        self.source = None
        self.res_table = {}

        self.fname = fname
        self.target_path = target_path
        self.source_path = source_path

        super().__init__()

        return

    def _get_ttype_boundaries(self) -> list:
        """ Gets indices of each transport type section represented in the source file """
        slice_indices = []

        # Trying to find each possible transport type boundaries
        for elem in self.transport_type:
            try:
                slice_indices.append(
                    self.source.iloc[:, self.start_col].apply(lambda x: str.lower(x),
                                                              convert_dtype=False).values.tolist().index(elem))
            except ValueError as err:
                print(f"Source table doesn't contain {elem}")

        # So we can go through the fine consequentially
        slice_indices.sort()

        return slice_indices

    def _calc_header(self, min_indx: int) -> None:
        """
        Calculates header for each transport type based on pre-defined keywords
        :param min_indx: index of the transport type row
        """
        # The cell with the current bias contains transport type value by default
        self.header_size = 0

        # Calculating the header size for each transport type
        # (cell value should neither be in any of the branches and transport types lists nor be equal to ''
        for row in self.source.iloc[min_indx:, self.start_col].values.tolist():
            # Empty string should be skipped
            if row == "":
                self.header_size += 1
                continue

            # Same for the transport type
            if any([elem in row.lower() for elem in self.transport_type]):
                self.header_size += 1
                continue

            # Means the cell contains summary info which is ok for us
            if any([elem in row.lower() for elem in [self.totals_name, self.stop_name]]):
                break

            # Truncating source string to the max branch name length
            tmp_len = max([len(elem) for elem in self.branches + self.tram_branches])
            if len(row) >= tmp_len:
                row = row[:tmp_len]

            # If not a match after cleaning the random intersections than it's not a branch name
            if all([elem not in row.lower() for elem in self.branches]) and \
                    all([elem not in row.lower() for elem in self.tram_branches]):
                self.header_size += 1
                continue

            break

        return None

    def _form_ttypes_summaries(self, full_res: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        """
        Forms summaries for each of the transport type
        :param full_res: source table partitioned by transport types without any empty strings
        """
        # Cleaning the table for each transport type
        res = dict(zip(self.transport_type, []))
        for ttype in full_res:
            # Tram branches have another names
            if ttype == self.tram_name:
                local_branches = self.tram_branches
            else:
                local_branches = self.branches

            # Adding a dataframe for each transport type
            res[ttype] = pd.DataFrame(columns=self.col_names[self.start_col:], index=local_branches)

            # Iterating through branches
            for branch in local_branches:
                # Saving the first col
                park_col = full_res[ttype].iloc[:, 0].apply(lambda x: str.lower(x)).values.tolist()

                # Looking for each branch there
                target_indx = -1
                for i in range(len(park_col)):
                    if branch in park_col[i]:
                        # Check for something like matching 'Ю' and 'ЮЗ'
                        if len(branch) == 1 and len(park_col[i]) >= 2:
                            if str.isalpha(park_col[i][self.start_col]):
                                continue
                        target_indx = i
                        break

                # No branch was found
                if target_indx == -1:
                    warnings.warn(f"Warning: no '{branch}' branch for '{ttype}' transport type", category=UserWarning)
                    res[ttype].loc[branch, :] = [0] * (len(full_res[ttype].columns) - 1)
                    continue

                # Updating index for a summary row
                park_col = park_col[target_indx:]
                target_indx += park_col.index(self.stop_name)
                res[ttype].loc[branch, :] = full_res[ttype].iloc[target_indx, self.start_col:].apply(lambda x: int(x))

                pass

            # Replacing empty strings with 0's
            res[ttype].replace("", 0, inplace=True)

            # Adding summary row
            res[ttype].loc[self.totals_name] = [0] * len(res[ttype].columns)
            res[ttype].dropna()
            res[ttype].loc[self.totals_name] = res[ttype].sum()
            # If the summary is zeroed trying to pull the summary data from the source table
            if (res[ttype].loc[self.totals_name] == 0).all():
                res[ttype].loc[self.totals_name, :] = full_res[ttype].iloc[full_res[ttype].iloc[:, 0].apply(
                    lambda x: str.lower(x)).values.tolist().index(self.totals_name), self.start_col:].apply(
                    lambda x: int(x))

            # Adding summary columns
            res[ttype][self.prod_name] = [0] * len(res[ttype].index)
            res[ttype][self.round_name] = [0] * len(res[ttype].index)
            # Iterating by rows because we expect some zeroes
            for branch in res[ttype].index.tolist():
                try:
                    res[ttype].loc[branch, self.prod_name] = res[ttype].loc[branch, self.col_names[2]] / \
                                                             res[ttype].loc[branch, self.col_names[1]] * 100.
                except ZeroDivisionError as err:
                    res[ttype].loc[branch, self.prod_name] = 0.
                try:
                    res[ttype].loc[branch, self.round_name] = res[ttype].loc[branch, self.col_names[4]] / \
                                                              res[ttype].loc[branch, self.col_names[3]] * 100.
                except ZeroDivisionError as err:
                    res[ttype].loc[branch, self.round_name] = 0.

        return res

    def _form_total_summary(self, res: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        """
        Forms pivot summary based on transport types used in the source table. Excludes trams
        :param res: result table without overall summary
        """
        # Summing only necessary transport types for target report
        ttypes = list(res.keys())
        if self.tram_name in ttypes:
            ttypes.remove(self.tram_name)
        if len(ttypes) != 0:
            res[self.pivot_name] = pd.DataFrame(0, columns=res[ttypes[0]].columns, index=res[ttypes[0]].index)
            for ttype in ttypes:
                res[self.pivot_name] = res[self.pivot_name].add(res[ttype])
        else:
            res[self.pivot_name] = pd.DataFrame()

        # Filling the result
        for branch in res[self.pivot_name].index.tolist():
            try:
                res[self.pivot_name].loc[branch, self.prod_name] = res[self.pivot_name].loc[branch, self.col_names[2]] / \
                                                         res[self.pivot_name].loc[branch, self.col_names[1]] * 100.
            except ZeroDivisionError as err:
                res[self.pivot_name].loc[branch, self.prod_name] = 0.
            try:
                res[self.pivot_name].loc[branch, self.round_name] = res[self.pivot_name].loc[branch, self.col_names[4]] / \
                                                          res[self.pivot_name].loc[branch, self.col_names[3]] * 100.
            except ZeroDivisionError as err:
                res[self.pivot_name].loc[branch, self.round_name] = 0.

        # Dropping rows with zeros only
        for ttype in list(res.keys()):
            res[ttype] = res[ttype].loc[(res[ttype] != 0).any(axis=1)]

        return res

    def parse_table(self, source: pd.DataFrame) -> dict[pd.DataFrame]:
        """
        Forms several reports: one for each transport type (each includes branches if possible) and a summary one
        :param source: source dataframe read from the xls/xlsx file
        """
        self.source = source

        # Basic emptiness check
        if self.source is None:
            print("The source table doesn't exist. Please, try again")
            return {}
        if self.source.empty:
            print("The source table is empty. Please, try again")
            return {}

        # Counting empty cols (starting for the left border)
        for i in range(len(source.columns)):
            if len(source.index) == len(source[source.iloc[:, i] == ""].index):
                self.start_col += 1

        # Getting each transport type boundaries
        slice_indices = self._get_ttype_boundaries()

        # Trimmed version of a source file partitioned by according transport types
        full_res = dict(zip(self.transport_type, []))

        # Forming a dictionary for each transport type
        for indx in range(len(slice_indices)):
            # Defining slice ranges
            if indx == len(slice_indices) - 1:
                min_indx = slice_indices[indx]
                max_indx = len(source.values)
            else:
                min_indx = slice_indices[indx]
                max_indx = slice_indices[indx + 1]

            # Calculating the current header size
            self._calc_header(min_indx)

            # Looking for target columns for current transport type
            self.transport[source.iloc[min_indx, self.start_col].lower()].append(self.start_col)
            for row in source.iloc[min_indx:min_indx + self.header_size, :].values.tolist():
                for tmp_indx, col in enumerate(row):
                    if self.cols_to_search[0] in col.lower() or self.cols_to_search[1] in col.lower():
                        self.transport[source.iloc[min_indx, self.start_col].lower()].append(tmp_indx)

            # Getting slice, containing rows accordingly to transport type and target cols only, which is defined by
            # each transport type separately. Resetting indices since we want them be renewed
            tmp = pd.DataFrame(source.iloc[min_indx + self.header_size:max_indx,
                               self.transport[source.iloc[min_indx, self.start_col].lower()]]).reset_index(drop=True)
            # Renaming cols with desired names
            tmp.rename(dict(zip(tmp.columns.tolist(), self.col_names)), axis="columns", inplace=True)

            # Filling the target dictionary
            full_res[source.iloc[min_indx, self.start_col].lower()] = tmp

        # Cropping the source table to a summary view
        res = self._form_ttypes_summaries(full_res)

        # Add pivot overall values
        res = self._form_total_summary(res)

        # Moving pivot to the first position
        cols = list(res.keys())
        cols = cols[-1:] + cols[:-1]
        reordered_res = {table: res[table] for table in cols}

        self.res_table = reordered_res

        return reordered_res

    def process_prod(self, source_path: str = "", fname: str = "", target_path: str = ""):
        """ Wrapper for full process cycle """
        # Checking paths
        if source_path != "" and fname != "" and target_path != "":
            self.source_path = source_path
            self.fname = fname
            self.target_path = target_path

        # Reading the source table if it's empty
        if self.source is None:
            tmp = io.read_xlsx_files(self.source_path)
            if len(tmp) == 1:
                self.source = tmp[0]
            if self.source is None:
                print("An error occurred during reading production table. Please, try again.")
                return None

        # Parsing the source table
        self.parse_table(self.source)

        # Dumping to file
        io.form_new_xlsx(self.res_table, self.target_path, file_name=self.fname, index=True)
        print("Done")

        return None


if __name__ == "__main__":
    if not all([_load_production(), _load_global()]):
        exit(0)

    print("Production settings loaded successfully")

    # Some code here
