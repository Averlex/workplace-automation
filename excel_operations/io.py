"""
A module contains common project read-write functions
"""

import os
from datetime import datetime
import pandas as pd
import xlrd
from pathos.multiprocessing import ProcessingPool as Pool
import multiprocessing as mp
from more_itertools import chunked_even
import glob
import warnings
from utils.utils import form_file_name


def read_xlsx_files(
                    xlsx_files_paths: list[str] | str = None,
                    mp_support: bool = True,
                    extensions: list[str] = None,
                    fname_stamp: bool = True,
                    date_stamp: bool = False
                    ) -> list[pd.DataFrame]:
    """
    Scans the folder and reads all Excel files from it. Assigning a number of file batches to separate processes
    :param xlsx_files_paths: list with full file paths, full folder paths or none. If none set, method takes all Excel files in the current folder
    :param mp_support: enabling multiprocessing support
    :param extensions: supported extensions. xls and xlsx by default
    :param date_stamp: a flag indicating that the table requires a separate column containing reading timestamp mark
    :param fname_stamp: a flag indicating that the table requires a separate column containing file name
    :return: a list of pd.DataFrames
    :raises OSError: if no file paths were set for the reading
    """
    default_extensions = ["xls", "xlsx"]

    # Paths check
    if xlsx_files_paths is None:
        raise OSError("No file paths are set for the reading")

    # Extensions check
    if extensions is None:
        extensions = default_extensions

    # Converting the argument type if necessary
    new_path_list = []
    if isinstance(xlsx_files_paths, str):
        new_path_list.append(xlsx_files_paths)
    else:
        new_path_list.extend(xlsx_files_paths)

    # Parsing paths: if folders found, converting them to a single list of file paths
    tmp_path_list = []
    for indx, path in enumerate(new_path_list):
        if os.path.isdir(path):
            for ext in extensions:
                tmp_path_list.extend(glob.glob(os.path.join(path, f"*.{ext}")))
        else:
            tmp_path_list.append(path)

    # Since it might be extended during parsing
    new_path_list = tmp_path_list
    files_num = len(new_path_list)
    if files_num <= 0:
        print("No files to read by the current path(s)")
        return []

    xlsx_files = []
    res = []
    print("Initializing file reading...")

    # Multiprocess reading by batches
    if mp_support:
        # Getting cores count
        n_cores = mp.cpu_count()

        # Forming batches with file names based on max available batch size (up to n = cpu_cores batches)
        batches = []
        if files_num > n_cores:
            # files_num // n_cores + 1 = batch length ||| 1 <= process_quantity <= cpu_cores
            batches = list(chunked_even(new_path_list, files_num // n_cores + 1))
        else:
            batches = list(chunked_even(new_path_list, 1))

        # Packing values for multiprocessing, assigning tasks to different processes
        batches = [{"paths": i, "fname_stamp": fname_stamp, "date_stamp": date_stamp} for i in batches]
        with Pool(nodes=len(batches)) as proc:
            results = proc.map(raw_xlsx_reading, batches, chunksize=1)

        # Merging the items of sublists into a single list
        res = [item for res_batch in results for item in res_batch]

    # Consequential reading
    else:
        res = raw_xlsx_reading(**{"paths": new_path_list, "fname_stamp": fname_stamp, "date_stamp": date_stamp})

    print(f"Total files read: {len(res)}")

    return res


def raw_xlsx_reading(*args, **kwargs) -> list[pd.DataFrame]:
    """
    Reads a batch of xlsx or xls files. Sorts the data frames by column order
    :keyword paths: list containing file paths to read
    :keyword fname_stamp: a flag indicating that the table requires a separate column containing file name
    :keyword date_stamp: a flag indicating that the table requires a separate column containing reading timestamp mark
    :return: a list of read pd.DataFrames
    """
    xlsx_files_paths = []
    fname_stamp = True
    date_stamp = False
    # Arguments unpacking
    try:
        param_dict = args[0]
        if isinstance(param_dict["paths"], list):
            xlsx_files_paths = param_dict["paths"]
        if isinstance(param_dict["fname_stamp"], bool) and isinstance(param_dict["date_stamp"], bool):
            fname_stamp = param_dict["fname_stamp"]
            date_stamp = param_dict["date_stamp"]
    except IndexError as err:
        if not kwargs:
            raise ValueError("No arguments passed to a function")
        xlsx_files_paths = kwargs.get("paths", [])
        fname_stamp = kwargs.get("fname_stamp", True)
        date_stamp = kwargs.get("date_stamp", False)

    xlsx_files: list[pd.DataFrame] = []
    error_paths: list[str] = []

    # Loop over the list of xlsx files with reading them, disabling openpyxl warnings
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        for f in xlsx_files_paths:
            is_error = False
            try:
                xlsx_files.append(pd.read_excel(f, na_filter=False, dtype=str))

                # Getting pure file name
                fpath = f
                if "\\" in f:
                    fpath = f.replace("\\", "/")
                fname = fpath[fpath.rfind(r"/") + 1: len(f)]

                print(f"File '{fname}' read successfully")

                # Stamping with fname
                if fname_stamp:
                    xlsx_files[-1]["файл"] = fname

                # Stamping with date
                if date_stamp:
                    xlsx_files[-1]["дата чтения"] = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
            # Expected errors
            except (FileNotFoundError, PermissionError) as err:
                print(f"No file was found by {f}, error message: {err.__str__()}. Skipping...")
                is_error = True
            except UnicodeDecodeError as err:
                print(f"An error occurred during file reading: {f}, error message: {err.__str__()}. Skipping...")
                is_error = True
            except (xlrd.biffh.XLRDError, pd.errors.ParserError) as err:
                print(f"An error occurred during file reading: {f}, error message: {err.__str__()}. "
                      f"Please, check the file data and/or try to re-save it. Skipping...")
                is_error = True
            # Skipping the file if an error occurred
            finally:
                if is_error:
                    error_paths.append(f)
                    continue

            # Converting the headers of the last DataFrame read to lowercase
            xlsx_files[-1].columns = xlsx_files[-1].columns.str.lower()

    if len(error_paths) == 0:
        print(f"Worker at {os.getpid()}: {len(xlsx_files_paths)} file(s) read successfully")
    else:
        print(f"Files read successfully: {len(xlsx_files_paths) - len(error_paths)}, with errors: {len(error_paths)}")

    return xlsx_files


def _write_sheets(table: pd.DataFrame | dict[pd.DataFrame] | dict[dict[pd.DataFrame]],
                  full_path: str,
                  index: bool = False) -> None:
    """
    A small wrapper for table writing
    :param table: source table for dumping or a dictionary with tables to write separate sheets in one file. dict[dict[pd.DataFrame]] might be passed to write several DataFrames on a single sheet
    :param full_path: full path to the target file (including file name and extension)
    :param index: flag defining whether write indices to the file or not (same to pandas to_excel() parameter)
    :return: None
    """
    if table is None:
        warnings.warn("Empty table was received as a source table argument for writing. Skipping the table...")
        return

    with pd.ExcelWriter(f"{full_path}") as writer:
        # Single sheet
        if isinstance(table, pd.DataFrame):
            if table.empty:
                warnings.warn("Empty table was received as a source table argument for writing. Skipping the table...")
                return None
            table.to_excel(writer, index=index)
        # Multiple sheets
        elif isinstance(table, dict):
            for item in table.keys():
                # Single table for a sheet
                if table[item] is None:
                    warnings.warn(f"None table <{item}> was received as a source table argument for writing. "
                                  f"Skipping the table...")
                    continue
                if isinstance(table[item], pd.DataFrame):
                    if table[item].empty:
                        warnings.warn(f"Empty table <{item}> was received as a source table argument for writing. "
                                      f"Skipping the table...")
                        continue
                    table[item].to_excel(writer, sheet_name=item, index=index)
                # Multiple tables for a sheet
                elif isinstance(table[item], dict):
                    startcol = 0
                    for elem in table[item].keys():
                        if table[item][elem] is None:
                            warnings.warn(f"None table <{item}/{elem}> was received as a source table argument "
                                          f"for writing. Skipping the table...")
                            continue
                        if table[item][elem].empty:
                            warnings.warn(f"Empty table <{item}/{elem}> was received as a source table argument "
                                          f"for writing. Skipping the table...")
                            continue
                        table[item][elem].to_excel(writer, sheet_name=item, index=index, startcol=startcol)
                        startcol += len(table[item][elem].columns.tolist()) + 1
                        if index:
                            startcol += table[item][elem].index.nlevels
                else:
                    warnings.warn(
                        f"Incorrect table type {item} was received as a source table argument for writing. "
                        f"Skipping the table...")
                    continue
        else:
            warnings.warn(
                "Incorrect table type was received as a source table argument for writing. Skipping the table...")

    return None


def form_new_xlsx(table: pd.DataFrame | dict[pd.DataFrame] = None,
                  target_address: str = "../",
                  dir_name: str = "",
                  file_name: str = "Свод",
                  index: bool = False
                  ) -> None:
    """
    Method forms a new .xlsx file based on pd.DataFrame object. Uses pd.DataFrame.to_xlsx.
    File is placed into a subfolder which may already exist
    :param table: source table for dumping or a dictionary with tables to write separate sheets in one file
    :param target_address: target adress for writing
    :param dir_name: preferable subfolder name
    :param file_name: preferable file name. Adding timestamp to the name if the file already exists
    :param index: flag defining whether write indices to the file or not (same to pandas to_excel() parameter)
    :return: None
    """
    if table is None:
        warnings.warn("<NoneType> received as a source table argument for writing. Skipping the table...")
        return None

    # Desired directory name and actual directory path
    if target_address == "":
        target_address = form_new_xlsx.__annotations__["target_address"].default

    # String maintenance with directory, creating a subfolder
    print("Forming a new file. This may take a while...")
    full_path = ""
    if dir_name != "":
        full_path = form_file_name(file_name, target_address + "/" + dir_name)
    else:
        full_path = form_file_name(file_name, target_address)

    # Writing a table to the file (multiple sheets if needed)
    try:
        _write_sheets(table, full_path, index)
    # Emergency backup if possible
    except OSError as err:
        full_path = r"../emergency_dumps"
        print(f"Unknown exception {type(err)} caught during writing the file: {err.__str__()}. "
              f"Path for dumping: {full_path}")
        full_path = form_file_name("emergency_dump", full_path)
        _write_sheets(table, full_path, index)

        return None

    print(f"File formed successfully at {full_path}")
    return None
