"""
A module contains global utility functions
"""

import os
import time


def form_file_name(target_fname: str, target_fpath: str) -> str:
    """
    A simple function that performs file name and path checks. May add a datetime to the filename if it already exists
    by the chosen path
    :param target_fname: target file name (without extension)
    :param target_fpath: target path to store the file
    :return: full file path including the name and extenstion
    """
    name = target_fname
    if not os.path.isdir(f"{target_fpath}"):
        # Normalizing the path
        if "\\" in target_fpath:
            target_fpath = target_fpath.replace("\\", "/")

        try:
            os.mkdir(target_fpath)
        except FileExistsError as err:
            print(f"Folder '{target_fpath}' already exists, file will be placed there")

    # Forming file name (add the datetime if the file exists)
    if os.path.exists(f"{target_fpath}/{name}.xlsx"):
        # If the file already exists, stamping its name with current datetime
        print(f"File '{name}' already exists. Stamping file name with the current datetime")
        name = name + time.strftime(" %d.%m.%Y %H-%M-%S")
    name = target_fpath + "/" + name + ".xlsx"

    return name


def validate_arg_type(arg: str | list) -> list:
    """
    Default list | str argument validator
    :param arg: source arg
    :return: list of source arg(s)
    :raises ValueError: on NoneType argument
    """
    if arg is None:
        raise ValueError(f"col_names argument has invalid type: {type(arg)}")

    if isinstance(arg, str):
        return [arg]
    else:
        return arg

