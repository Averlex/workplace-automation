""" Module contains default settings loaders """
import json
import os
from typing import Any
import pandas as pd


class GlobalDefaults:
    config = "global.json"
    parsed_date: Any = None
    na_val: Any = None
    datetime_formats: Any = None
    datetime_preferred_format: Any = None
    date: Any = None
    year: Any = None
    month: Any = None
    day: Any = None
    hour: Any = None
    week: Any = None
    file: Any = None
    resources_suffix: Any = None
    branches_suffix: Any = None
    garage_num_pattern: Any = None
    branches: Any = None
    old_branches: Any = None
    pivot_name: Any = None

    def __init__(self):
        pass


class SystemDefaults:
    config = "system.json"
    task_header: Any = None
    observation: Any = None
    creation_date: Any = None
    direction: Any = None
    direction_in: Any = None
    direction_out: Any = None
    res_direction: Any = None
    observation_pattern: Any = None
    datetime_format: Any = None
    prohibition: Any = None
    prohibition_all: Any = None
    prohibition_strict: Any = None
    fire_ext_na: Any = None
    fire_ext: Any = None
    fire_ext_main: Any = None
    fire_ext_main_key: Any = None
    fire_ext_add: Any = None
    fire_ext_add_key: Any = None
    garage_num: Any = None
    place: Any = None
    park: Any = None
    contract: Any = None
    check_kind: Any = None
    allowed_check_kind: Any = None
    check_type: Any = None
    allowed_check_type: Any = None
    priority: Any = None
    tasks_allowed_priority: Any = None
    checks_allowed_priority: Any = None
    stages: Any = None
    allowed_stages: Any = None
    forbidden_stages: Any = None
    observation_filter: Any = None
    appointed_to: Any = None
    appointed_to_check_vals: Any = None
    forbidden_header_vals: Any = None
    forbidden_parks: Any = None

    def __init__(self):
        pass


class ResourcesDefaults:
    config = "resources.json"
    date: Any = None
    park: Any = None
    branch: Any = None
    garage_num: Any = None
    modification: Any = None
    age: Any = None
    state_number: Any = None
    vin: Any = None
    contract: Any = None
    vehicle_class: Any = None
    vehicle_class_list: Any = None
    datetime_format: Any = None

    def __init__(self):
        pass


class ProductionDefaults:
    config = "production.json"
    tram_branches: Any = None
    transport_type: Any = None
    col_names: Any = None
    stop_name: Any = None
    totals_name: Any = None
    prod_name: Any = None
    round_name: Any = None
    cols_to_search: Any = None
    tram_name: Any = None

    def __init__(self):
        pass


class ClassifierDefaults:
    source = "Классификатор.xlsx"
    config = "classifier.json"
    task: Any = None
    new_task: Any = None
    system: Any = None
    table: Any = None


class BranchesDefaults:
    source = "Филиалы.xlsx"
    config = "branches.json"
    address: Any = None
    code: Any = None
    old_park_name: Any = None
    park_name: Any = None
    old_branch: Any = None
    branch: Any = None
    sap_branch: Any = None
    for_pptx: Any = None
    table: Any = None


def _try_loading(**arg) -> dict | None:
    """
    Function implements pure config file reading
    :param arg: arguments for open() function
    :return: default settings dictionary, None on failure
    """
    possible_paths = ["", "../", "../settings/", "settings/"]
    res_path = ""
    for path in possible_paths:
        res_path = path + arg.get('file')
        if os.path.isfile(res_path):
            break
    try:
        with open(file=res_path, mode=arg.get("mode"), encoding=arg.get("encoding")) as file:
            res = json.load(file)
            return res
    except (OSError, json.JSONDecodeError) as err:
        print(f"Error during loading config {arg.get('file')}: unable to open the file or it contains incorrect data")
        return None


def _load_global() -> bool:
    """
    Global settings loader
    :return: True on success, False otherwise
    """
    global_defaults = _try_loading(file=GlobalDefaults.config, mode="r", encoding="cp1251")
    if global_defaults is None:
        return False

    try:
        GlobalDefaults.parsed_date = global_defaults["parsed_date"]
        GlobalDefaults.na_val = global_defaults["na_val"]
        GlobalDefaults.datetime_formats = global_defaults["datetime_formats"]
        GlobalDefaults.datetime_preferred_format = global_defaults["datetime_preferred_format"]
        GlobalDefaults.date = global_defaults["date"]
        GlobalDefaults.year = global_defaults["year"]
        GlobalDefaults.month = global_defaults["month"]
        GlobalDefaults.day = global_defaults["day"]
        GlobalDefaults.hour = global_defaults["hour"]
        GlobalDefaults.week = global_defaults["week"]
        GlobalDefaults.file = global_defaults["file"]
        GlobalDefaults.resources_suffix = global_defaults["resources_suffix"]
        GlobalDefaults.branches_suffix = global_defaults["branches_suffix"]
        GlobalDefaults.garage_num_pattern = global_defaults["garage_num_pattern"]
        GlobalDefaults.branches = global_defaults["branches"]
        GlobalDefaults.old_branches = global_defaults["old_branches"]
        GlobalDefaults.pivot_name = global_defaults["pivot_name"]
    except KeyError as err:
        print("Some parameters are missing in global_defaults.json config. Please, verify config file and try again")
        return False

    return True


def _load_system() -> bool:
    """
    System settings loader
    :return: True on success, False otherwise
    """
    system_defaults = _try_loading(file=SystemDefaults.config, mode="r", encoding="cp1251")
    if system_defaults is None:
        return False

    try:
        SystemDefaults.task_header = system_defaults["task_header"]
        SystemDefaults.observation = system_defaults["observation"]
        SystemDefaults.creation_date = system_defaults["creation_date"]
        SystemDefaults.direction = system_defaults["direction"]
        SystemDefaults.direction_in = system_defaults["direction_in"]
        SystemDefaults.direction_out = system_defaults["direction_out"]
        SystemDefaults.res_direction = system_defaults["res_direction"]
        SystemDefaults.observation_pattern = system_defaults["observation_pattern"]
        SystemDefaults.datetime_format = system_defaults["datetime_format"]
        SystemDefaults.prohibition = system_defaults["prohibition"]
        SystemDefaults.prohibition_all = system_defaults["prohibition_all"]
        SystemDefaults.prohibition_strict = system_defaults["prohibition_strict"]
        SystemDefaults.fire_ext_na = system_defaults["fire_ext_na"]
        SystemDefaults.fire_ext = system_defaults["fire_ext"]
        SystemDefaults.fire_ext_main = system_defaults["fire_ext_main"]
        SystemDefaults.fire_ext_main_key = system_defaults["fire_ext_main_key"]
        SystemDefaults.fire_ext_add = system_defaults["fire_ext_add"]
        SystemDefaults.fire_ext_add_key = system_defaults["fire_ext_add_key"]
        SystemDefaults.garage_num = system_defaults["garage_num"]
        SystemDefaults.place = system_defaults["place"]
        SystemDefaults.park = system_defaults["park"]
        SystemDefaults.contract = system_defaults["contract"]
        SystemDefaults.check_kind = system_defaults["check_kind"]
        SystemDefaults.allowed_check_kind = system_defaults["allowed_check_kind"]
        SystemDefaults.check_type = system_defaults["check_type"]
        SystemDefaults.allowed_check_type = system_defaults["allowed_check_type"]
        SystemDefaults.priority = system_defaults["priority"]
        SystemDefaults.tasks_allowed_priority = system_defaults["tasks_allowed_priority"]
        SystemDefaults.checks_allowed_priority = system_defaults["checks_allowed_priority"]
        SystemDefaults.stages = system_defaults["stages"]
        SystemDefaults.allowed_stages = system_defaults["allowed_stages"]
        SystemDefaults.forbidden_stages = system_defaults["forbidden_stages"]
        SystemDefaults.observation_filter = system_defaults["observation_filter"]
        SystemDefaults.appointed_to = system_defaults["appointed_to"]
        SystemDefaults.appointed_to_check_vals = system_defaults["appointed_to_check_vals"]
        SystemDefaults.forbidden_header_vals = system_defaults["forbidden_header_vals"]
        SystemDefaults.forbidden_parks = system_defaults["forbidden_parks"]

    except KeyError as err:
        print(f"Some parameters are missing in {SystemDefaults.config} config. Please, verify config file and try again")
        return False

    return True


def _load_resources() -> bool:
    """
    Resources settings loader
    :return: True on success, False otherwise
    """
    resources_defaults = _try_loading(file=ResourcesDefaults.config, mode="r", encoding="cp1251")
    if resources_defaults is None:
        return False

    try:
        ResourcesDefaults.date = resources_defaults["date"]
        ResourcesDefaults.park = resources_defaults["park"]
        ResourcesDefaults.branch = resources_defaults["branch"]
        ResourcesDefaults.garage_num = resources_defaults["garage_num"]
        ResourcesDefaults.modification = resources_defaults["modification"]
        ResourcesDefaults.age = resources_defaults["age"]
        ResourcesDefaults.state_number = resources_defaults["state_number"]
        ResourcesDefaults.vin = resources_defaults["vin"]
        ResourcesDefaults.contract = resources_defaults["contract"]
        ResourcesDefaults.vehicle_class = resources_defaults["vehicle_class"]
        ResourcesDefaults.vehicle_class_list = resources_defaults["vehicle_class_list"]
        ResourcesDefaults.datetime_format = resources_defaults["datetime_format"]
    except KeyError as err:
        print(
            f"Some parameters are missing in {ResourcesDefaults.config} config. Please, verify config file and try again")
        return False

    return True


def _load_production() -> bool:
    """
    Production settings loader
    :return: True on success, False otherwise
    """
    production_defaults = _try_loading(file=ProductionDefaults.config, mode="r", encoding="cp1251")
    if production_defaults is None:
        return False

    try:
        ProductionDefaults.tram_branches = production_defaults["tram_branches"]
        ProductionDefaults.transport_type = production_defaults["transport_type"]
        ProductionDefaults.col_names = production_defaults["col_names"]
        ProductionDefaults.stop_name = production_defaults["stop_name"]
        ProductionDefaults.totals_name = production_defaults["totals_name"]
        ProductionDefaults.prod_name = production_defaults["prod_name"]
        ProductionDefaults.round_name = production_defaults["round_name"]
        ProductionDefaults.cols_to_search = production_defaults["cols_to_search"]
        ProductionDefaults.tram_name = production_defaults["tram_name"]
    except KeyError as err:
        print(
            f"Some parameters are missing in {ProductionDefaults.config} config. "
            f"Please, verify config file and try again")
        return False

    return True


def _load_classifier() -> bool:
    """
    Classifier settings loader
    :return: True on success, False otherwise
    """
    classifier_defaults = _try_loading(file=ClassifierDefaults.config, mode="r", encoding="cp1251")
    if classifier_defaults is None:
        return False

    try:
        ClassifierDefaults.task = classifier_defaults["task"]
        ClassifierDefaults.new_task = classifier_defaults["new_task"]
        ClassifierDefaults.system = classifier_defaults["system"]
        ClassifierDefaults.table = pd.read_json(classifier_defaults["table"], orient='records')
    except KeyError as err:
        print(f"Some parameters are missing in {ClassifierDefaults.config} config. "
              f"Please, verify config file and try again")
        return False

    return True


def _load_branches() -> bool:
    """
    Branches settings loader
    :return: True on success, False otherwise
    """

    branches_defaults = _try_loading(file=BranchesDefaults.config, mode="r", encoding="cp1251")
    if branches_defaults is None:
        return False

    try:
        BranchesDefaults.address = branches_defaults["address"]
        BranchesDefaults.code = branches_defaults["code"]
        BranchesDefaults.old_park_name = branches_defaults["old_park_name"]
        BranchesDefaults.park_name = branches_defaults["park_name"]
        BranchesDefaults.old_branch = branches_defaults["old_branch"]
        BranchesDefaults.branch = branches_defaults["branch"]
        BranchesDefaults.sap_branch = branches_defaults["sap_branch"]
        BranchesDefaults.for_pptx = branches_defaults["for_pptx"]
        BranchesDefaults.table = pd.read_json(branches_defaults["table"], orient='records')
    except KeyError as err:
        print(f"Some parameters are missing in {BranchesDefaults.config} config. "
              f"Please, verify config file and try again")

    return True


def load_settings() -> bool:
    """
    Wrapper for loading all settings for the project
    :return: True on success, False otherwise
    """
    if not all([_load_global(), _load_system(), _load_resources(), _load_production(), _load_classifier(), _load_branches()]):
        return False

    print("All settings successfully loaded")
    return True


if __name__ == "__main__":
    load_settings()
    pass
