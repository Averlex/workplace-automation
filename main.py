from workflow.resources_addition import *
from workflow.another_system_addition import SystemAddition
import excel_operations.io as io
from excel_operations.merger import concat_tables, merge_with_table
from datetime import timedelta
from settings.defaults import GlobalDefaults, SystemDefaults, ResourcesDefaults, ClassifierDefaults, BranchesDefaults, load_settings
from workflow.another_system_reports import Pivots


# TODO: logging, clean up the output
# TODO: unify reading (so the script might read all files at once and only after split them by partitions)

if __name__ == "__main__":
    # Loading settings
    if not load_settings():
        exit(0)

    print("All settings loaded successfully")
    tmp = ClassifierDefaults.table

    # Some code here
