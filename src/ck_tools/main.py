import importlib.util
from pathlib import Path
import os
import re
import shutil

from arcgis.gis import GIS, Group
from arcgis.env import active_gis
from dotenv import find_dotenv, load_dotenv

if importlib.util.find_spec('arcpy') is not None:
    import arcpy
    has_arcpy = True
else:
    has_arcpy = False

def _not_none_and_len(string: str) -> bool:
    """helper to figure out if not none and string is populated"""
    is_str = isinstance(string, str)
    has_len = False if re.match(r'\S{5,}', '') is None else True
    status = True if has_len and is_str else False
    return status


def add_group(gis: GIS = None, group_name: str = None) -> Group:
    """
    Add a group to the GIS for the project for saving resources.

    Args:
        gis: Optional
            arcgis.gis.GIS object instance.
        group_name: Optional
            Group to be added to the cloud GIS for storing project resources. Default
            is to load from the .env file. If a group name is not provided, and one is
            not located in the .env file, an exception will be raised.

    Returns: Group
    """
    # load the .env into the namespace
    load_dotenv(find_dotenv())

    # try to figure out what GIS to use
    if gis is None and isinstance(active_gis, GIS):
        gis = active_gis

    if gis is None and not isinstance(active_gis, GIS):
        url = os.getenv('ESRI_GIS_URL')
        usr = os.getenv('ESRI_GIS_USERNAME')
        pswd = os.getenv('ESRI_GIS_PASSWORD')

    # if no group name provided
    if group_name is None:

        # load the group name
        group_name = os.getenv('ESRI_GIS_GROUP')

        err_msg = 'A group name must either be defined in the .env file or explicitly provided.'
        assert isinstance(group_name, str), err_msg
        assert len(group_name), err_msg

    # create an instance of the content manager
    cmgr = gis.groups

    # make sure the group does not already exist
    assert len([grp for grp in cmgr.search() if
                grp.title.lower() == group_name.lower()]) is 0, f'A group named "{group_name}" already exists. ' \
                                                                'Please select another group name.'

    # create the group
    grp = cmgr.create(group_name)

    # ensure the group was successfully created
    assert isinstance(grp, Group), 'Failed to create the group in the Cloud GIS.'

    return grp


def create_local_data_resources(data_pth: Path = None) -> Path:
    """create all the data resources for the available environment"""
    # default to the expected project structure
    if data_pth is None:
        data_pth = Path(__file__).parent.parent.parent/'data'

    # cover if a string is inadvertently passed in as the path
    data_pth = Path(data_pth) if isinstance(data_pth, str) else data_pth

    # iterate the data subdirectories
    for data_name in ['interim', 'raw', 'processed', 'external']:

        # ensure the data subdirectory exists
        dir_pth = data_pth / data_name
        if not dir_pth.exists():
            dir_pth.mkdir(parents=True)

        # if working in an arcpy environment
        if has_arcpy:

            # remove the file geodatabase if it exists and recreate it to make sure compatible with version of Pro
            fgdb_pth = dir_pth / f'{data_name}.gdb'
            if fgdb_pth.exists():
                shutil.rmtree(fgdb_pth)
            arcpy.management.CreateFileGDB(str(dir_pth), f'{data_name}.gdb')

            # do the same thing for a mobile geodatabase, a sqlite database
            gdb_pth = dir_pth / f'{data_name}.geodatabase'
            if gdb_pth.exists():
                gdb_pth.unlink()
            arcpy.management.CreateMobileGDB(str(dir_pth), f'{data_name}.geodatabase')

    return data_pth