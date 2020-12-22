from gzip import GzipFile
from io import BytesIO
import importlib
import os
from pathlib import Path
import shutil
import sys
from typing import Tuple, List

from arcgis.features import GeoAccessor, GeoSeriesAccessor

import boto3
from dotenv import load_dotenv, find_dotenv
import pandas as pd

# load environment variables from .env
load_dotenv(find_dotenv())

# what we're going to let the rest of the world see
__all__ = ['SafegraphClient']


def get_year_month(pth: Path) -> Tuple[int, int]:
    """Get the year and month from the file path convention- useful for filtering."""

    pth = Path(pth) if isinstance(pth, str) else pth

    # get the year and month from the path depending on whether or not it is from the backports
    if 'backfill' in str(pth):
        yr_mth = pth.parts[6:8]
    else:
        yr_mth = pth.parts[2:4]

    # convert the year and month to integers for sorting
    yr_mth = tuple(int(val) for val in yr_mth)

    return yr_mth


def get_standardized_path(pth: Path) -> Path:
    """Get a standardized path for saving."""

    # get the year and the month from the file path
    year, month = get_year_month(pth)

    # get all the path parts
    pth_prts = Path(pth).parts

    # now, standardize the path
    pth_root = Path(pth_prts[0])
    typ = pth_prts[1].replace('_backfill', '')
    fl_nm = pth_prts[-1]

    # put everything back together in a standard schema
    out_pth = pth_root / typ / f'{year:04d}' / f'{month:02d}' / fl_nm

    return out_pth


def get_resource_type(pth: Path) -> str:
    """Get the resource type from the path - useful for finding the right resources."""
    typ = Path(pth).parts[1].replace('_backfill', '')
    return typ


def get_content_dataframe(s3, bucket='sg-c19-response', prefix='monthly-patterns'):
    """Provide ability to introspectively retrieve a dataframe of monthly patterns data."""

    # get the contents of the bucket with the specified prefix
    bkt_ls = s3.list_objects(Bucket=bucket, Prefix=prefix)
    cntnts = bkt_ls['Contents']

    # get a dataframe of all the available resources, starting with the S3 bucket path
    cntnt_df = pd.Series([itm['Key'] for itm in cntnts if not itm['Key'].endswith('_SUCCESS')],
                         name='source_path').to_frame()

    # calculate the year and month for the resource
    cntnt_df[['year', 'month']] = cntnt_df.source_path.apply(lambda pth: pd.Series(get_year_month(pth)))

    # get the resource category
    cntnt_df['resource_type'] = cntnt_df.source_path.apply(lambda pth: get_resource_type(pth))

    # get a standardized path - useful for saving outputs
    cntnt_df['standardized_path'] = cntnt_df.source_path.apply(lambda pth: get_standardized_path(pth))

    return cntnt_df


def check_list(in_lst, dtype=str):
    """Helper function to ensure input is a list of correct data type."""

    assert isinstance(in_lst, (list, dtype))

    if isinstance(in_lst, list):
        for itm in in_lst:
            assert isinstance(itm, dtype)
    else:
        in_lst = [in_lst]

    return in_lst


class SafegraphClient:
    """Client streamlining process of retrieving data from AWS S3 and preparing a proejct for analysis."""

    def __init__(self, bucket='sg-c19-response', prefix='monthly-patterns', access_key=None, secret_key=None):

        self.bucket = bucket
        self.prefix = prefix

        if access_key or secret_key:
            assert (
                        access_key and secret_key), 'If explicitly providing an access_key and secret_key for accessing AWS S3, you must provide both.'

        # retrieve credentials from environment variables if not explicitly provided
        access_key = os.getenv('AWS_KEY') if not access_key else access_key
        secret_key = os.getenv('AWS_SECRET') if not secret_key else secret_key

        assert access_key, 'If "AWS_KEY" is not set in the environment variables, it must be explicitly provided in the "access_key" parameter.'
        assert secret_key, 'If "AWS_SECRET" is not set in the environment variables, it must be explicitly provided in the "secret_key" parameter.'

        # start a session connecting to AWS with credentials
        aws_session = boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                                    region_name='us-east-1')

        # using the authenticated session, create an S3 client accessing the safegraph data
        self.s3 = aws_session.client('s3', endpoint_url='https://s3.wasabisys.com')

        self._content_dataframe = None

    @property
    def content_dataframe(self):

        if self._content_dataframe is None:
            self._content_dataframe = get_content_dataframe(self.s3, self.bucket, self.prefix)

        return self._content_dataframe

    def get_dataframe_from_remote_path(self, source_s3_path: str):
        """Get a dataframe for a resource using the remove path, the, "Key," referncing the path within the bucket in AWS S3."""

        src_pth = str(source_s3_path) if isinstance(source_s3_path, Path) else source_s3_path

        assert self.content_dataframe.source_path.str.contains(src_pth).any()

        # get the headers describing the file
        resp = self.s3.get_object(Bucket=self.bucket, Key=src_pth)

        # handle compressed data differently - only applies to patterns data
        if resp['ContentType'] == 'application/gzip':

            # create a gzip object mapped to the stream
            gz = GzipFile(fileobj=resp.get('Body'))

            # stream into a pandas dataframe
            df = pd.read_csv(gz, dtype=str)

        # the rest are just in flat csv's, so use BytesIO for them
        else:
            byt = BytesIO(resp.get('Body').read())
            df = pd.read_csv(byt)

        return df

    def get_patterns_dataframe(self, year: [int, List[int]], month: [int, List[int]] = None,
                               safegraph_pois: [str, List[str]] = None,
                               placekeys: [str, List[str]] = None) -> pd.DataFrame:
        """
        Get a patterns dataframe for a specific month and year with the option (recommended) to filter to a specific
        point of interest using the Safegraph POI ID. A combination of both year and month filters can be used to
        select just a single season from multiple years.

        Args:
            year: Required
                Year(s) to retrieve.
            month: Optional
                Month(s) to retrieve.
            safegraph_pois: Optional
                Safegraph point-of-interest id's used to filter the data.
            placekeys: Optional
                Placekeys used to filter the data.

        Returns:
            Dataframe of Safegraph patterns data.

        .. code-block:: python

            from sg_data import SafegraphClient

            sg_poi = 'sg:af471021a929414cbf69854e6f8f1b0c'  # white pass ski area

            sg = SafegraphClient()

            # just getting the months the ski area is open for two consecutive years
            wp_df = sg.get_patterns_dataframe([2018, 2019], [11, 12, 1, 2, 3, 4], safegraph_pois=sg_poi)

        """
        # run a data type check on the year input and make sure it is a list
        year = check_list(year, int)

        if month is not None:
            month = check_list(month, int)

            # filter the content for just this month's patterns data
            ym_cntnt_df = self.content_dataframe[(self.content_dataframe.year.isin(year)) &
                                                 (self.content_dataframe.month.isin(month)) &
                                                 (self.content_dataframe.resource_type == 'patterns')]

        else:
            # filter the content for just this month's patterns data
            ym_cntnt_df = self.content_dataframe[(self.content_dataframe.year.isin(year)) &
                                                 (self.content_dataframe.resource_type == 'patterns')]

        # empty list to populate
        ym_df_lst = []

        # for every one of the remote files
        for pth in ym_cntnt_df.source_path:

            # get a dataframe for the remote file
            tmp_df = self.get_dataframe_from_remote_path(pth)

            # filter the dataframe based on Safegraph POI ID's
            if safegraph_pois is not None:
                safegraph_pois = check_list(safegraph_pois, str)
                tmp_df = tmp_df[(tmp_df.safegraph_place_id.isin(safegraph_pois))]

            # filter the dataframe based on Placekeys
            if placekeys is not None:
                placekeys = check_list(placekeys, str)
                tmp_df = tmp_df[(tmp_df.placekey.isin(placekeys))]

            # add the dataframe to the list
            ym_df_lst.append(tmp_df)

        # combine all the output dataframes
        ym_df = pd.concat(ym_df_lst)

        return ym_df
