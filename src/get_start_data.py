from pathlib import Path
import sys

from arcgis.features import FeatureLayer

src_dir = Path(__file__).parent
sys.path.append(str(src_dir))

from ck_tools import create_local_data_resources

# url for block groups in the US
bg_url = 'https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/USA_Block_Groups/FeatureServer/0/'

# data directories
dir_prj = src_dir.parent
dir_data = dir_prj/'data'
dir_raw = dir_data/'raw'

if __name__ == '__main__':

    # make sure all the target data directories exist
    dir_data = create_local_data_resources()

    # create a feature layer to access the data
    bg_lyr = FeatureLayer(bg_url)

    # get all the block groups
    bg_df = bg_lyr.query(out_fields=['FIPS']).sdf

    # convert the geometry to json
    bg_df['SHAPE'] = bg_df['SHAPE'].to_dict()

    # save as parquet
    bg_df.to_parquet(dir_raw/'block_groups.parquet')
