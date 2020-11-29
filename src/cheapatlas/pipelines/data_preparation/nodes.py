# Copyright 2020 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.
"""
This is a boilerplate pipeline 'data_preparation'
generated using Kedro 0.16.6
"""
import pandas as pd
import numpy as np
import sys
import os

from pyrosm import OSM
from src.cheapatlas.commons.helpers import _left

# for logging
import logging
log = logging.getLogger(__name__)

# 1st node
def get_region_data(plz_place, geofabrik, int_output_path, buildings_plz_path):
    """
    Enhance building objects data in all PLZ with data from OSM region dump (Geofabrik)
    1. Geometry
    2. Surface area
    3. Total area
    4. Classification (manual)

    Args:
        plz_place: plz or all towns in Germany
        geofabrik: Geofabrik region OSM data saved location (etc: data/01_raw/geofabrik/)
        int_output_path: output save to 02_intermediate
        buildings_plz_path: saved location of 01_raw/buildings_plz
    Returns:

    """
    # full path pbf
    region_list_path = [os.path.join(path, name) for path, subdirs, files in os.walk(geofabrik['output_path']) for name in files]
    # region pbf name
    pbf_list = [name for path, subdirs, files in os.walk(geofabrik['output_path']) for name in files]

    # Start loop for all region
    # i = 13,16 # not enough mem
    i = 17
    while i < len(pbf_list):
        # Get target region
        target_region_path = region_list_path[i]
        target_region = pbf_list[i]
        # Get AGS code belong to the target region
        target_ags_list = geofabrik['ags_code'].get(target_region)

        logging.info(f'{i}/{len(pbf_list)} Reading OSM info of {target_region}')
        # Length of AGS (2 or 3)
        ags_len = len(target_ags_list[0])

        # Initialize the OSM parser object
        osm = OSM(target_region_path)
        # Get buildings in the region
        buildings = osm.get_buildings()

        logging.info(f'Total of {len(buildings)} buildings in region {target_region}')

        # Extract info of all PLZ belong to that region
        region_plz_list = plz_place[(_left(plz_place.ags.str, ags_len).isin(target_ags_list))].plz.reset_index(drop=True)

        # Iterate through list of PLZ to enhance dataset
        enhance_plz(region_plz_list, buildings, buildings_plz_path, int_output_path)
        i = i + 1


def enhance_plz(region_plz_list, buildings, buildings_plz_path, int_output_path):
    """
    Scan all available PLZs in the region.
    Populate PLZ building objects with data from region OSM dump (Geofabrik)

    Args:
        region_plz_list: list of PLZs in the region
        buildings: buildings dataframe from region OSM
        buildings_plz_path: saved location at 01_raw/buildings_plz
        int_output_path: output save to 02_intermediate
    """
    k = 0

    # Check for progress of crawled postal codes
    name_list = os.listdir(int_output_path)
    done_plz = [x.split('.')[0].split('_')[1] for x in name_list if 'buildings' in x]

    # Get to-be-enhanced list. In list region_plz but not done_plz
    region_plz_list = np.setdiff1d(region_plz_list, done_plz)
    logging.info(f'Total of {len(region_plz_list)} PLZs in the region')

    while k < len(region_plz_list):
        plz = region_plz_list[k]
        plz_path = f'{buildings_plz_path}/buildings_{plz}.csv'

        # Read in building objects data in the postal code
        try:
            df = pd.read_csv(plz_path,
                             dtype={'tags.addr:suburb': 'object',
                                    'tags.building:levels': 'object',
                                    'tags.source': str,
                                    'tags.addr:postcode': str},
                             converters={"nodes": lambda x: x.strip("[]").split(", ")})  # read column as list

            # remove empty elements (no lat/lon)
            df = df[df['center.lat'].isna() == False].reset_index(drop=True)

            # replace NaN in building_levels
            df = df.rename(columns={'tags.building:levels': 'building_levels',
                                    'tags.addr:postcode': 'postcode'})
            # Fill all missing building level = 1 floor
            df.building_levels = df.building_levels.fillna(1)

            df_res = df.merge(buildings[['id', 'geometry', 'timestamp']],
                              how='left',
                              on='id')
            df_res.geometry = df_res.geometry.fillna(np.nan)

            # Calculate surface + total area
            df_res['surface_area'] = df_res.geometry.apply(lambda x: calculate_surface_area(x) * 10 ** 10)
            df_res['total_area'] = df_res['building_levels'].astype(int) * df_res['surface_area']

            # Classify building types (manual)
            df_res['building_types'] = df_res['tags.building'].apply(lambda x: manual_classify_building(x))

            # Save result to 02_intermediate/buildings_plz/buildings_<plz>.csv
            # create saving location folder if not exists
            if not os.path.exists(int_output_path):
                os.makedirs(int_output_path)

            logging.info(f'Total of {len(df)} buildings in PLZ {plz} at position {k}/{len(region_plz_list)}. Saving result...')
            # Save result
            df_res.to_csv(f'{int_output_path}/buildings_{plz}.csv', index=False)
        except Exception:
            logging.warning(f'Cannot enhance data on PLZ {plz} at position {k}/{len(region_plz_list)}')
        finally:
            k = k + 1

def calculate_surface_area(polygon):
    'Calculate surface area of the building object'
    if polygon == None:
        area = 0
    else:
        area = polygon.area
    return area

def manual_classify_building(building_type):
    """
    Adopt manual classification from the paper and add further tags
    'Estimation of Building Types on OpenStreetMap Based on Urban Morphology Analysis'
    
    """
    residential = ['apartments','aparments (s)',
                   'domitory','house','residential',
                   'retirement_home', 'terrace',
                   # self-add
                  'allotment_house', 'bungalow','summer_house','semidetached_house',
                  'terraced_house','dwelling_house','dormitory','family_house']
    
    commercial = ['bank','bar','boat_rental','cafe',
                 'club','dentist','doctors','fast_food','fuel',
                 'guest_house','hostel','hotel','pharmacy',
                 'pub','restaurant','restaurant;bierg','shop','supermarket',
                  # self-added
                 'commercial','retail','fuel_station','service','kiosk'
                 ]
    
    accessory_storage = ['carport','garage','garages','hut','roof','shelter',
                         # self-add
                        'barn','basement','storage_tank','shed','cabin','bunker','chimney','detached',
                         'parking_garage','container','hangar','silo'
                        ]
    
    accessory_supply = ['car_wash','surveillance','tower','warehouse',
                        # self-add
                       'aviary','farm_auxiliary','farm','power','electricity',
                        'transformer_house','transformer_tower','cowshed'
                       ]
    
    industrial = ['industrial',
                  # self-add
                  'construction','manufacture']
    
    public = ['MDF','attraction','arts_center','canteen','castle','hospital','church',
             'college','community_centre','museum','fire_station','greenhouse',
             'information','kindergarten','library','office','parking',
             'place_of_worship','police','public','public_building','school',
             'science_park','station','townhall','train_station','university',
             'youth_centre','theatre','toilets',
              # self-add
              'cathedral','historic','ambulance_station','bridge','government','transportation',
              'synagogue','sports_centre','ship','mosque','tech_cab','railway','gymnasium','religious',
              'chapel','civic','sports_hall','pavilion','bahnhof'
             ]
    
    not_classified = ['yes','YES']
    
    if (building_type in residential):
        result = 'residential'
    elif (building_type in commercial):
        result = 'commercial'
    elif (building_type in accessory_storage):
        result = 'accessory_storage'
    elif (building_type in accessory_supply):
        result = 'accessory_supply'
    elif (building_type in industrial):
        result = 'industrial'
    elif (building_type in public):
        result = 'public'
    elif (building_type in not_classified):
        result = 'to_be_classified'
    else:
        result = 'other'
    
    return result
