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
This is a boilerplate pipeline 'buildings_classification'
generated using Kedro 0.16.6
"""
import pandas as pd
import numpy as np
import os
import re
from shapely import wkt
from shapely.geometry import box, Polygon
from geopandas import GeoDataFrame

import hdbscan

import logging
log = logging.getLogger(__name__)

from src.cheapatlas.commons.helpers import _left

# Classify building types
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score
from xgboost import XGBClassifier

import warnings
# Disable depreciation warning
warnings.filterwarnings("ignore", category=DeprecationWarning)

# 1st node
def generate_features(plz_ags,
                      boundary_type,
                      int_buildings_path,
                      pri_buildings_path):
    """
    Scan all available PLZ/AGS in the region.
    Populate PLZ/AGS building objects with data from region OSM dump (Geofabrik)

    Args:
        plz_ags: list of AGS in the region
        boundary_type: PLZ or AGS code
        int_buildings_path: output save to 02_intermediate
        pri_buildings_path: output save to 03_primary
    """
    k = 0

    # create saving location folder if not exists
    if not os.path.exists(pri_buildings_path):
        os.makedirs(pri_buildings_path)

    # Check for progress of already done areas
    name_list = os.listdir(pri_buildings_path)
    id_list = [x.split('.')[0].split('_')[2] for x in name_list if 'buildings' in x]

    # Get list of AGS codes
    plz_ags = plz_ags[[boundary_type]]
    plz_ags = pd.DataFrame(np.setdiff1d(plz_ags, id_list), columns=[boundary_type])

    logging.info(f'Total of {len(plz_ags)} {boundary_type}(s) in the country')

    while k < len(plz_ags):
        boundary_id = plz_ags[boundary_type].iloc[k]
        buildings_path = f'{int_buildings_path}/buildings_{boundary_type}_{boundary_id}.csv'

        try:
            # Read in building objects data in the area
            df = pd.read_csv(buildings_path,
                             dtype={'tags.addr:suburb': 'object',
                                    'tags.building:levels': 'object',
                                    'tags.source': str,
                                    'postcode': str},  # supposed to be AGS
                             converters={"nodes": lambda x: x.strip("[]").split(", ")})  # read column as list
            # Filter out NaN
            df = df[df.geometry.isna() == False].reset_index(drop=True)

            # Convert geometry to GeoSeries
            df['geometry'] = df['geometry'].apply(wkt.loads)
            # Convert to GeoPandas type
            df_geo = GeoDataFrame(df, geometry='geometry')

            # Shape & Size
            df_geo[['surface_area','rectangularity']] = df_geo.apply(lambda row: pd.Series(shape_size(row['geometry'])),axis=1)

            # Total area
            df_geo['total_area'] = df_geo['building_levels'].astype(int) * df_geo['surface_area']

            # Save result to 02_intermediate/buildings_plz/buildings_<boundary_type>_<boundary_id>.csv
            logging.info(f'Total of {len(df)} buildings in {boundary_type} {boundary_id} at position {k+1}/{len(plz_ags)}. Saving result...')

            # Save result
            df_geo.to_csv(f'{pri_buildings_path}/buildings_{boundary_type}_{boundary_id}.csv', index=False)

        except Exception as e:
            logging.warning(f'Cannot enhance data on {boundary_type} {boundary_id} at position {k+1}/{len(plz_ags)}. Error: {e}')
        finally:
            k = k + 1


def shape_size(footprint_coord):
    """
    Calculate shape of a building footprint polygon using the
    ratio between surface area / minimum bounding box
    """

    # Get bounding box
    bbox_coords = footprint_coord.bounds

    bbox = list(box(bbox_coords[0],bbox_coords[1],
                    bbox_coords[2],bbox_coords[3]).exterior.coords)

    bbox_area = Polygon(bbox).area * (10**10)

    # Surface area
    size = (footprint_coord.area)*(10**10)

    # Shape of a building footprint = Rectangularity
    shape = size / bbox_area

    return (shape, size)


# 2nd node
def building_block_clustering(plz_ags:pd.DataFrame,
                              boundary_type:str,
                              pri_buildings_path:str,
                              fea_buildings_path:str):
    """
    This node aims to cluster building footprints into block using HDBSCAN and save district-level data into 04_feature
    1. Aggregate data from municipality-level (AGS key) to district-level (the first 5-digit of AGS key)
    2. Perform clustering on district-level dataset (total ~ 400 districts in Germany)

    Args:
        plz_ags: list of municipalities in Germany
        boundary_type: PLZ or AGS code
        pri_buildings_path: inputs from 03_primary
        fea_buildings_path: outputs save to 04_feature
    """
    # create saving location folder if not exists
    if not os.path.exists(fea_buildings_path):
        os.makedirs(fea_buildings_path)

    # Generate list of districts
    plz_ags['ags_district'] = plz_ags[boundary_type].apply(lambda x: _left(x, 5))
    # Group to get only district-level ==> ~ 400 districts
    plz_ags_dist = plz_ags.groupby('ags_district').size().to_frame('count').reset_index()

    # Check for progress of already done areas
    name_list = os.listdir(fea_buildings_path)
    id_list = [x.split('.')[0].split('_')[2] for x in name_list if 'buildings' in x]

    # Get list of AGS codes
    plz_ags_dist = plz_ags['ags_district']
    plz_ags_dist = pd.DataFrame(np.setdiff1d(plz_ags_dist, id_list), columns=['ags_district'])

    # Iterate through list of district and perform clustering on each of them
    for idx, dist_id in enumerate(plz_ags_dist.ags_district):
        try:
            logging.info(f'Assembling footprints data for district {dist_id} at position {idx+1}/{len(plz_ags_dist)+1}')
            dist_df = generate_dist_data(dist_id, pri_buildings_path)

            # Drop unnecessary column
            dist_df.drop(columns=['Unnamed: 0'], errors='ignore', inplace=True)
            #### TEMP #### RUN ONLY ONCE
            dist_df.rename(columns={'postcode': 'ags'}, errors='ignore', inplace=True)

            # Perform on district-level dataframe
            # parameters follow the paper suggestion baseline
            buildings_clust_df = hdbscan_bld(dist_df,
                                             min_cluster_size=8, #min number of buildings in 1 cluster
                                             cluster_selection_epsilon=0.0003,  # 3 meters
                                             min_samples=2)
            # Save result
            buildings_clust_df.to_csv(f'{fea_buildings_path}/buildings_{boundary_type}_{dist_id}.csv', index=False)
        except Exception as e:
            logging.warning(f'Cannot clustering data at district {dist_id} at position {idx+1}/{len(plz_ags_dist)+1}. Error: {e}')

def generate_dist_data(dist_id, buildings_pri_path):
    """Generate district-level building footprints dataframe"""

    regex = re.compile(f'(buildings_ags_{dist_id})')
    # Create district building dataframe
    li = []
    for root, dirs, files in os.walk(buildings_pri_path):
        for file in files:
            if regex.match(file):
                df = pd.read_csv(os.path.join(buildings_pri_path, file),
                                 index_col=None, header=0,
                                 low_memory = False)
                li.append(df)

    dist_df = pd.concat(li, axis=0, ignore_index=True)
    return dist_df

def hdbscan_bld(buildings_df: pd.DataFrame, min_cluster_size: int, cluster_selection_epsilon: int, min_samples: int):
    """
    Take in building objects dataframe with coordinates (lat + lon) and perform HDBSCAN to group buildings into blocks

    Args:
        buildings_df: dataframe of building objects (osmid, coordinates, geometry, building_types - naive classification)
        min_cluster_size: minimum number of footprints to be considered as a "block"
        cluster_selection_epsilon: ensure cluster distance smaller than this threshold will not be split further (in meters 10^-4)
        min_samples: the larger the value, the more conservative split ==> have more noises
    Results:
        buildings_clust_df: with additional column as cluster id
    """
    # Setting up coordinate matrix
    coord_mat = np.array(buildings_df[['center.lat', 'center.lon']])
    rads = np.radians(coord_mat)

    clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size,
                                metric='haversine',  # haversine distance on earth surface
                                cluster_selection_epsilon=cluster_selection_epsilon,
                                min_samples=min_samples
                                )
    # Apply to get cluster id
    cluster_labels = clusterer.fit_predict(coord_mat)
    cluster_res = pd.DataFrame(cluster_labels, columns={'building_block'})

    # Logging info
    total_blks = cluster_res.groupby('building_block').size().shape[0]
    logging.info(f'Generate total of {total_blks} building blocks in the area.')

    buildings_clust_df = buildings_df.join(cluster_res)
    buildings_clust_df['building_block'] = buildings_clust_df['building_block'].astype(str)

    return buildings_clust_df

# 3rd node
def building_types_classification(plz_ags:pd.DataFrame,
                                  boundary_type:str,
                                  fea_buildings_path:str,
                                  model_output_path:str):
    """
    Classify building footprints into residential and non-residential. 
    Merge results with existing naive classification (from data_preparation pipeline)
    
    Args:
        plz_ags: list of municipalities in Germany
        boundary_type: PLZ or AGS code
        fea_buildings_path: inputs from 04_feature
        model_output_path: outputs to 07_model_output
    """

    # create saving location folder if not exists
    if not os.path.exists(model_output_path):
        os.makedirs(model_output_path)

    # Generate list of districts
    plz_ags['ags_district'] = plz_ags[boundary_type].apply(lambda x: _left(x, 5))
    # Group to get only district-level ==> ~ 400 districts
    plz_ags_dist = plz_ags.groupby('ags_district').size().to_frame('count').reset_index()

    # Check for progress of already done areas
    name_list = os.listdir(model_output_path)
    id_list = [x.split('.')[0].split('_')[2] for x in name_list if 'buildings' in x]

    # Get list of AGS codes
    plz_ags_dist = plz_ags['ags_district']
    plz_ags_dist = pd.DataFrame(np.setdiff1d(plz_ags_dist, id_list), columns=['ags_district'])
    # Iterate through list of district and perform clustering on each of them
    for idx, dist_id in enumerate(plz_ags_dist.ags_district):
        try:
            logging.info(f'Classifying footprints for district {dist_id} at position {idx + 1}/{len(plz_ags_dist) + 1}')
            buildings_clust_df = pd.read_csv(os.path.join(fea_buildings_path,f'buildings_ags_{dist_id}.csv'),
                                             low_memory=False)
            classified_buildings_clust_df = xgboost_classify_building(buildings_clust_df)

            # Save result
            classified_buildings_clust_df.to_csv(f'{model_output_path}/buildings_{boundary_type}_{dist_id}.csv', index=False)
        except Exception as e:
            logging.warning(
                f'Cannot classifying footprints in district {dist_id} at position {idx + 1}/{len(plz_ags_dist) + 1}. Error: {e}')


def xgboost_classify_building(buildings_clust_df):
    """
    Classify building footprints into residential and non-residential.
    Merge results with existing naive classification (from data_preparation pipeline)

    Args:
        buildings_clust_df: building footprints dataset for an area with building_block results from HDBSCAN

    """

    # Turn into a binary classification problem: residential vs the rest
    df = buildings_clust_df[buildings_clust_df.building_types != 'to_be_classified'][
        ['building_types',  # target variable
         'rectangularity',
         'surface_area',
         'building_block'
         ]]

    df['building_types'] = np.where(df.building_types != 'residential', 'non-residential', 'residential')

    # Factorize features
    df.building_types = pd.factorize(df['building_types'])[0]
    df.building_block = pd.factorize(df['building_block'])[0]

    # Splitting the data into independent and dependent variables
    X = df[['rectangularity',
            'surface_area',
            'building_block',
            ]].values

    y = df[['building_types']].values

    # Using Skicit-learn to split data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)

    # Feature Scaling
    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train)
    X_test = scaler.transform(X_test)

    # Fit classifier
    classifier = XGBClassifier(eval_metric='logloss',
                               use_label_encoder=False,
                               random_state=42)

    # Get result from CV steps and apply
    classifier.fit(X_train, y_train)
    y_pred = classifier.predict(X_test)

    logging.info(f'Accuracy: {round(accuracy_score(y_test, y_pred)*100,2)}%')

    ########################################
    # Classify unknown footprints
    classify_df = buildings_clust_df[buildings_clust_df.building_types == 'to_be_classified']
    # Apply scaling
    classify_scaled = scaler.transform(classify_df[['rectangularity',
                                                    'surface_area',
                                                    'building_block'
                                                    ]])
    yhat = classifier.predict(classify_scaled)

    classify_df = classify_df.assign(building_types_pred=yhat)[['id', 'building_types_pred']]
    # Only take those classified as "residential"
    classify_df = classify_df[classify_df.building_types_pred == 1]

    # Get list of OSM_ID
    residential_list = list(classify_df.id)
    buildings_clust_df['building_types'] = np.where(buildings_clust_df.id.isin(residential_list), 'residential',
                                                    buildings_clust_df.building_types)

    return buildings_clust_df