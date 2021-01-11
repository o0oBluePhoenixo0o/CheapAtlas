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
from shapely import wkt
from shapely.geometry import box, Polygon
from geopandas import GeoDataFrame

import logging
log = logging.getLogger(__name__)

# 1st node
def generate_features(region_id_list,
                      boundary_type,
                      int_buildings_path,
                      pri_buildings_path):
    """
    Scan all available PLZ/AGS in the region.
    Populate PLZ/AGS building objects with data from region OSM dump (Geofabrik)

    Args:
        region_id_list: list of AGS in the region
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
    region_id_list = region_id_list[[boundary_type]]
    region_id_list = pd.DataFrame(np.setdiff1d(region_id_list, id_list), columns=[boundary_type])

    logging.info(f'Total of {len(region_id_list)} {boundary_type}(s) in the country')

    while k < len(region_id_list):
        boundary_id = region_id_list[boundary_type].iloc[k]
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
            df_geo[['surface_area','rectangularity']] = df_geo.apply(lambda row: pd.Series(shape_size(row['geometry'])),
                                                                     axis=1)

            # Total area
            df_geo['total_area'] = df_geo['building_levels'].astype(int) * df_geo['surface_area']

            # Save result to 02_intermediate/buildings_plz/buildings_<boundary_type>_<boundary_id>.csv
            logging.info(f'Total of {len(df)} buildings in {boundary_type} {boundary_id} at position {k+1}/{len(region_id_list)}. Saving result...')

            # Save result
            df_geo.to_csv(f'{pri_buildings_path}/buildings_{boundary_type}_{boundary_id}.csv', index=False)

        except Exception as e:
            logging.warning(f'Cannot enhance data on {boundary_type} {boundary_id} at position {k+1}/{len(region_id_list)}. Error: {e}')
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
def generate_stats_table(buildings_clust_df):
    """
    Generate statistical analysis table of building types in the area

    Args:
        buildings_clust_df: building footprints dataframe after performed building blocks assignment (HDBSCAN)

    Return:
        stat_table: statistical analysis results which contains means and standard deviations values for every building type in the area
    """

    # Mean
    mean_table = buildings_clust_df.groupby('building_types')[
        ['building_types', 'surface_area', 'rectangularity']].mean().reset_index()
    mean_table.columns = ['building_types', 'mean_surface_area', 'mean_rectangularity']

    # Standard deviation
    sd_table = buildings_clust_df.groupby('building_types')[['surface_area', 'rectangularity']].agg(np.std,
                                                                                                    ddof=0).reset_index()

    # Rename columns
    sd_table.columns = ['building_types', 'sd_surface_area', 'sd_rectangularity']
    stat_table = mean_table.merge(sd_table)

    stat_table = stat_table[stat_table.columns[[0, 1, 3, 2, 4]]]

    return stat_table