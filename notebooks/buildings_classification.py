import pandas as pd
import numpy as np
from shapely import wkt
import sys
import os
import re
import logging
from shapely.geometry import box, Polygon
from geopandas import GeoDataFrame


import hdbscan

def generate_dist_data(dist_id, buildings_pri_path):
    """Generate district-level building footprints dataframe"""
    
    regex = re.compile(f'(buildings_ags_{dist_id})')
    # Create district building dataframe
    li = []
    for root, dirs, files in os.walk(buildings_pri_path):
        for file in files:
            if regex.match(file):
                df = pd.read_csv(os.path.join(buildings_pri_path,file), 
                                 index_col=None, header=0)
                li.append(df)

    dist_df = pd.concat(li, axis=0, ignore_index=True)
    return dist_df


def hdbscan_bld(buildings_df:pd.DataFrame, min_cluster_size:int, cluster_selection_epsilon:int, min_samples:int):
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
    coord_mat = np.array(buildings_df[['center.lat','center.lon']])
    rads = np.radians(coord_mat)

    clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size,
                                metric='haversine', # haversine distance on earth surface
                                cluster_selection_epsilon = cluster_selection_epsilon,
                                min_samples = min_samples
                               )
    # Apply to get cluster id
    cluster_labels = clusterer.fit_predict(coord_mat)
    cluster_res = pd.DataFrame(cluster_labels, columns = {'building_block'})
    
    # Logging info
    total_blks = cluster_res.groupby('building_block').size().shape[0]
    logging.info(f'Generate total of {total_blks} building blocks in the area.')
    
    buildings_clust_df = buildings_df.join(cluster_res)
    buildings_clust_df['building_block'] = buildings_clust_df['building_block'].astype(str)
    
    return buildings_clust_df

def plot_buildings_area(buildings_clust_df, plot_type, legend_ncol):
    """
    Plot buildings in the area according to building types / building blocks
    
    Args:
        buildings_clust_df: result of clustering building groups (cols: geometry, coordinates, id, building_block, building_types)
        plot_type: building_block or building_types
        legend_ncol: display how many columns for the legend
    """
    
    # Plot
    df = buildings_clust_df

    # Filter out NaN
    df = df[df.geometry.isna() == False].reset_index(drop = True)

    # Convert geometry to GeoSeries
    df['geometry'] = df['geometry'].apply(wkt.loads)
    # Convert to GeoPandas type
    df_geo = GeoDataFrame(df, geometry='geometry')

    # Reorder columns for plotting
    df_geo = df_geo[['center.lon','center.lat','geometry','building_types','id','building_block']]

    df_geo.rename(columns = {'center.lat':'lat',
                            'center.lon':'lon'}, inplace = True)
       
    # Plot
    ax =   df_geo.plot(column=plot_type, 
                       markersize=3, 
                       figsize=(20,20), 
                       legend=True, 
                       legend_kwds=dict(loc='upper left', ncol=legend_ncol, bbox_to_anchor=(1, 1))
                      )
    
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

    return [shape, size]

def generate_stats_table(buildings_clust_df):
    """
    Generate statistical analysis table of building types in the area
    
    Args:
        buildings_clust_df: building footprints dataframe after performed building blocks assignment (HDBSCAN)
        
    Return:
        stat_table: statistical analysis results which contains means and standard deviations values for every building type in the area
    """
    # Count
    count_table = buildings_clust_df.groupby('building_types')[['building_types']].size().to_frame('count').reset_index()
    
    # Mean 
    mean_table = buildings_clust_df.groupby('building_types')[['building_types','surface_area','rectangularity']].mean().reset_index()
    mean_table.columns = ['building_types','mean_surface_area','mean_rectangularity']

    # Standard deviation
    sd_table=buildings_clust_df.groupby('building_types')[['surface_area','rectangularity']].agg(np.std, ddof=0).reset_index()

    # Rename columns
    sd_table.columns = ['building_types','sd_surface_area','sd_rectangularity']
    stat_table = count_table.merge(mean_table).merge(sd_table)

    stat_table = stat_table[stat_table.columns[[0,1,3,2,4]]]
    
    return stat_table

def generate_stats_table(buildings_clust_df):
    """
    Generate statistical analysis table of building types in the area
    
    Args:
        buildings_clust_df: building footprints dataframe after performed building blocks assignment (HDBSCAN)
        
    Return:
        stat_table: statistical analysis results which contains means and standard deviations values for every building type in the area
    """
    # Count
    count_table = buildings_clust_df.groupby('building_types')[['building_types']].size().to_frame('count').reset_index()
    
    # Mean 
    mean_table = buildings_clust_df.groupby('building_types')[['building_types','surface_area','rectangularity']].mean().reset_index()
    mean_table.columns = ['building_types','mean_surface_area','mean_rectangularity']

    # Standard deviation
    sd_table=buildings_clust_df.groupby('building_types')[['surface_area','rectangularity']].agg(np.std, ddof=0).reset_index()

    # Rename columns
    sd_table.columns = ['building_types','sd_surface_area','sd_rectangularity']
    stat_table = count_table.merge(mean_table).merge(sd_table)
    # Reorder columns
    stat_table = stat_table[stat_table.columns[[0,1,3,2,4,5]]]
    
    return stat_table