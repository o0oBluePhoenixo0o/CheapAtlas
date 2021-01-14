import pandas as pd
import numpy as np
from shapely import wkt
import sys
import os
import re
import logging
from shapely.geometry import box, Polygon
from geopandas import GeoDataFrame

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score
from xgboost import XGBClassifier

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


def xgboost_classify_building(buildings_clust_df):
    """
    Classify building footprints into residential and non-residential. 
    Merge results with existing naive classification (from data_preparation pipeline)
    
    Args:
        buildings_clust_df: building footprints dataset for an area with building_block results from HDBSCAN
    
    """
    
    # Turn into a binary classification problem: residentials vs the rest
    df = buildings_clust_df[buildings_clust_df.building_types != 'to_be_classified'][['building_types', # target variable
                                                                                      'rectangularity',
                                                                                      'surface_area',
                                                                                      'building_block']]
    
    df['building_types'] = np.where(df.building_types != 'residential', 'non-residential','residential')
    
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
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.25, random_state = 42)

    # Feature Scaling
    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train)
    X_test = scaler.transform(X_test)
    
    # Fit classifier
    classifier = XGBClassifier(random_state = 42)
    
    # Get result from CV steps and apply 
    classifier.fit(X_train, y_train)
    y_pred = classifier.predict(X_test)
    
    logging.info(f'Accuracy: %.3f (%.3f) {accuracy_score(y_test, y_pred)}')
    
    ########################################
    # Classify unknown footprints
    classify_df = buildings_clust_df[buildings_clust_df.building_types == 'to_be_classified']
    # Apply scaling
    classify_scaled = scaler.transform(classify_df[['rectangularity','surface_area','building_block']])
    yhat = classifier.predict(classify_scaled)
    
    classify_df = classify_df.assign(building_types_pred = yhat)[['id','building_types_pred']]
    # Only take those classified as "residential"
    classify_df = classify_df[classify_df.building_types_pred == 1]

    # Get list of OSM_ID 
    residential_list = list(classify_df.id)
    buildings_clust_df['building_types'] = np.where(buildings_clust_df.id.isin(residential_list), 'residential', buildings_clust_df.building_types)
    
    return buildings_clust_df


