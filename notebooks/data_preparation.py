import osmapi
import pandas as pd
import numpy as np
# calculate surface area
from area import area 

def _calculate_surface_area(polygon):
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
                 'commercial','retail','fuel_station','service','kiosk','nightclub'
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
    
def get_total_area(node_list, building_level):
    ''' Building objects are recorded as "ways" & "relations".
        Collect all "nodes" coordinates for each building
        Calculate building total area = surface area * building levels
    '''
    
    api = osmapi.OsmApi()
    # collection of x & y coords
    x_col = []
    y_col = []
    try:
        for item in node_list:
            node_res = api.NodeGet(item)
            node_lat = node_res['lat']
            node_lon = node_res['lon']

            # add to collection of coords
            x_col.append(node_lat)
            y_col.append(node_lon)

        coords_col = list(zip(x_col, y_col))
        # Convert to object
        obj = {'type':'Polygon','coordinates':[coords_col]}  

        # Total area
        total_area = area(obj) * building_level
    except:
        total_area = 0
    
    return total_area

def get_region_data(buildings_plz_location, geofabrik_location, region_ags_dict, output_path):
    """
    Enhance building objects data in all PLZ with data from OSM region dump (Geofabrik)
    1. Geometry
    2. Surface area
    3. Total area
    4. Classification (manual)

    Args:
        buildings_plz_location: building objs / plz saved location (etc: data/01_raw/buildings_plz/)
        plz_place: plz or all towns in Germany
        geofabrik_location: Geofabrik region OSM data saved location (etc: data/01_raw/geofabrik/)
        region_ags_dict: dictionary of region AGS code
        output_path: output save to 02_intermediate
    Returns:
    
    """
    # Extract plz list
    plz_list = os.listdir(buildings_plz_location)
    plz_list = [x.split('.')[0].split('_')[1] for x in plz_list if 'buildings' in x]
    
    # full path pbf
    region_list_path = [os.path.join(path, name) for path, subdirs, files in os.walk(geofabrik_location) for name in files]
    # region pbf name
    pbf_list = [name for path, subdirs, files in os.walk(geofabrik_location) for name in files]
    
    # Start loop for all region
    i = 0
    while i < len(pbf_list):
        # Get target region
        target_region_path = region_list_path[i]
        target_region = pbf_list[i]
        # Get AGS code belong to the target region
        target_ags_list = region_ags_dict.get(target_region)
        # Length of AGS (2 or 3)
        ags_len=len(target_ags_list[0])
        
        logging.info(f'{i}/{len(pbf_list)} Reading info of region {target_region}')
        
        # Initialize the OSM parser object
        osm = OSM(target_region_path)
        # Get buildings in the region
        buildings = osm.get_buildings()
        
        logging.info(f'Total of {len(buildings)} in region {target_region}')
        
        # Extract info of all PLZ belong to that region
        region_plz_list = plz_place[(dp.left(plz_place.ags.str, ags_len).isin(target_ags_list))].plz.reset_index(drop=True)
        
        # Iterate through list of PLZ to enhance dataset
        enhance_plz(region_plz_list, buildings, output_path)
        i = i + 1                     
        
def enhance_plz(region_plz_list, buildings, output_path):
    """
    Scan all available PLZs in the region.
    Populate PLZ building objects with data from region OSM dump (Geofabrik)
    
    Args:
        region_plz_list: list of PLZs in the region
        buildings: buildings dataframe from region OSM
        output_path: output save to 02_intermediate
    """
    k = 0
    while k < len(region_plz_list):
        plz = region_plz_list[k]
        plz_path = f'../data/01_raw/buildings_plz/buildings_{plz}.csv'

        # Read in building objects data in the postal code
        df = pd.read_csv(plz_path,
                         dtype={'tags.addr:suburb': 'object',
                                'tags.building:levels': 'object',
                                'tags.source': str,
                                'tags.addr:postcode': str},
                         converters={"nodes": lambda x: x.strip("[]").split(", ")}) # read column as list

        # remove empty elements (no lat/lon)
        df = df[df['center.lat'].isna() == False].reset_index(drop=True)

        # replace NaN in building_levels
        df = df.rename(columns = {'tags.building:levels': 'building_levels',
                                  'tags.addr:postcode' : 'postcode'})
        # Fill all missing building level = 1 floor
        df.building_levels = df.building_levels.fillna(1)

        df_res = df.merge(buildings[['id','geometry','timestamp']],
                          how = 'left',
                          on = 'id')
        df_res.geometry = df_res.geometry.fillna(np.nan)
        
        # Calculate surface + total area
        df_res['surface_area'] = df_res.geometry.apply(lambda x: _calculate_surface_area(x) * 10**10)
        df_res['total_area'] = df['building_levels'].astype(int) * df_res['surface_area']
        
        # Classify building types (manual)
        df['building_types'] = df['tags.building'].apply(lambda x: dp.manual_classify_building(x))
        
        # Save result to 02_intermediate/buildings_plz/buildings_<plz>.csv
        # create saving location folder if not exists
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        
        logging.info(f'Total of {len(df)} buildings in PLZ {plz}. Saving result...')
        # Save result
        df_res.to_csv(output_path + f'buildings_{plz}.csv', index = False)