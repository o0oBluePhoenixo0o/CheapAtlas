import osmapi
import pandas as pd
import numpy as np
# calculate surface area
from area import area 

def left(s, amount):
    return s[:amount]

def right(s, amount):
    return s[-amount:]

def mid(s, offset, amount):
    return s[offset:offset+amount]

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