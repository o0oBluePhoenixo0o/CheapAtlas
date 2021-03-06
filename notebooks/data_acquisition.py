import urllib.request
from urllib.parse import urlparse
from tqdm import tqdm  # progress bar

import pandas as pd
import requests
import os
import time
# for logging
import logging
log = logging.getLogger(__name__)


# 1st node
def get_data(raw_plz_ags, saved_location):
    """
    Function to acquire building objects in each postal code
    Check current crawling progress by scanning "saved_location" for missing postal codes
        Args:
             raw_plz_ags: population in each postal code data
             saved_location: saved location of crawled data
    """
    # Check for progress of crawled postal codes
    name_list = os.listdir(saved_location)
    done_plz = [x.split('.')[0].split('_')[1] for x in name_list if 'buildings' in x]

    # Get to-be-crawled list
    plz_de = raw_plz_ags[raw_plz_ags.plz.isin(done_plz) == False].reset_index(drop=True)

    # Define start-end point
    start = 0
    end = len(plz_de)

    while start <= end - 1:
        # Get all the building foot prints in target postal code
        target_plz = plz_de.plz[start]

        start = start + 1

        # Extract buildings
        results_df = get_buildings_plz(target_plz)

        plz_path = f'{saved_location}/buildings_{target_plz}.csv'

        # Save results
        if results_df.empty:
            logging.error(f'{start}/{end} Can not extract data for postal code {target_plz}')
        else:
            # Saving files
            save_building_result(results_df, target_plz, plz_path)
            logging.info(f'{start}/{end} Complete extraction for postal code {target_plz}')

    return None

def get_buildings(boundary_type: str, boundary_id: str):
    '''Acquire list of buildings from OpenStreetMap through the following ID type
    - Postal code (PLZ) 
    - Official community key (AGS) '''

    overpass_url = "http://overpass-api.de/api/interpreter"
    if boundary_type == 'plz':
        overpass_query = f"""
                        [out:json];
                        area["ISO3166-1"="DE"]->.b;
                        rel(area.b)[postal_code='{boundary_id}'];
                        map_to_area ->.a;
                        (
                          /* buildings */
                          nwr["building"](area.a);
                        );
                        out center;
                        """
    elif boundary_type == 'ags':
        overpass_query = f"""
                        [out:json];
                        area[type=boundary]["de:amtlicher_gemeindeschluessel"="{boundary_id}"]->.boundaryarea;
                        (nwr["building"](area.boundaryarea);
                        );
                        out center;  
                        """
    
     
    status = 0
    counter = 0

    # Try 3 times with wait time 1s if status is not 200
    while (status != 200) & (counter <= 3):
        # Send request to api
        response = requests.get(overpass_url,
                                params={'data': overpass_query})
        status = response.status_code
        counter = counter + 1

        if status == 200:
            counter = 3

        time.sleep(1)  # wait 1s

    try:
        # Only get contains of elements (building footprints)
        data = response.json()
        data = data['elements']

        result = pd.json_normalize(data)
    except:
        result = pd.DataFrame()  # return empty dataframe
    return result


def save_building_result(df, boundary_type, boundary_id, csv_path):
    """ Write results to csv file
        If newly crawled data has different column sets ==> manipulate the set to fit the standard and save
    """

    standard_df = pd.DataFrame(columns=('type', 'id', 'nodes', 'center.lat', 'center.lon',
                                        'tags.building', 'tags.building:levels', 'tags.source',
                                        'tags.addr:city', 'tags.addr:housenumber', 'tags.addr:postcode',
                                        'tags.addr:street', 'tags.addr:suburb'))

    # Convert to standard dataframe columns
    df = pd.concat([standard_df, df])[standard_df.columns]
    
    # Add PLZ
    if boundary_type == 'plz':
        df['tags.addr:postcode'] = boundary_id
    elif boundary_type == 'ags':
        df['ags'] = boundary_id

    if not os.path.exists(csv_path):
        df.to_csv(csv_path, header=True, index=False)
    else:
        df.to_csv(csv_path, mode='a', header=False, index=False)


# 2nd node

class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)


def download_url(state_list, geofabrik):
    # Iterate through states list
    for state in state_list:
        url_list = geofabrik[state]
        logging.info(f'Downloading Geofabrik data for {state}')
        save_folder = geofabrik['output_path'] + state + "/"

        # create saving location folder if not exists
        if not os.path.exists(save_folder):
            os.makedirs(save_folder)

        for url in url_list['download_url']:
            filename = urlparse(url).path.split('/')
            output_path = save_folder + filename[len(filename) - 1]

            with DownloadProgressBar(unit='B', unit_scale=True,
                                     miniters=1, desc=url.split('/')[-1]) as t:
                urllib.request.urlretrieve(url, filename=output_path, reporthook=t.update_to)
