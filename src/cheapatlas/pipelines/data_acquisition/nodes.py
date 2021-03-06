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
This is a boilerplate pipeline 'data_acquisition'
generated using Kedro 0.16.6
"""
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
def get_data(plz_ags, boundary_type, saved_location):
    """
    Function to acquire building objects in each postal code
    Check current crawling progress by scanning "saved_location" for missing postal codes
        Args:

             plz_ags: collection of postal code and ags code in Germany
             boundary_type: separate crawled data based on PLZ or AGS code
             saved_location: saved location of crawled data
    """
    # Create saved location if not existed
    if not os.path.exists(saved_location):
        os.makedirs(saved_location)

    # Check for progress of crawled postal codes
    name_list = os.listdir(saved_location)
    done_id = [x.split('.')[0].split('_')[2] for x in name_list if 'buildings' in x]

    # Get to-be-crawled list
    id_list = plz_ags[plz_ags[boundary_type].isin(done_id) == False][[boundary_type]]\
        .drop_duplicates()\
        .reset_index(drop=True)

    logging.info(f'Start crawling for a total of {len(id_list)} {boundary_type}(s) out of {len(plz_ags[[boundary_type]].drop_duplicates())} {boundary_type}(s)')

    # Define start-end point
    start = 0
    end = len(id_list)

    while start <= end - 1:
        # Get all the building foot prints in target postal code
        boundary_id = id_list[boundary_type][start]

        start = start + 1

        # Extract buildings
        results_df = get_buildings(boundary_type, boundary_id)

        # Add boundary id
        results_df.insert(len(results_df), boundary_type, boundary_id)

        save_path = f'{saved_location}/buildings_{boundary_type}_{boundary_id}.csv'

        # Save results
        if results_df.empty:
            logging.error(f'{start}/{end} Can not extract data for {boundary_type} {boundary_id}')
        else:
            # Saving files
            save_building_result(results_df, save_path)
            logging.info(f'{start}/{end} Complete extraction for {boundary_type} {boundary_id}')

    return None


def get_buildings(boundary_type: str, boundary_id: str):
    """
    Acquire list of buildings from OpenStreetMap through the following ID type
        - Postal code (PLZ)
        - Official community key (AGS)
    """

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


def save_building_result(df, csv_path):
    """ Write results to csv file
        If newly crawled data has different column sets ==> manipulate the set to fit the standard and save
    """

    standard_df = pd.DataFrame(columns=('type', 'id', 'nodes', 'center.lat', 'center.lon',
                                        'tags.building', 'tags.building:levels', 'tags.source',
                                        'tags.addr:city', 'tags.addr:housenumber', 'tags.addr:postcode',
                                        'tags.addr:street', 'tags.addr:suburb'))

    # Convert to standard dataframe columns
    df = pd.concat([standard_df, df])[standard_df.columns]

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
