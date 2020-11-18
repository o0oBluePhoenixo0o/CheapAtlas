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
import pandas as pd
import requests
import json
import os
import time

def get_buildings_plz(plz:str):
    'Acquire list of buildings in postalcode(plz) from OpenStreetMap'
    
    overpass_url = "http://overpass-api.de/api/interpreter"
    overpass_query = f"""
                    [out:json];
                    area["ISO3166-1"="DE"]->.b;
                    rel(area.b)[postal_code='{plz}'];
                    map_to_area ->.a;
                    (
                      /* buildings */
                      nwr["building"](area.a);
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
            
        time.sleep(1) # wait 1s
        
    try:
        # Only get contains of elements (building footprints)
        data = response.json()
        data = data['elements']
        
        result = pd.json_normalize(data)
    except:
        result = pd.DataFrame() # return empty dataframe
    return(result)

def save_building_result(df, target_plz, csv_path):
    """ Write results to csv file
        If newly crawled data has different column sets ==> manipulate the set to fit the standard and save
    """
    
    standard_df = pd.DataFrame(columns = ('type','id','nodes','center.lat','center.lon', 
                                          'tags.building', 'tags.building:levels','tags.source',
                                          'tags.addr:city', 'tags.addr:housenumber', 'tags.addr:postcode', 'tags.addr:street', 'tags.addr:suburb')) 
    
    # Convert to standard dataframe columns
    df = pd.concat([standard_df,df])[standard_df.columns]
    # Add PLZ
    df['tags.addr:postcode'] = target_plz
    
    if not os.path.exists(csv_path):
        df.to_csv(csv_path, header = True, index = False)
    else:
        df.to_csv(csv_path, mode = 'a', header = False, index = False)
