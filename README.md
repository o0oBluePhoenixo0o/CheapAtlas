# CheapAtlas
How to build your own simple demographics atlas using OSM (OpenStreetMap) & OGD (Open Government Data) with kedro pipeline


Repository of the project "CheapAtlas"

Link to Medium article: 

![In-a-nutshell](docs/In%20a%20nutshell%202.png)

# How to use

The current project is separated into 4 modular pipelines (4 stages)
![Project pipelines](docs/Project%20flowchart.jpg)

1. Clone the whole repository (main branch). Create ```data``` folder if not exists (you will have to run ```data_preparation_pipeline``` to get the data on your local machine)
2. Manually download the data from Germany Regional Data Bank (demographics) & list of AGS codes (municipality keys):

    - Population (separated by age and gender for each municipal): https://www.regionalstatistik.de/genesis//online?operation=table&code=12411-02-03-5&bypass=true&levelindex=0&levelid=1608920330516#abreadcrumb
    - Accommodations https://www.regionalstatistik.de/genesis//online?operation=table&code=31231-02-01-5&bypass=true&levelindex=1&levelid=1608920543970#abreadcrumb
    - Municipality codes https://www.suche-postleitzahl.org/downloads (dowload the zuordnung_plz_ort_landkreis.csv and only use the columns ags, osm - you can delete the rest)
2. Install necessary packages ```pip install src/requirements.txt```
3. Run the whole project by ```kedro run``` or run each pipeline (with tags or names)
