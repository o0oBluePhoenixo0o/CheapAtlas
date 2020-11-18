from bokeh.io import output_notebook
from bokeh.layouts import gridplot,row,column
from bokeh.plotting import figure,show
output_notebook()

def plot_buildings_plz(df, plz):
    'Plot buildings by types in a postal code'
    
    p = figure(title=f"Building types map at postal code {plz}",
           y_range=(df['center.lat'].min() - 0.08,
                    df['center.lat'].max() + 0.08),
           x_range=(df['center.lon'].min() - 0.08,
                    df['center.lon'].min() + 0.08))

    p.xaxis.axis_label = 'Longitude'
    p.yaxis.axis_label = 'Latitude'

    # industrial
    industrial_lat=df['center.lat'][df['building_types']=='industrial']
    industrial_lon=df['center.lon'][df['building_types']=='industrial']

    # residential
    residential_lat=df['center.lat'][df['building_types']=='residential']
    residential_lon=df['center.lon'][df['building_types']=='residential']

    # commercial
    commercial_lat=df['center.lat'][df['building_types']=='commercial']
    commercial_lon=df['center.lon'][df['building_types']=='commercial']

    # public
    public_lat=df['center.lat'][df['building_types']=='public']
    public_lon=df['center.lon'][df['building_types']=='public']

    # to_be_classified
    unknown_lat=df['center.lat'][df['building_types']=='to_be_classified']
    unknown_lon=df['center.lon'][df['building_types']=='to_be_classified']

    p.circle(industrial_lon, industrial_lat,
             size = 5, color = 'blue',
             fill_alpha=0.5,line_alpha=0.5,
             legend_label='Industrial')

    p.circle(commercial_lon, commercial_lat,
             size=5, color= 'green',
             fill_alpha=0.5,line_alpha=0.5,
             legend_label='Commercial')

    p.circle(residential_lon, residential_lat,
             size=5, color = 'yellow',
             fill_alpha=0.5,line_alpha=0.5,
             legend_label='Residential')

    p.circle(public_lon, public_lat,
             size=5, color= 'purple',
             fill_alpha=0.5,line_alpha=0.5,
             legend_label='Public')

    p.circle(unknown_lon, unknown_lat,
             size = 5, color = 'grey',
             fill_alpha=0.5,line_alpha=0.5,
             legend_label='Unknown')

    show(p, notebook_handle=True)