''' Present an interactive function explorer with slider widgets.

Scrub the sliders to change the properties of the ``sin`` curve, or
type into the title text box to update the title of the plot.

Use the ``bokeh serve`` command to run the example by executing:

    bokeh serve vis.py

at your command prompt. Then navigate to the URL

    http://localhost:5006/vis

in your browser.

'''
import numpy as np
import pandas as pd

from bokeh.io import curdoc
from bokeh.models import ColumnDataSource, HBox, VBox
from bokeh.models.widgets import Select, Button
from bokeh.plotting import Figure


class VisBean:
    def __init__(self, mean, eigs, projection):
        self._mean = mean
        self._eigs = eigs
        self._projection = projection
        self._k = np.shape(self._eigs)[0]

    def get_reconstruction(self):
        Eig = np.matrix(self._eigs)
        rec = np.array(self._projection * Eig + self._mean)
        return np.ravel(rec)


# parse data
years = [2008, 2009, 2010, 2011, 2013, 2014, 2015]
partitions = ['weekday', 'weekend']
projection_columns = [2, 3, 4, 5, 6]
#
base = '/home/dyerke/Documents/DSE/capstone_project/traffic/data/weekday_weekend_partitions/{}'
station_partition_key_format = '{}_{}'  # year_partition
visbean_key_format = '{}_{}_{}'  # year_partition_stationid
mean_filename_format = 'total_flow_{}_mean_vector.pivot_{}_grouping_pca_tmp.csv'
eigs_filename_format = 'total_flow_{}_eigenvectors.pivot_{}_grouping_pca_tmp.csv'
station_filename_format = 'total_flow_{}_transformed.pivot_{}_grouping.csv'
#
visbeans = {}
station_partitions= {}
for year in years:
    for partition in partitions:
        mean_filename = '/'.join([base.format(partition), mean_filename_format.format(partition, year)])
        eigs_filename = '/'.join([base.format(partition), eigs_filename_format.format(partition, year)])
        station_filename = '/'.join([base.format(partition), station_filename_format.format(partition, year)])
        #
        mean = pd.read_csv(mean_filename, header=None).values[0]
        eigs = pd.read_csv(eigs_filename, header=None).values.T  # eigenvectors per row matrix (5 X 288)
        stations_df = pd.read_csv(station_filename, header=None)
        #
        stations_list= []
        for _, row in stations_df.iterrows():
            station_id = int(row[0])
            projection = row[projection_columns].values
            #
            stations_list.append(str(station_id))
            #
            key = visbean_key_format.format(year, partition, station_id)
            visbean = VisBean(mean, eigs, projection)
            visbeans[key] = visbean
        station_partitions[station_partition_key_format.format(year, partition)]= sorted(stations_list, key=lambda k: int(k))

# Set up data
global source
source = ColumnDataSource(data=dict(x=[], y=[]))

# Set up plot
plot = Figure(plot_height=800, plot_width=1200, title="Reconstruction",
              tools='',
              x_range=[0, 289], y_range=[0, 1500.], x_axis_label='Time', y_axis_label='Veh/5m')

plot.line('x', 'y', source=source, line_width=3, line_alpha=0.6)

# Set up widgets
select_years= Select(title='Year', value=str(years[0]), options=[str(y) for y in years])
select_partition= Select(title='Partition', value=str(partitions[0]), options=[str(p) for p in partitions])

year_value= select_years.value
partition_value= select_partition.value
key= station_partition_key_format.format(year_value, partition_value)
stations_list= station_partitions[key]
select_stations= Select(title='Station', value=stations_list[0], options=stations_list)

button= Button(label='Submit')

# Set up callbacks
def update_station_list(attrname, old, new):
    year_value= select_years.value
    partition_value= select_partition.value
    #
    key= station_partition_key_format.format(year_value, partition_value)
    stations_list= station_partitions[key]
    #
    new_select_station= Select(title='Station', value=stations_list[0], options=stations_list)
    new_select_station.options=stations_list
    #
    new_children= [select_years, select_partition, new_select_station]
    inputs.children= new_children

def on_click(*args):
    # TODO: process button
    print('***click***')
    #
    year= int(select_years.value)
    partition= select_partition.value
    station= int(inputs.children[2].value)
    #
    key= visbean_key_format.format(year, partition, station)
    visbean= visbeans[key]
    rec= visbean.get_reconstruction()
    x= [i for i in xrange(len(rec))]
    y= rec
    #
    source.data= dict(x=x, y=y)

select_years.on_change('value', update_station_list)
select_partition.on_change('value', update_station_list)
button.on_click(on_click)

# Set up layouts and add to document
global inputs
inputs = HBox(children=[select_years, select_partition, select_stations], height=75)

m_doc= curdoc()
m_doc.add_root(VBox(children=[inputs, button, plot], width=1000))
