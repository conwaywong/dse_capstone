''' Present an interactive function explorer with slider widgets.

Scrub the sliders to change the properties of the ``sin`` curve, or
type into the title text box to update the title of the plot.

Use the ``bokeh serve`` command to run the example by executing:

    bokeh serve eigenvector_analysis.py

at your command prompt. Then navigate to the URL

    http://localhost:5006/eigenvector_analysis

in your browser.

'''
import numpy as np
import pandas as pd
from bokeh.io import curdoc
from bokeh.models import ColumnDataSource, HBox, VBox
from bokeh.models.widgets import Slider
from bokeh.plotting import Figure


def get_reconstruction(eigenvectors, m_projection, Mean):
    k = len(m_projection)
    U = eigenvectors[:k]
    Eig = np.matrix(U.T)
    rec = np.array(m_projection * Eig.transpose() + Mean)
    rec = np.ravel(rec)
    return rec


p = '/var/tmp/ca_2010/total_flow_'
fid = 'CA_2010'
m_filename = p + 'eigenvectors.{}.csv'.format(fid)
m_eigs = pd.read_csv(m_filename, header=None).values
m_eigs_t = m_eigs.T

m_filename = p + 'mean_vector.{}.csv'.format(fid)
m_mean_vector = pd.read_csv(m_filename, header=None).values[0]

trans_file = '/var/tmp/transform_results/total_flow_transformed.transform.csv'
m_df = pd.read_csv(trans_file, header=None)

v0_struct = (np.min(m_df[0]), np.max(m_df[0]))
v1_struct = (np.min(m_df[1]), np.max(m_df[1]))
v2_struct = (np.min(m_df[2]), np.max(m_df[2]))
v3_struct = (np.min(m_df[3]), np.max(m_df[3]))

# Set up data
m_projection = [v0_struct[1], 0., 0., 0.]
rec = get_reconstruction(m_eigs_t, m_projection, m_mean_vector)
x = [i for i in xrange(np.shape(rec)[0])]
y = [v for v in rec]
source = ColumnDataSource(data=dict(x=x, y=y))

# Set up plot
plot = Figure(plot_height=800, plot_width=1200, title="Eigenvector Analysis",
              tools="pan,reset,resize,wheel_zoom",
              x_range=[0, 289], y_range=[0, 1500.], x_axis_label='Time', y_axis_label='Veh/5m')

plot.line('x', 'y', source=source, line_width=3, line_alpha=0.6)

# Set up widgets
v0 = Slider(title="V0_Coefficient", value=m_projection[0], start=v0_struct[0], end=v0_struct[1])
v1 = Slider(title="V1_Coefficient", value=m_projection[1], start=v1_struct[0], end=v1_struct[1])
v2 = Slider(title="V2_Coefficient", value=m_projection[2], start=v2_struct[0], end=v2_struct[1])
v3 = Slider(title="V3_Coefficient", value=m_projection[3], start=v3_struct[0], end=v3_struct[1])


# Set up callbacks
def update_data(attrname, old, new):
    # Get the current slider values
    v0_value = v0.value
    v1_value = v1.value
    v2_value = v2.value
    v3_value = v3.value

    # Generate the new curve
    m_projection = [v0_value, v1_value, v2_value, v3_value]
    rec = get_reconstruction(m_eigs_t, m_projection, m_mean_vector)
    x = [i for i in xrange(np.shape(rec)[0])]
    y = [v for v in rec]

    source.data = dict(x=x, y=y)


for w in [v0, v1, v2, v3]:
    w.on_change('value', update_data)

# Set up layouts and add to document
inputs = HBox(children=[v0, v1, v2, v3])

curdoc().add_root(VBox(children=[inputs, plot], width=800))
