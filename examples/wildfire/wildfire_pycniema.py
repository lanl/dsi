import pycinema
import pycinema.filters
import pycinema.theater
import pycinema.theater.views

# pycinema settings
PYCINEMA = { 'VERSION' : '2.0.0'}

# layout
vf0 = pycinema.theater.Theater.instance.centralWidget()
vf0.setHorizontalOrientation()
vf0.insertView( 0, pycinema.theater.views.NodeEditorView() )
vf1 = vf0.insertFrame(1)
vf1.setVerticalOrientation()
TableView_0 = vf1.insertView( 0, pycinema.theater.views.TableView() )
ImageView_0 = vf1.insertView( 1, pycinema.theater.views.ImageView() )
vf1.setSizes([441, 440])
vf0.setSizes([958, 957])

# filters
CinemaDatabaseReader_0 = pycinema.filters.CinemaDatabaseReader()
ImageReader_0 = pycinema.filters.ImageReader()

# properties
CinemaDatabaseReader_0.inputs.path.set("../dsi/examples/wildfire/wildfire.cdb", False)
CinemaDatabaseReader_0.inputs.file_column.set("FILE", False)
TableView_0.inputs.table.set(CinemaDatabaseReader_0.outputs.table, False)
ImageView_0.inputs.images.set(ImageReader_0.outputs.images, False)
ImageReader_0.inputs.table.set(CinemaDatabaseReader_0.outputs.table, False)
ImageReader_0.inputs.file_column.set("FILE", False)
ImageReader_0.inputs.cache.set(True, False)

# execute pipeline
CinemaDatabaseReader_0.update()
