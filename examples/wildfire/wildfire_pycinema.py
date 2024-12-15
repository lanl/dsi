import pycinema
import pycinema.filters
import pycinema.theater
import pycinema.theater.views

# pycinema settings
PYCINEMA = { 'VERSION' : '3.0.0'}

# filters
CinemaDatabaseReader_0 = pycinema.filters.CinemaDatabaseReader()
TableView_0 = pycinema.filters.TableView()
ImageReader_0 = pycinema.filters.ImageReader()
ImageView_0 = pycinema.filters.ImageView()

# properties
CinemaDatabaseReader_0.inputs.path.set("./examples/wildfire/wildfire.cdb", False)
CinemaDatabaseReader_0.inputs.file_column.set("FILE", False)
TableView_0.inputs.table.set(CinemaDatabaseReader_0.outputs.table, False)
TableView_0.inputs.selection.set([], False)
ImageReader_0.inputs.table.set(CinemaDatabaseReader_0.outputs.table, False)
ImageReader_0.inputs.file_column.set("FILE", False)
ImageReader_0.inputs.cache.set(True, False)
ImageView_0.inputs.images.set(ImageReader_0.outputs.images, False)
ImageView_0.inputs.selection.set([], False)

# layout
tabFrame0 = pycinema.theater.TabFrame()
splitFrame0 = pycinema.theater.SplitFrame()
splitFrame0.setHorizontalOrientation()
view0 = pycinema.theater.views.NodeEditorView()
splitFrame0.insertView( 0, view0 )
splitFrame1 = pycinema.theater.SplitFrame()
splitFrame1.setVerticalOrientation()
view2 = pycinema.theater.views.FilterView( TableView_0 )
splitFrame1.insertView( 0, view2 )
view4 = pycinema.theater.views.FilterView( ImageView_0 )
splitFrame1.insertView( 1, view4 )
splitFrame1.setSizes([487, 497])
splitFrame0.insertView( 1, splitFrame1 )
splitFrame0.setSizes([897, 896])
tabFrame0.insertTab(0, splitFrame0)
tabFrame0.setTabText(0, 'Layout 1')
tabFrame0.setCurrentIndex(0)
pycinema.theater.Theater.instance.setCentralWidget(tabFrame0)

# execute pipeline
CinemaDatabaseReader_0.update()
