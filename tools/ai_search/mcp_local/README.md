# DSI tools with MCP. 
This is a basic example of how to use the MCP server here

- Start the MCP as follows:
```bash
python tools/ai_search/mcp/dsi_mcp_server.py
# or
python -m tools.ai_search.mcp.dsi_mcp_server
```

- Then, run the test as follows:
```bash
python tools/ai_search/mcp/test_mcp.py
```



## To use with [URSA](https://github.com/lanl/ursa)

```bash
(venv_dsi_dev) pascalgrosset@pn2301251 dsi % ursa --config tools/ai_search/mcp/ursa_dsi_tools.yaml
<frozen importlib._bootstrap>:241: DeprecationWarning: builtin type SwigPyPacked has no __module__ attribute
<frozen importlib._bootstrap>:241: DeprecationWarning: builtin type SwigPyObject has no __module__ attribute
<frozen importlib._bootstrap>:241: DeprecationWarning: builtin type swigvarlink has no __module__ attribute

  __  ________________ _
 / / / / ___/ ___/ __ `/
/ /_/ / /  (__  ) /_/ /
\__,_/_/  /____/\__,_/

For help, type: ? or help. Exit with Ctrl+d.
ursa> ?

Documented commands (type help <topic>):
========================================
EOF  agents  arxiv  chat  clear  execute  exit  help  models  web

Undocumented commands:
======================
dsi  hypothesize  plan


ursa> dsi
dsi: list the contents of the database at: /Users/pascalgrosset/projects/dsi/tools/ai_search/data/oceans_11/ocean_11_datasets.db"
No DSI database provided. Please load one
Here are the contents of /Users/pascalgrosset/projects/dsi/tools/ai_search/data/oceans_11/ocean_11_datasets.db (table genesis_datacard, all rows):                                   

                                                                                                                                                                                     
  Title                    Keywords/Tag            Update/Modification_D…   Theme/Category_:_Dom…   Contact_Point_        Email               Originating_Organizat…   dsi_database  
 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 
  Deep Water Impact        asteroid impact,           2017-05-02 00:00:00   physics                 John Patchett         patchett@lanl.gov   DOE:LANL                 (null)        
  Ensemble Dataset         meteor, insitu,                                                                                                                                           
                           visualization,                                                                                                                                            
                           simulation                                                                                                                                                
  Bowtie Dataset           semiconductor,             2025-08-21 00:00:00   manufacturing           Nathan DeBardeleben   ndebard@lanl.gov    DOE:LANL                 (null)        
                           manufacturing                                                                                                                                             
  Higrad Firetex           wildfire, simulation,      2023-03-22 00:00:00   physics                 Rodman Linn           rrl@lanl.gov        DOE:LANL                 (null)        
  Wildfire Simulations     highgrad, firetec                                                                                                                                         
  Gray-Scott               gray-scott, pde,           2025-10-13 00:00:00   physics                 Nathan DeBardeleben   ndebard@lanl.gov    DOE:LANL                 (null)        
  reaction-diffusion       simulation, complex                                                                                                                                       
  dataset                  dynamics                                                                                                                                                  
  The High Explosives &    High Explosives,           2025-10-14 00:00:00   eulerian                Kyle Hickmann         hickmank@lanl.gov   DOE:LANL                 (null)        
  Affected Targets         HEAT, AI Ready, ML,                                                                                                                                       
  (HEAT) Dataset           Eulerean                                                                                                                                                  
  Heat Equations           head, diffusion,           2025-10-13 00:00:00   physics                 Nathan DeBardeleben   ndebard@lanl.gov    DOE:LANL                 (null)        
                           simulation, partial                                                                                                                                       
                           differential                                                                                                                                              
                           equations                                                                                                                                                 
  Monopoly Dataset         computed tomography,       2025-10-07 00:00:00   manufacturing           Nathan DeBardeleben   ndebard@lanl.gov    DOE:LANL                 (null)        
                           scans, monopoly                                                                                                                                           
                           hotels, steel,                                                                                                                                            
                           materials                                                                                                                                                 
  3D FLASH Computation     NIF, fusion, 3d,           2023-03-22 00:00:00   fusion                  Joshua Paul Sauppe    jpsauppe@lanl.gov   DOE:LANL                 nif.db        
  of National Ignition     simulation                                                                                                                                                
  Facility Shot                                                                                                                                                                      
                                                                                                                                                                                     

If you want, I can print the full Description_ text for each row too (it’s long, so I omitted it here).                                                                              

ursa> exit
Exiting ursa...
```