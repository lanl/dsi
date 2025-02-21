import dsi.plugins.file_writer as writer

if __name__ == "__main__":
    database_path = "/Users/mhan/Desktop/data/256MPC_RUNS_HACC_2PARAM/halo_galaxy_5.db"
    er_diagram = writer.ER_Diagram(database_path)
    er_diagram.export_erd(database_path, "diagram.png")