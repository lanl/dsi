from collections import OrderedDict
from pandas import DataFrame, read_csv, concat

from dsi.plugins.file_reader import FileReader

class TextFile(FileReader):
    """
    External Plugin to read in an individual or a set of text files.
    Assuming all text files have data for same table
    """
    def __init__(self, filenames, **kwargs):
        """
        `filenames`: one text file or a list of text files to be ingested
        """
        super().__init__(filenames, **kwargs)
        if isinstance(filenames, str):
            self.text_files = [filenames]
        else:
            self.text_files = filenames
        self.text_file_data = OrderedDict()

    def add_rows(self) -> None:
        """
        Parses text file data and creates an ordered dict whose keys are table names and values are an ordered dict for each table.
        """
        total_df = DataFrame()
        for filename in self.text_files:
            temp_df = read_csv(filename)
            total_df = concat([total_df, temp_df], axis=0, ignore_index=True)

        self.text_file_data["people"] = OrderedDict(total_df.to_dict(orient='list'))

        self.set_schema_2(self.text_file_data)