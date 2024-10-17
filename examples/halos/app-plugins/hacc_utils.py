import os
from pathlib import Path



def parse_folder(folder_path, file_starts_with):

    # Get the files
    files_and_dirs = os.listdir(folder_path)

    # Select only those that start with 
    filtered_items = [item for item in files_and_dirs if item.startswith(file_starts_with)]

    # Put the data into a new fomat
    data = []
    header = filtered_items[0].split('_')
    data.append(header[::2])

    for f in filtered_items:
        values =  f.split('_')
        data.append(values[1::2])

    return len(data), data




def parse_params(file_path):
    if not os.path.exists(file_path):
        print(f"File '{file_path}' does not exist")
        return
    
    key_value_map = {}

    with open(file_path, 'r') as file:
        for line in file:
            # Skip lines that start with '#'
            if line.strip().startswith('#'):
                continue

            if line.strip() == "":
                continue
            
            
    
            try:
                key, value = line.split(' ', 1)
            except ValueError:
                key = line.split(' ', 1)[0]
                value = ""
                print(f"Warning: line: {line}")

            key = key.strip()
            value = value.strip()
            key_value_map[key] = value

    return key_value_map