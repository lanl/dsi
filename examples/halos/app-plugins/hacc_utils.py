import os


def parse_params(file_path):
    """
    Compute a key-value storage from HACC .params and .dat

    Args:
        file_path (str): name of the param file to load
    
    Returns:
        map : key and values in the file
    """
    

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