import inspect

from dsi.backends.webserver import Webserver

VALID_HPC_LOCATION_TYPES = ["github", "hpc", "hpc-kerberos", "url", "s3", "local"]


def webserver_backend_names(module_collection):
    '''
    Returns valid webserver backend names by inspecting a Terminal's module_collection['backend'].
    '''
    names = []
    for mod in module_collection.values():
        for _, cls in inspect.getmembers(mod, inspect.isclass):
            if cls.__module__ == mod.__name__ and issubclass(cls, Webserver) and cls is not Webserver:
                names.append(cls.__name__)
    return names


def build_source_entry(valid_webserver_names, webserver_name=None, location_type=None,
                        location=None, path=None, submitter_name=""):
    '''
    Builds a federation source entry dict (webserver or HPC), inferring kind from which fields
    were given. Raises ValueError on invalid/ambiguous input.

    Args:
        valid_webserver_names: list of registered webserver backend names to validate against
            (e.g. from webserver_backend_names()).
        webserver_name: a webserver backend name (e.g. "OSTI"), or None for an HPC source.
        location_type, location, path, submitter_name: HPC source fields, or None for a webserver.
    '''
    hpc_fields = [location_type, location, path]
    if webserver_name and any(hpc_fields):
        raise ValueError("specify either a webserver name OR the HPC fields, not both.")

    if webserver_name:
        match = next((n for n in valid_webserver_names if n.lower() == webserver_name.lower()), None)
        if match is None:
            raise ValueError(f"'{webserver_name}' is not a valid webserver backend. Valid options: {', '.join(valid_webserver_names)}")
        return {"kind": "webserver", "backend_name": match}

    elif all(hpc_fields):
        if location_type.lower() not in VALID_HPC_LOCATION_TYPES:
            raise ValueError(f"location_type must be one of {', '.join(VALID_HPC_LOCATION_TYPES)}")
        return {
            "kind": "hpc",
            "location_type": location_type,
            "location": location,
            "path": path,
            "submitter_name": submitter_name,
        }

    else:
        raise ValueError("provide either a webserver name, or all three of location_type, location, path")
