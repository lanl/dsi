"""
A collection of pydantic models for Plugin schema validation
"""

from pydantic import BaseModel, create_model


def create_dynamic_model(name: str, col_names: list[str],
                         col_types: list[type], base=None) -> BaseModel:
    """
    Creates a pydantic model at runtime with given name, column names
    and types, and an optional base model to extend.

    This is useful for when column names are not known until
    they are retrieved at runtime.
    """
    name_type_dict = {k: (v, ...) for k, v in zip(col_names, col_types)}
    return create_model(name, __base__=base, **name_type_dict)


class EnvironmentModel(BaseModel):
    uid: int
    effective_gid: int
    moniker: str
    gid_list: list[int]


class HostnameModel(EnvironmentModel):
    hostname: str


class GitInfoModel(EnvironmentModel):
    git_remote: str
    git_commit: str


class SystemKernelModel(EnvironmentModel):
    kernel_info: str
