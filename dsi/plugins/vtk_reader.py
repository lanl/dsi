import math
import os
from collections import OrderedDict
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
try:
    import vtk
except ImportError:
    raise RuntimeError("Must have vtk package installed to use the VTK reader.")

from dsi.plugins.file_reader import FileReader

class VTK_Reader(FileReader):
    """
    A DSI Reader that reads in VTK data -- can be vtk,vti,vtm,vtu data
    """
    def __init__(self, filenames, table_name = None, **kwargs):
        """
        Initializes the VTK Reader with user specified filenames and optional table_name.

        `filenames` : str or list of str
            Required. One or more VTK file paths to be loaded into DSI.
            If multiple files are provided, all data must correspond to the same table.
        """
        super().__init__(filenames, **kwargs)
        self.vtk_data = OrderedDict()
        if isinstance(filenames, str):
            self.filenames = [filenames]
        else:
            self.filenames = filenames
        self.table_name = table_name
        
        self.readers = {
                ".vti": ("vtkXMLImageDataReader", "VTK XML ImageData"),
                ".vtm": ("vtkXMLMultiBlockDataReader", "VTK XML MultiBlockDataSet"),
                ".vtk": ("vtkGenericDataObjectReader", "VTK legacy"),
            }

    def _finite(self, value: Any) -> float | int | None:
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(number):
            return None
        return int(number) if number.is_integer() else number

    def _numbers(self, values: Iterable[Any]) -> list[float | int | None]:
        return [self._finite(value) for value in values]

    def _array_roles(self, attributes: Any) -> dict[int, list[str]]:
        roles: dict[int, list[str]] = {}
        getters = {
            "scalars": "GetScalars",
            "vectors": "GetVectors",
            "normals": "GetNormals",
            "texture_coordinates": "GetTCoords",
            "tensors": "GetTensors",
            "global_ids": "GetGlobalIds",
            "pedigree_ids": "GetPedigreeIds",
        }
        for role, method_name in getters.items():
            method = getattr(attributes, method_name, None)
            active = method() if method else None
            if active is not None:
                roles.setdefault(hash(active), []).append(role)
        return roles

    def _array_metadata(self, array: Any, index: int, roles: list[str] | None = None) -> dict[str, Any]:
        name = array.GetName()
        components = int(array.GetNumberOfComponents())
        result: dict[str, Any] = {
            "index": index,
            "name": name if name else None,
            "vtk_class": array.GetClassName(),
            "data_type": array.GetDataTypeAsString(),
            "components": components,
            "tuples": int(array.GetNumberOfTuples()),
            "component_names": [
                array.GetComponentName(i) if array.GetComponentName(i) else None
                for i in range(components)
            ],
            "roles": roles or [],
        }

        numeric = vtk.vtkDataArray.SafeDownCast(array)
        if numeric is not None:
            ranges_list = [
                {
                    "component": i,
                    "name": result["component_names"][i],
                    "min": self._finite(numeric.GetRange(i)[0]),
                    "max": self._finite(numeric.GetRange(i)[1]),
                }
                for i in range(components)
            ]
            ranges_dict = {key: [row[key] for row in ranges_list] for key in ranges_list[0]} if ranges_list else {}
            for key, values in ranges_dict.items():
                result[f"ranges.{key}"] = values
            if components > 1:
                magnitude_range = numeric.GetRange(-1)
                result["magnitude_range"] = {
                    "min": self._finite(magnitude_range[0]),
                    "max": self._finite(magnitude_range[1]),
                }
        return result

    def _association_metadata(self, container: Any, has_roles: bool) -> list[dict[str, Any]]:
        roles = self._array_roles(container) if has_roles else {}
        arrays: list[dict[str, Any]] = []
        for index in range(container.GetNumberOfArrays()):
            array = container.GetAbstractArray(index)
            if array is not None:
                arrays.append(self._array_metadata(array, index, roles.get(hash(array))))
        if arrays:
            arrays = {key: [row[key] for row in arrays] for key in arrays[0]}
        return arrays

    def _time_metadata(self, reader: Any) -> dict[str, Any]:
        info = reader.GetOutputInformation(0)
        pipeline = vtk.vtkStreamingDemandDrivenPipeline
        result: dict[str, Any] = {"steps": [], "range": None}

        steps_key = pipeline.TIME_STEPS()
        if info is not None and info.Has(steps_key):
            result["steps"] = self._numbers(
                info.Get(steps_key, i) for i in range(info.Length(steps_key))
            )

        range_key = pipeline.TIME_RANGE()
        if info is not None and info.Has(range_key):
            result["range"] = self._numbers(
                info.Get(range_key, i) for i in range(info.Length(range_key))
            )
        return result

    def _reader_for(self, path: Path) -> tuple[Any, str]:
        try:
            reader_name, file_format = self.readers[path.suffix.lower()]
        except KeyError:
            raise ValueError(
                f"Unsupported extension {path.suffix!r}; expected .vtk, .vti, or .vtm"
            ) from None
        reader = getattr(vtk, reader_name)()
        reader.SetFileName(str(path))
        return reader, file_format

    def _direction_matrix(self, dataset: Any) -> list[list[float | int | None]] | None:
        getter = getattr(dataset, "GetDirectionMatrix", None)
        if getter is None:
            return None
        matrix = getter()
        if matrix is None:
            return None
        return [[self._finite(matrix.GetElement(r, c)) for c in range(3)] for r in range(3)]

    def _bounds(self, dataset: Any) -> list[float | int | None] | None:
        getter = getattr(dataset, "GetBounds", None)
        if getter is None:
            return None

        try:
            values = getter()
        except TypeError:
            values = [0.0] * 6
            getter(values)

        return self._numbers(values) if values is not None else None

    def _dataset_metadata(self, dataset: Any) -> dict[str, Any]:
        result: dict[str, Any] = {
            "vtk_class": dataset.GetClassName(),
            "vtk_data_object_type": int(dataset.GetDataObjectType()),
            "actual_memory_size_kib": int(dataset.GetActualMemorySize()),
            "points": int(dataset.GetNumberOfPoints())
            if hasattr(dataset, "GetNumberOfPoints")
            else None,
            "cells": int(dataset.GetNumberOfCells())
            if hasattr(dataset, "GetNumberOfCells")
            else None,
            "bounds": self._bounds(dataset),
        }

        structured_getters = {
            "extent": "GetExtent",
            "dimensions": "GetDimensions",
            "origin": "GetOrigin",
            "spacing": "GetSpacing",
        }
        for key, method_name in structured_getters.items():
            method = getattr(dataset, method_name, None)
            result[key] = self._numbers(method()) if method else None
        result["direction"] = self._direction_matrix(dataset)
        return result

    def _arrays_metadata(self, dataset: Any) -> dict[str, list[dict[str, Any]]]:
        point_data = dataset.GetPointData() if hasattr(dataset, "GetPointData") else None
        cell_data = dataset.GetCellData() if hasattr(dataset, "GetCellData") else None
        field_data = dataset.GetFieldData() if hasattr(dataset, "GetFieldData") else None
        return {
            "point": self._association_metadata(point_data, True) if point_data else [],
            "cell": self._association_metadata(cell_data, True) if cell_data else [],
            "field": self._association_metadata(field_data, False) if field_data else [],
        }

    def _union_bounds(self, blocks: list[dict[str, Any]]) -> list[float | int | None] | None:
        valid = []
        for block in blocks:
            bounds = block.get("bounds")
            if (
                bounds is not None
                and len(bounds) == 6
                and all(value is not None for value in bounds)
                and all(bounds[i] <= bounds[i + 1] for i in (0, 2, 4))
            ):
                valid.append(bounds)
        if not valid:
            return None
        return [
            min(bounds[0] for bounds in valid),
            max(bounds[1] for bounds in valid),
            min(bounds[2] for bounds in valid),
            max(bounds[3] for bounds in valid),
            min(bounds[4] for bounds in valid),
            max(bounds[5] for bounds in valid),
        ]

    def _composite_metadata(self, dataset: Any) -> list[dict[str, Any]]:
        iterator = dataset.NewIterator()
        if hasattr(iterator, "SkipEmptyNodesOn"):
            iterator.SkipEmptyNodesOn()
        if hasattr(iterator, "VisitOnlyLeavesOn"):
            iterator.VisitOnlyLeavesOn()

        blocks: list[dict[str, Any]] = []
        name_key = vtk.vtkCompositeDataSet.NAME()
        iterator.InitTraversal()
        while not iterator.IsDoneWithTraversal():
            block = iterator.GetCurrentDataObject()
            if block is not None:
                info = iterator.GetCurrentMetaData()
                name = info.Get(name_key) if info is not None and info.Has(name_key) else None
                block_info = {
                    "flat_index": int(iterator.GetCurrentFlatIndex()),
                    "name": name,
                    **self._dataset_metadata(block),
                    "arrays": self._arrays_metadata(block),
                }
                blocks.append(block_info)
            iterator.GoToNextItem()
        return blocks

    def extract_metadata(self, filename: str | os.PathLike[str]) -> dict[str, Any]:
        path = Path(filename).expanduser().resolve()
        reader, file_format = self._reader_for(path)

        suppress_vtm_errors = path.suffix.lower() == ".vtm"
        previous_warning_state = vtk.vtkObject.GetGlobalWarningDisplay()
        if suppress_vtm_errors:
            vtk.vtkObject.GlobalWarningDisplayOff()
        try:
            reader.UpdateInformation()
            time = self._time_metadata(reader)
            reader.Update()
        finally:
            if suppress_vtm_errors:
                vtk.vtkObject.SetGlobalWarningDisplay(previous_warning_state)

        dataset = reader.GetOutputDataObject(0)
        if dataset is None:
            raise RuntimeError(f"VTK could not read a data object from: {path}")

        stat = path.stat()
        dataset_info = self._dataset_metadata(dataset)
        dataset_info["leaf_block_count"] = None
        dataset_info["blocks"] = None

        composite = vtk.vtkCompositeDataSet.SafeDownCast(dataset)
        if composite is not None:
            blocks = self._composite_metadata(composite)
            dataset_info["leaf_block_count"] = len(blocks)
            dataset_info["points"] = sum(block["points"] or 0 for block in blocks)
            dataset_info["cells"] = sum(block["cells"] or 0 for block in blocks)
            dataset_info["bounds"] = self._union_bounds(blocks)

        metadata: dict[str, Any] = {
            "file": {
                "path": str(path),
                "name": path.name,
                "format": file_format,
                "size_bytes": stat.st_size,
                "modified_utc": datetime.fromtimestamp(
                    stat.st_mtime, tz=timezone.utc
                ).isoformat(),
                "legacy_header": None,
                "legacy_encoding": None,
            },
            "dataset": dataset_info,
            "time": time,
            "arrays": self._arrays_metadata(dataset),
        }

        if path.suffix.lower() == ".vtk":
            get_header = getattr(reader, "GetHeader", None)
            metadata["file"]["legacy_header"] = get_header() if get_header else None
            get_file_type = getattr(reader, "GetFileType", None)
            if get_file_type:
                metadata["file"]["legacy_encoding"] = (
                    "binary" if get_file_type() == vtk.VTK_BINARY else "ascii"
                )

        return metadata

    def flatten_metadata(self, metadata: Mapping[str, Any], prefix: str = "") -> OrderedDict[str, Any]:
        flattened: OrderedDict[str, Any] = OrderedDict()
        for key, value in metadata.items():
            flat_key = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(value, Mapping):
                flattened.update(self.flatten_metadata(value, flat_key))
            else:
                flattened[flat_key] = value
        return flattened

    def add_rows(self) -> OrderedDict[str, list[Any]]:

        rows = [self.flatten_metadata(self.extract_metadata(filename)) for filename in self.filenames]
        if not rows:
            raise ValueError("At least one input file is required")
        schema = OrderedDict.fromkeys(key for row in rows for key in row)

        vtk_dict = OrderedDict((key, [row.get(key, None) for row in rows]) for key in schema)

        if self.table_name is None:
            self.vtk_data["vtk_metadata"] = vtk_dict
        else:
            self.vtk_data[self.table_name] = vtk_dict
        self.set_schema_2(self.vtk_data)