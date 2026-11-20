#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/unordered_map.h>
#include <nanobind/stl/vector.h>

#include "chunking.hpp"

namespace nb = nanobind;

NB_MODULE(_svman_core, m) {
    m.doc() = "Native snapshot storage for dsi-vcs.";

    nb::class_<chunking::FileEntry>(m, "FileEntry")
        .def(nb::init<>())
        .def_rw("absolute_path", &chunking::FileEntry::absolutePath)
        .def_rw("relative_path", &chunking::FileEntry::relativePath)
        .def_rw("file_type", &chunking::FileEntry::fileType);

    nb::class_<chunking::StoreResult>(m, "StoreResult")
        .def_ro("file_hashes", &chunking::StoreResult::fileHashes)
        .def_ro("chunk_counts", &chunking::StoreResult::chunkLength)
        .def_ro("manifest_roots", &chunking::StoreResult::manifestRoots);

    m.def("store_snapshot", [](const std::string &root,
                                const std::vector<chunking::FileEntry> &files,
                                const std::unordered_map<std::string, std::string> &base_roots) {
        nb::gil_scoped_release release;
        return chunking::storeChunksForSnapshot(root, files, 0, base_roots, false);
    }, nb::arg("root"), nb::arg("files"), nb::arg("base_roots"));

    m.def("rebuild_file", [](const std::string &root, const std::string &relative_path,
                             const std::string &manifest_root, const std::string &file_hash,
                             const std::string &output_directory) {
        nb::gil_scoped_release release;
        return chunking::rebuildFileFromManifest(root, relative_path, manifest_root,
                                                file_hash, output_directory);
    }, nb::arg("root"), nb::arg("relative_path"), nb::arg("manifest_root"),
       nb::arg("file_hash"), nb::arg("output_directory"));
}
