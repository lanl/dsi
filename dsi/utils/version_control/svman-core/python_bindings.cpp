#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/unordered_map.h>
#include <nanobind/stl/vector.h>

#include "chunking.hpp"
#include "sha256.hpp"

namespace nb = nanobind;

namespace {
void update_sha256(chunking::Sha256Ctx &ctx, nb::handle data) {
    Py_buffer view{};
    if (PyObject_GetBuffer(data.ptr(), &view, PyBUF_CONTIG_RO) != 0)
        throw nb::python_error();
    try {
        ctx.update(view.buf, static_cast<std::size_t>(view.len));
    } catch (...) {
        PyBuffer_Release(&view);
        throw;
    }
    PyBuffer_Release(&view);
}
}

NB_MODULE(_svman_core, m) {
    m.doc() = "Native snapshot storage for dsi-vcs.";

    nb::class_<chunking::Sha256Ctx>(m, "Sha256")
        .def(nb::init<>())
        .def("update", &update_sha256, nb::arg("data"))
        .def("hexdigest", &chunking::Sha256Ctx::hexdigest)
        .def("digest", [](const chunking::Sha256Ctx &ctx) {
            const auto digest = ctx.rawdigest();
            return nb::bytes(reinterpret_cast<const char *>(digest.data()), digest.size());
        });

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
