// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <string>

#include "rust/cxx.h"
#include "src/ray/ffi/spilled_object_bridge_gen.h"

namespace ray {

/// Result of parsing an object URL.
class RustParsedObjectUrl {
 public:
  RustParsedObjectUrl(ffi::ParsedObjectUrl result)
      : success_(result.success),
        file_path_(std::string(result.file_path)),
        offset_(result.offset),
        size_(result.size) {}

  bool success() const { return success_; }
  const std::string &file_path() const { return file_path_; }
  uint64_t offset() const { return offset_; }
  uint64_t size() const { return size_; }

 private:
  bool success_;
  std::string file_path_;
  uint64_t offset_;
  uint64_t size_;
};

/// Parse an object URL in the format: {path}?offset={offset}&size={size}
inline RustParsedObjectUrl RustParseObjectURL(const std::string &url) {
  return RustParsedObjectUrl(ffi::parse_object_url(url));
}

/// Convert 8 bytes (little-endian) to uint64.
inline uint64_t RustBytesToUint64(const std::string &bytes) {
  rust::Slice<const uint8_t> slice(reinterpret_cast<const uint8_t *>(bytes.data()),
                                   bytes.size());
  return ffi::bytes_to_uint64(slice);
}

/// Calculate the number of chunks needed for a given data size and chunk size.
inline uint64_t RustCalculateNumChunks(uint64_t data_size, uint64_t chunk_size) {
  return ffi::calculate_num_chunks(data_size, chunk_size);
}

}  // namespace ray
