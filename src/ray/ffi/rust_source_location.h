// Copyright 2024 The Ray Authors.
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

#include <iostream>
#include <string>
#include <cstdint>

#include "rust/cxx.h"

// Include the generated CXX bridge header
#include "src/ray/ffi/source_location_bridge_gen.h"

namespace ray {

/// SourceLocation backed by Rust implementation.
class RustSourceLocation {
 public:
  /// Default constructor (invalid location).
  RustSourceLocation() : impl_(ffi::source_location_new()) {}

  /// Constructor with file and line.
  RustSourceLocation(const std::string& filename, int line)
      : impl_(ffi::source_location_with_location(filename, line)) {}

  /// Check if this is a valid source location.
  bool IsValid() const {
    return ffi::source_location_is_valid(*impl_);
  }

  /// Get the filename.
  std::string filename() const {
    return static_cast<std::string>(ffi::source_location_filename(*impl_));
  }

  /// Get the line number.
  int line_no() const {
    return ffi::source_location_line_no(*impl_);
  }

  /// Convert to string representation.
  std::string ToString() const {
    return static_cast<std::string>(ffi::source_location_to_string(*impl_));
  }

  /// Stream output operator.
  friend std::ostream& operator<<(std::ostream& os, const RustSourceLocation& loc) {
    os << loc.ToString();
    return os;
  }

 private:
  rust::Box<ffi::RustSourceLocation> impl_;
};

}  // namespace ray

/// Macro to create a RustSourceLocation at the current location.
#define RUST_LOC() \
  ray::RustSourceLocation { __FILE__, __LINE__ }
