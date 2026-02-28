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

#include <string>
#include <vector>

#include "rust/cxx.h"
#include "src/ray/ffi/cmd_line_utils_bridge_gen.h"

namespace ray {

/// Command line syntax type.
enum class RustCommandLineSyntax { POSIX, Windows };

/// Parse a command line string into a vector of arguments.
inline std::vector<std::string> RustParseCommandLine(const std::string &cmdline,
                                                     RustCommandLineSyntax syntax) {
  rust::Vec<rust::String> result;
  if (syntax == RustCommandLineSyntax::POSIX) {
    result = ::ffi::cmd_parse_posix(cmdline);
  } else {
    result = ::ffi::cmd_parse_windows(cmdline);
  }

  std::vector<std::string> args;
  args.reserve(result.size());
  for (const auto &arg : result) {
    args.emplace_back(std::string(arg));
  }
  return args;
}

/// Create a command line string from a vector of arguments.
inline std::string RustCreateCommandLine(const std::vector<std::string> &args,
                                         RustCommandLineSyntax syntax) {
  rust::Vec<rust::String> rust_args;
  rust_args.reserve(args.size());
  for (const auto &arg : args) {
    rust_args.push_back(rust::String(arg));
  }

  // Convert Vec to Slice for FFI call
  rust::Slice<const rust::String> slice(rust_args.data(), rust_args.size());

  rust::String result;
  if (syntax == RustCommandLineSyntax::POSIX) {
    result = ::ffi::cmd_create_posix(slice);
  } else {
    result = ::ffi::cmd_create_windows(slice);
  }
  return std::string(result);
}

// Aliases for compatibility with original API
using CommandLineSyntax = RustCommandLineSyntax;

inline std::vector<std::string> ParseCommandLine(const std::string &cmdline,
                                                  CommandLineSyntax syntax) {
  return RustParseCommandLine(cmdline, syntax);
}

inline std::string CreateCommandLine(const std::vector<std::string> &args,
                                      CommandLineSyntax syntax) {
  return RustCreateCommandLine(args, syntax);
}

}  // namespace ray
