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

#include <memory>
#include <string>
#include <cstdint>
#include <optional>

#include "rust/cxx.h"

// Include the generated CXX bridge header
#include "src/ray/ffi/shared_lru_bridge_gen.h"

namespace ray {

/// Thread-safe shared LRU cache backed by Rust implementation.
/// This matches the interface of ThreadSafeSharedLruCache<string, string>.
class RustSharedLruCache {
 public:
  explicit RustSharedLruCache(size_t max_entries)
      : impl_(ffi::shared_lru_cache_new(max_entries)) {}

  /// Insert `value` with key `key`. This will replace any previous entry with
  /// the same key.
  void Put(const std::string& key, const std::string& value) {
    ffi::shared_lru_cache_put(*impl_, key, value);
  }

  /// Look up the entry with key `key`. Return nullptr if key doesn't exist.
  std::shared_ptr<std::string> Get(const std::string& key) {
    if (!ffi::shared_lru_cache_has(*impl_, key)) {
      return nullptr;
    }
    auto val = ffi::shared_lru_cache_get(*impl_, key);
    return std::make_shared<std::string>(static_cast<std::string>(val));
  }

  /// Delete the entry with key `key`. Return true if the entry was found.
  bool Delete(const std::string& key) {
    return ffi::shared_lru_cache_delete(*impl_, key);
  }

  /// Clear the cache.
  void Clear() {
    ffi::shared_lru_cache_clear(*impl_);
  }

  /// Get the maximum number of entries.
  size_t max_entries() const {
    return ffi::shared_lru_cache_max_entries(*impl_);
  }

  /// Get the current number of entries.
  size_t size() const {
    return ffi::shared_lru_cache_len(*impl_);
  }

 private:
  rust::Box<ffi::RustSharedLruCache> impl_;
};

/// Integer-keyed LRU cache backed by Rust.
/// This matches ThreadSafeSharedLruCache<int, int>.
class RustIntLruCache {
 public:
  explicit RustIntLruCache(size_t max_entries)
      : impl_(ffi::int_lru_cache_new(max_entries)) {}

  void Put(int key, int value) {
    ffi::int_lru_cache_put(*impl_, key, value);
  }

  std::shared_ptr<int> Get(int key) {
    if (!ffi::int_lru_cache_has(*impl_, key)) {
      return nullptr;
    }
    return std::make_shared<int>(ffi::int_lru_cache_get(*impl_, key));
  }

  size_t max_entries() const {
    return ffi::int_lru_cache_max_entries(*impl_);
  }

 private:
  rust::Box<ffi::RustIntLruCache> impl_;
};

}  // namespace ray
