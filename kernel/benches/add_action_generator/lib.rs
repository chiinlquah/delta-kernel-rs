// Copyright 2025 The Delta Kernel Rust Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Library interface for add_action_generator
//!
//! Provides functions to generate Add actions and write them to checkpoint format parquet files.
//! This library is used by multiple binaries including the add-action-generator CLI tool and
//! the backfill-delta-table tool.

pub mod deletion_vector;
pub mod generator;
pub mod stats;
pub mod writer;

// Re-export commonly used types for convenience
pub use generator::{generate_add_actions, AddActionMetadata};
pub use writer::write_checkpoint_parquet;
