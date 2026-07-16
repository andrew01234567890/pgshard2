//! Core domain types for pgshard: keyspace ids, key ranges, shard functions,
//! LSNs, and the sharding schema shared by the router, agent, and CLI.

pub mod keyspace;
pub mod lsn;
pub mod shardfn;
pub mod vschema;

pub use keyspace::{KeyRange, KeyspaceId, PartitionError, validate_partition};
pub use lsn::Lsn;
pub use shardfn::{ScalarValue, ShardFnError, ShardFunction, shard_function};
pub use vschema::{SequenceBinding, TableDef, TableName, VSchema, VSchemaError};
