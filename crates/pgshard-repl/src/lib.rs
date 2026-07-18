//! Logical replication support for pgshard.
//!
//! The reshard seeder, CDC (vstream) endpoint, and online-DDL backfill all
//! consume a PostgreSQL logical-replication stream in the `pgoutput` format.
//! [`pgoutput`] decodes that stream; higher layers filter by keyspace-id and
//! apply with a transactional checkpoint.

pub mod filter;
pub mod pgoutput;
pub mod stream;
