use std::collections::BTreeMap;
use std::fmt;

use thiserror::Error;

use crate::shardfn;

/// Fully qualified table name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableName {
    pub schema: String,
    pub name: String,
}

impl TableName {
    pub fn new(schema: impl Into<String>, name: impl Into<String>) -> Self {
        TableName {
            schema: schema.into(),
            name: name.into(),
        }
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.schema, self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceBinding {
    pub column: String,
    pub sequence: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableDef {
    Sharded {
        shard_key_column: String,
        shard_function: String,
        sequences: Vec<SequenceBinding>,
    },
    /// Lives in the system (unsharded) database.
    Global,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum VSchemaError {
    #[error("empty schema or table name in {0:?}.{1:?}")]
    EmptyTableName(String, String),
    #[error("table {0} declared twice")]
    DuplicateTable(TableName),
    #[error("table {0}: shard key column is empty")]
    EmptyShardKey(TableName),
    #[error("table {0}: unknown shard function {1:?}")]
    UnknownShardFunction(TableName, String),
    #[error("table {0}: column {1:?} bound to multiple sequences")]
    DuplicateSequenceColumn(TableName, String),
}

/// The sharding schema: which tables are sharded, by what key, with which
/// sequence bindings.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VSchema {
    tables: BTreeMap<TableName, TableDef>,
}

impl VSchema {
    pub fn insert(&mut self, table: TableName, def: TableDef) -> Result<(), VSchemaError> {
        if table.schema.is_empty() || table.name.is_empty() {
            return Err(VSchemaError::EmptyTableName(table.schema, table.name));
        }
        if self.tables.contains_key(&table) {
            return Err(VSchemaError::DuplicateTable(table));
        }
        if let TableDef::Sharded {
            shard_key_column,
            shard_function,
            sequences,
        } = &def
        {
            if shard_key_column.is_empty() {
                return Err(VSchemaError::EmptyShardKey(table));
            }
            if shardfn::shard_function(shard_function).is_err() {
                return Err(VSchemaError::UnknownShardFunction(
                    table,
                    shard_function.clone(),
                ));
            }
            let mut seen = std::collections::HashSet::new();
            for binding in sequences {
                if !seen.insert(&binding.column) {
                    return Err(VSchemaError::DuplicateSequenceColumn(
                        table,
                        binding.column.clone(),
                    ));
                }
            }
        }
        self.tables.insert(table, def);
        Ok(())
    }

    pub fn get(&self, table: &TableName) -> Option<&TableDef> {
        self.tables.get(table)
    }

    pub fn tables(&self) -> impl Iterator<Item = (&TableName, &TableDef)> {
        self.tables.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sharded(function: &str) -> TableDef {
        TableDef::Sharded {
            shard_key_column: "customer_id".into(),
            shard_function: function.into(),
            sequences: vec![SequenceBinding {
                column: "id".into(),
                sequence: "orders_id".into(),
            }],
        }
    }

    #[test]
    fn accepts_valid_tables_and_rejects_duplicates() {
        let mut schema = VSchema::default();
        let orders = TableName::new("public", "orders");
        schema
            .insert(orders.clone(), sharded("xxhash64_v1"))
            .unwrap();
        schema
            .insert(TableName::new("public", "settings"), TableDef::Global)
            .unwrap();
        assert_eq!(
            schema.insert(orders.clone(), TableDef::Global),
            Err(VSchemaError::DuplicateTable(orders))
        );
    }

    #[test]
    fn rejects_empty_names() {
        let mut schema = VSchema::default();
        assert_eq!(
            schema.insert(TableName::new("", "orders"), TableDef::Global),
            Err(VSchemaError::EmptyTableName("".into(), "orders".into()))
        );
        assert_eq!(
            schema.insert(TableName::new("public", ""), TableDef::Global),
            Err(VSchemaError::EmptyTableName("public".into(), "".into()))
        );
    }

    #[test]
    fn rejects_unknown_shard_function() {
        let mut schema = VSchema::default();
        let t = TableName::new("public", "orders");
        assert_eq!(
            schema.insert(t.clone(), sharded("md5")),
            Err(VSchemaError::UnknownShardFunction(t, "md5".into()))
        );
    }
}
