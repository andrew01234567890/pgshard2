//! Planner v1: route a parsed statement to shards.
//!
//! Given one parsed statement, the sharding schema ([`VSchema`]), and the
//! database's [`ShardCatalog`], [`plan`] returns a [`Plan`] telling the router
//! where the statement goes: one shard, a scatter of shards, every shard, the
//! unsharded system database, or a rejection.
//!
//! # v1 scope and guarantees
//!
//! The planner never *under-routes*: the shards it targets always cover every
//! row the statement can touch (see [`extract`]). Within that guarantee it is
//! deliberately conservative:
//!
//! - Shard keys are read from top-level `key = value` / `key IN (...)`
//!   constraints (AND-reachable); OR/NOT subtrees are not mined, so such a read
//!   scatters and such a write is rejected.
//! - Literal shard keys are hashed by their SQL form — integer literals as
//!   `Int64`, string literals as `Text`. Typed hashing (uuid/bytea columns) is
//!   deferred; the data-plane insert path hashes the same way, so routing stays
//!   consistent.
//! - A `$n` shard key yields a [`Plan::Parameterized`]: the value is known only
//!   at Bind, so bind-time resolution is left to the executor.
//! - Cross-shard writes, keyless writes, and updates of the shard key are
//!   rejected with SQLSTATE `0A000`. MERGE and COPY are not yet routed.
//! - `search_path` is not modeled: an unqualified relation defaults to `public`.
//! - Joins are rejected: routing is decided from a single sharded table in the
//!   top-level FROM. Only that clause is analyzed, so a subquery referencing a
//!   sharded table on another shard is **not** seen here — enabling the scatter
//!   executor's single-shard fast path for statements that contain subqueries
//!   is gated separately (a follow-up), so this planner is never the thing that
//!   under-routes such a query on its own.

pub mod catalog;
mod extract;

use std::collections::BTreeSet;

use pg_query::NodeEnum;
use pg_query::protobuf::{DeleteStmt, InsertStmt, SelectStmt, UpdateStmt};

use pgshard_core::{TableDef, TableName, VSchema, shard_function};
use pgshard_sql::{Parsed, StatementKind};

pub use catalog::{ShardCatalog, ShardId};
use extract::{KeyVal, from_tables, range_var_table, where_key_values};

/// SQLSTATE `feature_not_supported`: what a statement the router cannot route
/// (a cross-shard write, an unsupported form) is rejected with.
const CROSS_SHARD: &str = "0A000";

/// Where the router should send a statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// Exactly one shard: every shard key resolved to the same shard.
    SingleShard(ShardId),
    /// Fan a read out to these shards and merge the results.
    Scatter(Vec<ShardId>),
    /// Run on every shard (schema/DDL change).
    Broadcast(Vec<ShardId>),
    /// The unsharded system database (only global tables are involved).
    Unsharded,
    /// Not shard-routed: the router's session layer handles it (SET, SHOW,
    /// transaction control, PREPARE/EXECUTE, empty statement).
    RouterLocal,
    /// The shard key is a bind parameter; routing finishes at Bind.
    Parameterized(Parameterized),
    /// The statement cannot be routed; the router returns this SQLSTATE.
    Reject { code: &'static str, reason: String },
}

/// A plan whose shard key is only known once bind parameters arrive. The
/// executor re-resolves it against the bound values before running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parameterized {
    /// Shard function that maps each bound shard-key value to a keyspace id.
    pub shard_function: String,
    /// The 1-based `$n` positions that supply shard-key values.
    pub param_indices: Vec<u32>,
    /// True for INSERT/UPDATE/DELETE: at Bind every value must land on one
    /// shard or the write is rejected as cross-shard.
    pub write: bool,
}

/// Plan a single statement. `kind` is the classification from [`pgshard_sql`];
/// `node` is that statement's AST root.
pub fn plan(
    kind: StatementKind,
    node: &NodeEnum,
    vschema: &VSchema,
    shards: &ShardCatalog,
) -> Plan {
    match (kind, node) {
        (StatementKind::Select, NodeEnum::SelectStmt(s)) => plan_select(s, vschema, shards),
        (StatementKind::Insert, NodeEnum::InsertStmt(s)) => plan_insert(s, vschema, shards),
        (StatementKind::Update, NodeEnum::UpdateStmt(s)) => plan_update(s, vschema, shards),
        (StatementKind::Delete, NodeEnum::DeleteStmt(s)) => plan_delete(s, vschema, shards),
        (StatementKind::Ddl, _) => Plan::Broadcast(shards.all()),
        (StatementKind::Merge, _) => reject("MERGE is not supported in M1"),
        (StatementKind::Copy, _) => reject("COPY to a sharded table is not supported in M1"),
        (StatementKind::Other, _) => {
            reject("data-modifying CTEs and this statement form are not supported in M1")
        }
        // SET / SHOW / transaction control / PREPARE / EXECUTE / empty.
        _ => Plan::RouterLocal,
    }
}

/// Plan every statement of a parsed (possibly multi-statement) query.
pub fn plan_all(parsed: &Parsed, vschema: &VSchema, shards: &ShardCatalog) -> Vec<Plan> {
    let kinds = parsed.statements();
    parsed
        .result()
        .protobuf
        .stmts
        .iter()
        .enumerate()
        .map(|(i, raw)| {
            let kind = kinds.get(i).copied().unwrap_or(StatementKind::Other);
            match raw.stmt.as_ref().and_then(|s| s.node.as_ref()) {
                Some(node) => plan(kind, node, vschema, shards),
                None => Plan::RouterLocal,
            }
        })
        .collect()
}

fn reject(reason: &str) -> Plan {
    Plan::Reject {
        code: CROSS_SHARD,
        reason: reason.to_owned(),
    }
}

/// How a set of shard-key values resolves against the catalog.
enum Resolution {
    /// No usable shard-key constraint was found.
    Unkeyed,
    /// At least one value is a bind parameter; carries every `$n` seen.
    Params(Vec<u32>),
    /// All values are literals; carries the distinct shards they hit.
    Shards(Vec<ShardId>),
}

fn route_values(values: &[KeyVal], shard_fn: &str, shards: &ShardCatalog) -> Resolution {
    if values.is_empty() {
        return Resolution::Unkeyed;
    }
    let params: Vec<u32> = values
        .iter()
        .filter_map(|v| match v {
            KeyVal::Param(n) => Some(*n),
            KeyVal::Const(_) => None,
        })
        .collect();
    if !params.is_empty() {
        return Resolution::Params(params);
    }
    let func = shard_function(shard_fn).expect("vschema validates the shard function name");
    let mut hit = BTreeSet::new();
    for v in values {
        if let KeyVal::Const(sv) = v {
            hit.insert(shards.route(func.keyspace_id(sv)).clone());
        }
    }
    Resolution::Shards(hit.into_iter().collect())
}

/// Turn a resolved literal shard set into a read plan.
fn read_from_shards(mut hit: Vec<ShardId>) -> Plan {
    if hit.len() == 1 {
        Plan::SingleShard(hit.pop().expect("len checked"))
    } else {
        Plan::Scatter(hit)
    }
}

/// The sharded/global classification of a referenced table.
enum Placement<'a> {
    Sharded { key: &'a str, shard_fn: &'a str },
    Global,
    Unknown,
}

fn placement<'a>(vschema: &'a VSchema, table: &TableName) -> Placement<'a> {
    match vschema.get(table) {
        Some(TableDef::Sharded {
            shard_key_column,
            shard_function,
            ..
        }) => Placement::Sharded {
            key: shard_key_column,
            shard_fn: shard_function,
        },
        Some(TableDef::Global) => Placement::Global,
        None => Placement::Unknown,
    }
}

fn plan_select(s: &SelectStmt, vschema: &VSchema, shards: &ShardCatalog) -> Plan {
    let tables = from_tables(&s.from_clause);
    let sharded: Vec<(&str, &str)> = tables
        .iter()
        .filter_map(|t| match placement(vschema, t) {
            Placement::Sharded { key, shard_fn } => Some((key, shard_fn)),
            _ => None,
        })
        .collect();

    if sharded.is_empty() {
        // No sharded table: a read of only global/system relations goes to the
        // unsharded system database; a tableless read (`SELECT 1`) is handled
        // by the router itself.
        return if tables.is_empty() {
            Plan::RouterLocal
        } else {
            Plan::Unsharded
        };
    }

    // A join — more than one relation alongside the sharded table — cannot be
    // routed by shard key without cross-table co-location analysis (which needs
    // per-alias qualifier resolution the extractor does not do), and joins
    // across sharded tables or to a global table are out of M1 scope. Reject
    // rather than risk under-routing.
    let [(key, shard_fn)] = sharded[..] else {
        return reject("SELECT joining multiple sharded tables is not supported in M1");
    };
    if tables.len() > 1 {
        return reject(
            "SELECT joining a sharded table with another relation is not supported in M1",
        );
    }

    let values = where_key_values(s.where_clause.as_deref(), key);
    match route_values(&values, shard_fn, shards) {
        Resolution::Unkeyed => Plan::Scatter(shards.all()),
        Resolution::Params(param_indices) => Plan::Parameterized(Parameterized {
            shard_function: shard_fn.to_owned(),
            param_indices,
            write: false,
        }),
        Resolution::Shards(hit) => read_from_shards(hit),
    }
}

fn plan_insert(s: &InsertStmt, vschema: &VSchema, shards: &ShardCatalog) -> Plan {
    let Some(rv) = s.relation.as_ref() else {
        return reject("INSERT without a target relation");
    };
    let table = range_var_table(rv);
    match placement(vschema, &table) {
        Placement::Global => Plan::Unsharded,
        Placement::Unknown => reject(&format!("table {table} is not in the sharding schema")),
        Placement::Sharded { key, shard_fn } => {
            let Some(values) = extract::insert_key_values(s, key) else {
                return reject(&format!(
                    "INSERT into sharded table {table} must list column {key} with a literal or bind-parameter value"
                ));
            };
            match route_values(&values, shard_fn, shards) {
                Resolution::Unkeyed => reject(&format!(
                    "INSERT into sharded table {table} does not set shard key {key}"
                )),
                Resolution::Params(param_indices) => Plan::Parameterized(Parameterized {
                    shard_function: shard_fn.to_owned(),
                    param_indices,
                    write: true,
                }),
                Resolution::Shards(hit) => write_to_shards(hit, &table),
            }
        }
    }
}

fn plan_update(s: &UpdateStmt, vschema: &VSchema, shards: &ShardCatalog) -> Plan {
    let Some(rv) = s.relation.as_ref() else {
        return reject("UPDATE without a target relation");
    };
    let table = range_var_table(rv);
    match placement(vschema, &table) {
        Placement::Global => Plan::Unsharded,
        Placement::Unknown => reject(&format!("table {table} is not in the sharding schema")),
        Placement::Sharded { key, shard_fn } => {
            if extract::sets_column(&s.target_list, key) {
                return reject(&format!("shard key {key} of {table} is immutable"));
            }
            plan_keyed_write(
                where_key_values(s.where_clause.as_deref(), key),
                shard_fn,
                shards,
                &table,
                key,
                "UPDATE",
            )
        }
    }
}

fn plan_delete(s: &DeleteStmt, vschema: &VSchema, shards: &ShardCatalog) -> Plan {
    let Some(rv) = s.relation.as_ref() else {
        return reject("DELETE without a target relation");
    };
    let table = range_var_table(rv);
    match placement(vschema, &table) {
        Placement::Global => Plan::Unsharded,
        Placement::Unknown => reject(&format!("table {table} is not in the sharding schema")),
        Placement::Sharded { key, shard_fn } => plan_keyed_write(
            where_key_values(s.where_clause.as_deref(), key),
            shard_fn,
            shards,
            &table,
            key,
            "DELETE",
        ),
    }
}

fn plan_keyed_write(
    values: Vec<KeyVal>,
    shard_fn: &str,
    shards: &ShardCatalog,
    table: &TableName,
    key: &str,
    verb: &str,
) -> Plan {
    match route_values(&values, shard_fn, shards) {
        Resolution::Unkeyed => reject(&format!(
            "{verb} of sharded table {table} must constrain shard key {key}"
        )),
        Resolution::Params(param_indices) => Plan::Parameterized(Parameterized {
            shard_function: shard_fn.to_owned(),
            param_indices,
            write: true,
        }),
        Resolution::Shards(hit) => write_to_shards(hit, table),
    }
}

fn write_to_shards(mut hit: Vec<ShardId>, table: &TableName) -> Plan {
    match hit.len() {
        1 => Plan::SingleShard(hit.pop().expect("len checked")),
        _ => reject(&format!(
            "write to sharded table {table} spans {} shards; cross-shard writes are not supported",
            hit.len()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgshard_core::{KeyRange, ScalarValue, SequenceBinding};

    /// Four shards splitting the keyspace at 40/80/c0, each named by its range.
    fn catalog() -> ShardCatalog {
        let shards = KeyRange::FULL
            .split_evenly(4)
            .unwrap()
            .into_iter()
            .map(|r| (r, ShardId::new(r.to_string())))
            .collect();
        ShardCatalog::new(shards).unwrap()
    }

    fn vschema() -> VSchema {
        let sharded = || TableDef::Sharded {
            shard_key_column: "customer_id".into(),
            shard_function: "xxhash64_v1".into(),
            sequences: vec![SequenceBinding {
                column: "id".into(),
                sequence: "orders_id".into(),
            }],
        };
        let mut v = VSchema::default();
        v.insert(TableName::new("public", "orders"), sharded())
            .unwrap();
        v.insert(TableName::new("public", "line_items"), sharded())
            .unwrap();
        v.insert(TableName::new("public", "settings"), TableDef::Global)
            .unwrap();
        v
    }

    fn plan1(sql: &str) -> Plan {
        let parsed = pgshard_sql::parse(sql).unwrap();
        plan_all(&parsed, &vschema(), &catalog())
            .into_iter()
            .next()
            .expect("one statement")
    }

    /// The shard an integer shard key routes to, computed the same way the
    /// planner does — so the assertions never hardcode a hash output.
    fn shard_of(val: i64) -> ShardId {
        let f = shard_function("xxhash64_v1").unwrap();
        catalog()
            .route(f.keyspace_id(&ScalarValue::Int64(val)))
            .clone()
    }

    fn is_reject(p: &Plan) -> bool {
        matches!(
            p,
            Plan::Reject {
                code: CROSS_SHARD,
                ..
            }
        )
    }

    #[test]
    fn single_shard_read_on_literal_key() {
        assert_eq!(
            plan1("SELECT * FROM orders WHERE customer_id = 1"),
            Plan::SingleShard(shard_of(1))
        );
        // The key can appear on either side, buried in an AND chain.
        assert_eq!(
            plan1("SELECT * FROM orders WHERE status = 'new' AND 1 = customer_id"),
            Plan::SingleShard(shard_of(1))
        );
    }

    #[test]
    fn keyless_read_scatters_to_all_shards() {
        assert_eq!(
            plan1("SELECT * FROM orders"),
            Plan::Scatter(catalog().all())
        );
        // A top-level OR is not mined, so the read scatters rather than risk
        // under-routing.
        assert_eq!(
            plan1("SELECT * FROM orders WHERE customer_id = 1 OR customer_id = 2"),
            Plan::Scatter(catalog().all())
        );
    }

    #[test]
    fn in_list_routes_to_the_covered_shards() {
        // 0 and 1 hash to different shards, so the read scatters across exactly
        // those two.
        let Plan::Scatter(shards) = plan1("SELECT * FROM orders WHERE customer_id IN (0, 1)")
        else {
            panic!("expected scatter");
        };
        let mut expected = vec![shard_of(0), shard_of(1)];
        expected.sort();
        expected.dedup();
        assert_eq!(shards, expected);
        assert_ne!(shard_of(0), shard_of(1), "test relies on distinct shards");
    }

    #[test]
    fn in_list_within_one_shard_is_single_shard() {
        assert_eq!(
            plan1("SELECT * FROM orders WHERE customer_id IN (7, 7)"),
            Plan::SingleShard(shard_of(7))
        );
    }

    #[test]
    fn parameterized_key_defers_to_bind() {
        assert_eq!(
            plan1("SELECT * FROM orders WHERE customer_id = $1"),
            Plan::Parameterized(Parameterized {
                shard_function: "xxhash64_v1".into(),
                param_indices: vec![1],
                write: false,
            })
        );
    }

    #[test]
    fn global_and_tableless_reads() {
        assert_eq!(plan1("SELECT * FROM settings"), Plan::Unsharded);
        assert_eq!(plan1("SELECT 1"), Plan::RouterLocal);
        // A relation the schema does not know goes to the system database.
        assert_eq!(plan1("SELECT * FROM pg_catalog.pg_class"), Plan::Unsharded);
    }

    #[test]
    fn joins_are_rejected_not_misrouted() {
        // Two sharded tables: rejected (co-located-join routing is out of M1
        // scope; routing by one table's key could under-route the other).
        assert!(is_reject(&plan1(
            "SELECT * FROM orders o JOIN line_items l ON l.order_id = o.id \
             WHERE o.customer_id = 5 AND l.customer_id = 5"
        )));
        // Sharded joined to a global table: also rejected (no reference tables
        // in M1).
        assert!(is_reject(&plan1(
            "SELECT * FROM orders o JOIN settings s ON s.k = o.status \
             WHERE o.customer_id = 5"
        )));
    }

    #[test]
    fn single_table_qualified_key_still_routes() {
        // A table alias qualifier on the key resolves for the single-table case.
        assert_eq!(
            plan1("SELECT * FROM orders o WHERE o.customer_id = 9"),
            Plan::SingleShard(shard_of(9))
        );
    }

    #[test]
    fn insert_routes_by_values() {
        assert_eq!(
            plan1("INSERT INTO orders (customer_id, total) VALUES (1, 100)"),
            Plan::SingleShard(shard_of(1))
        );
        // Multi-row insert within one shard is fine.
        assert_eq!(
            plan1("INSERT INTO orders (customer_id) VALUES (7), (7)"),
            Plan::SingleShard(shard_of(7))
        );
        // Parameterized value.
        assert_eq!(
            plan1("INSERT INTO orders (customer_id) VALUES ($1)"),
            Plan::Parameterized(Parameterized {
                shard_function: "xxhash64_v1".into(),
                param_indices: vec![1],
                write: true,
            })
        );
    }

    #[test]
    fn insert_that_cannot_be_routed_is_rejected() {
        // Multi-row spanning shards.
        assert!(is_reject(&plan1(
            "INSERT INTO orders (customer_id) VALUES (0), (1)"
        )));
        // No column list (positional): column order is unknown.
        assert!(is_reject(&plan1("INSERT INTO orders VALUES (1, 100)")));
        // INSERT ... SELECT: values are not literals here.
        assert!(is_reject(&plan1(
            "INSERT INTO orders (customer_id) SELECT id FROM other"
        )));
        // Column list without the shard key.
        assert!(is_reject(&plan1("INSERT INTO orders (total) VALUES (100)")));
        // DEFAULT in the shard-key position.
        assert!(is_reject(&plan1(
            "INSERT INTO orders (customer_id) VALUES (DEFAULT)"
        )));
    }

    #[test]
    fn insert_into_global_table_is_unsharded() {
        assert_eq!(
            plan1("INSERT INTO settings (k, v) VALUES ('x', 'y')"),
            Plan::Unsharded
        );
    }

    #[test]
    fn update_and_delete_route_by_key() {
        assert_eq!(
            plan1("UPDATE orders SET total = 1 WHERE customer_id = 1"),
            Plan::SingleShard(shard_of(1))
        );
        assert_eq!(
            plan1("DELETE FROM orders WHERE customer_id = 1"),
            Plan::SingleShard(shard_of(1))
        );
    }

    #[test]
    fn keyless_writes_are_rejected() {
        assert!(is_reject(&plan1("UPDATE orders SET total = 1")));
        assert!(is_reject(&plan1("DELETE FROM orders")));
        assert!(is_reject(&plan1(
            "DELETE FROM orders WHERE customer_id IN (0, 1)"
        )));
    }

    #[test]
    fn updating_the_shard_key_is_rejected() {
        let p = plan1("UPDATE orders SET customer_id = 2 WHERE customer_id = 1");
        assert!(matches!(&p, Plan::Reject { reason, .. } if reason.contains("immutable")));
    }

    #[test]
    fn unknown_table_write_is_rejected() {
        let p = plan1("INSERT INTO widgets (id) VALUES (1)");
        assert!(matches!(&p, Plan::Reject { reason, .. } if reason.contains("sharding schema")));
    }

    #[test]
    fn ddl_broadcasts_and_merge_copy_are_rejected() {
        assert_eq!(
            plan1("CREATE TABLE t (id int)"),
            Plan::Broadcast(catalog().all())
        );
        assert_eq!(
            plan1("ALTER TABLE orders ADD COLUMN note text"),
            Plan::Broadcast(catalog().all())
        );
        assert!(is_reject(&plan1(
            "MERGE INTO orders o USING s ON o.id = s.id WHEN MATCHED THEN DO NOTHING"
        )));
        assert!(is_reject(&plan1("COPY orders FROM STDIN")));
    }

    #[test]
    fn session_statements_are_router_local() {
        for sql in [
            "SET search_path = app",
            "SHOW server_version",
            "BEGIN",
            "COMMIT",
        ] {
            assert_eq!(plan1(sql), Plan::RouterLocal, "{sql}");
        }
    }
}
