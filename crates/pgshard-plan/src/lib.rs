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
//!   `Int64`, string literals as `Text`. The vschema does not yet carry the
//!   shard-key column's type, so a quoted literal against an integer column
//!   (`customer_id = '1'`) hashes as text and can route differently from the
//!   `Int64` the row was stored under. Typed coercion (and uuid/bytea hashing)
//!   is a follow-up; until then the operator must use literals in the column's
//!   native form.
//! - A `$n` shard key yields a [`Plan::Parameterized`]: the value is known only
//!   at Bind, so bind-time resolution is left to the executor. A predicate that
//!   mixes a literal and a parameter (`key IN (1, $1)`) is not parameterizable
//!   soundly — the read scatters and the write is rejected.
//! - Cross-shard writes, keyless writes, updates of the shard key (including via
//!   `ON CONFLICT DO UPDATE`), `UPDATE ... FROM` / `DELETE ... USING`, and
//!   MERGE/COPY are rejected with SQLSTATE `0A000`.
//! - `search_path` is not modeled: an unqualified relation defaults to `public`.
//! - Joins and subqueries are rejected, not routed: key-routing needs a single
//!   plain sharded table in the FROM and no subquery in the WHERE. This keeps
//!   the qualifier-blind extractor sound (only one table is ever in scope) and
//!   never under-routes a query whose other relations live on other shards.

pub mod catalog;
mod extract;

use std::collections::BTreeSet;

use pg_query::NodeEnum;
use pg_query::protobuf::{DeleteStmt, InsertStmt, OnConflictAction, SelectStmt, UpdateStmt};

use pgshard_core::{ScalarValue, TableDef, TableName, VSchema, shard_function};
use pgshard_sql::{Parsed, StatementKind};

pub use catalog::{ShardCatalog, ShardId};
use extract::{KeyVal, analyze_from, contains_sublink, range_var_table, where_key_values};

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

/// Complete a [`Parameterized`] plan once its bind parameters are known.
///
/// `values` are the statement's bound parameters, 0-indexed (`$1` = `values[0]`),
/// already coerced to the column's type by the wire layer. Each shard-key
/// parameter is hashed with the plan's shard function and routed through
/// `shards` — the same decision the planner makes for literals: one shard →
/// [`Plan::SingleShard`]; a read spanning several → [`Plan::Scatter`]; a write
/// spanning several → rejected as cross-shard.
pub fn resolve_bound(param: &Parameterized, values: &[ScalarValue], shards: &ShardCatalog) -> Plan {
    let func = shard_function(&param.shard_function).expect("vschema validates the shard function");
    let mut hit = BTreeSet::new();
    for &idx in &param.param_indices {
        let Some(value) = values.get((idx as usize).wrapping_sub(1)) else {
            return reject(&format!(
                "bind parameter ${idx} for the shard key is missing"
            ));
        };
        hit.insert(shards.route(func.keyspace_id(value)).clone());
    }
    let hit: Vec<ShardId> = hit.into_iter().collect();
    match (param.write, hit.len()) {
        // A Parameterized plan always carries at least one shard-key param, so
        // `hit` is never empty; guard defensively all the same.
        (_, 0) => reject("parameterized plan named no shard-key parameters"),
        (_, 1) => Plan::SingleShard(hit.into_iter().next().expect("len 1")),
        (false, _) => Plan::Scatter(hit),
        (true, n) => reject(&format!(
            "write resolves to {n} shards; cross-shard writes are not supported"
        )),
    }
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
    /// Every value is a bind parameter; carries each `$n` seen.
    Params(Vec<u32>),
    /// A mix of literal and bind-parameter values. The literal shards cannot be
    /// dropped (they may differ from where the params land), and bind-time
    /// resolution sees only the params, so neither `Shards` nor `Params` is
    /// sound: the caller must scatter (read) or reject (write).
    Mixed,
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
    let has_const = values.iter().any(|v| matches!(v, KeyVal::Const(_)));
    match (params.is_empty(), has_const) {
        (false, true) => return Resolution::Mixed,
        (false, false) => return Resolution::Params(params),
        _ => {}
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
    let from = analyze_from(&s.from_clause);
    let sharded: Vec<(&str, &str)> = from
        .tables
        .iter()
        .filter_map(|t| match placement(vschema, t) {
            Placement::Sharded { key, shard_fn } => Some((key, shard_fn)),
            _ => None,
        })
        .collect();

    if sharded.is_empty() {
        // No sharded table in a plain FROM. If every FROM item is a plain
        // relation, this is a global/system read (or a tableless `SELECT 1`);
        // a subquery/function in the FROM could hide a sharded table, so route
        // it to the system database rather than the shards either way.
        return if from.tables.is_empty() && from.all_plain {
            Plan::RouterLocal
        } else {
            Plan::Unsharded
        };
    }

    // Only a single plain sharded table can be key-routed. A join (more FROM
    // items, or a subquery/function that hides relations) needs per-alias
    // qualifier resolution the extractor does not do and cross-shard join
    // semantics that are out of M1 scope — reject rather than risk
    // under-routing.
    let [(key, shard_fn)] = sharded[..] else {
        return reject("SELECT joining multiple sharded tables is not supported in M1");
    };
    if from.tables.len() > 1 || !from.all_plain {
        return reject(
            "SELECT joining a sharded table with another relation is not supported in M1",
        );
    }
    // A subquery in the WHERE would run only on the routed shard yet could
    // reference rows on others; refuse the shard-key fast path.
    if contains_sublink(s.where_clause.as_deref()) {
        return reject("SELECT with a subquery on a sharded table is not supported in M1");
    }

    let values = where_key_values(s.where_clause.as_deref(), key);
    match route_values(&values, shard_fn, shards) {
        Resolution::Unkeyed | Resolution::Mixed => Plan::Scatter(shards.all()),
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
            // `ON CONFLICT ... DO UPDATE` runs only on the shard the INSERT
            // routes to, but the conflicting row (found by an arbitrary arbiter)
            // may live on another shard, and the SET may even move the shard key.
            // Neither is safe on a single shard, so reject any DO UPDATE (DO
            // NOTHING is fine).
            if let Some(oc) = s.on_conflict_clause.as_deref()
                && oc.action == OnConflictAction::OnconflictUpdate as i32
            {
                return reject(&format!(
                    "INSERT into sharded table {table} with ON CONFLICT DO UPDATE is not supported in M1"
                ));
            }
            let Some(values) = extract::insert_key_values(s, key) else {
                return reject(&format!(
                    "INSERT into sharded table {table} must list column {key} with a literal or bind-parameter value"
                ));
            };
            match route_values(&values, shard_fn, shards) {
                Resolution::Unkeyed => reject(&format!(
                    "INSERT into sharded table {table} does not set shard key {key}"
                )),
                Resolution::Mixed => reject(&format!(
                    "INSERT into sharded table {table} mixes literal and parameter shard keys across rows"
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
            // `UPDATE ... FROM other` joins another relation whose rows may live
            // on other shards; running it on the target's shard alone matches the
            // wrong set. A WHERE subquery has the same problem.
            if !s.from_clause.is_empty() {
                return reject(&format!(
                    "UPDATE {table} ... FROM another relation is not supported in M1"
                ));
            }
            if contains_sublink(s.where_clause.as_deref()) {
                return reject(&format!(
                    "UPDATE {table} with a subquery is not supported in M1"
                ));
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
        Placement::Sharded { key, shard_fn } => {
            // `DELETE ... USING other` and a WHERE subquery both bring in rows
            // that may live on other shards; neither is routable on one shard.
            if !s.using_clause.is_empty() {
                return reject(&format!(
                    "DELETE FROM {table} ... USING another relation is not supported in M1"
                ));
            }
            if contains_sublink(s.where_clause.as_deref()) {
                return reject(&format!(
                    "DELETE FROM {table} with a subquery is not supported in M1"
                ));
            }
            plan_keyed_write(
                where_key_values(s.where_clause.as_deref(), key),
                shard_fn,
                shards,
                &table,
                key,
                "DELETE",
            )
        }
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
        Resolution::Mixed => reject(&format!(
            "{verb} of sharded table {table} mixes literal and parameter shard keys"
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

    fn param(indices: Vec<u32>, write: bool) -> Parameterized {
        Parameterized {
            shard_function: "xxhash64_v1".into(),
            param_indices: indices,
            write,
        }
    }

    #[test]
    fn resolve_bound_completes_a_parameterized_plan() {
        let cat = catalog();
        // A single shard-key param routes to its one shard, read or write.
        assert_eq!(
            resolve_bound(&param(vec![1], true), &[ScalarValue::Int64(1)], &cat),
            Plan::SingleShard(shard_of(1))
        );
        // The value is hashed by its bound type — a text key routes by text.
        assert_eq!(
            resolve_bound(
                &param(vec![1], false),
                &[ScalarValue::Text("hello".into())],
                &cat
            ),
            {
                let f = shard_function("xxhash64_v1").unwrap();
                Plan::SingleShard(
                    cat.route(f.keyspace_id(&ScalarValue::Text("hello".into())))
                        .clone(),
                )
            }
        );
    }

    #[test]
    fn resolve_bound_reads_scatter_writes_reject_across_shards() {
        let cat = catalog();
        // $1=0 and $2=1 hash to different shards.
        assert_ne!(shard_of(0), shard_of(1));
        let vals = [ScalarValue::Int64(0), ScalarValue::Int64(1)];
        let mut expected = vec![shard_of(0), shard_of(1)];
        expected.sort();
        assert_eq!(
            resolve_bound(&param(vec![1, 2], false), &vals, &cat),
            Plan::Scatter(expected)
        );
        // The same spread as a write is a cross-shard rejection.
        assert!(matches!(
            resolve_bound(&param(vec![1, 2], true), &vals, &cat),
            Plan::Reject {
                code: CROSS_SHARD,
                ..
            }
        ));
    }

    #[test]
    fn resolve_bound_rejects_a_missing_parameter() {
        // $2 referenced but only one value bound.
        assert!(matches!(
            resolve_bound(&param(vec![2], false), &[ScalarValue::Int64(1)], &catalog()),
            Plan::Reject { .. }
        ));
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
    fn insert_on_conflict_do_update_is_rejected_but_do_nothing_routes() {
        // DO NOTHING only affects the routed shard, so it still routes.
        assert_eq!(
            plan1(
                "INSERT INTO orders (customer_id, total) VALUES (1, 5) \
                 ON CONFLICT (customer_id) DO NOTHING"
            ),
            Plan::SingleShard(shard_of(1))
        );
        // DO UPDATE is rejected wholesale: the conflicting row may live on
        // another shard and the SET may move the shard key.
        for sql in [
            "INSERT INTO orders (customer_id) VALUES (1) ON CONFLICT (customer_id) DO UPDATE SET total = 9",
            "INSERT INTO orders (id, customer_id) VALUES (7, 1) ON CONFLICT (id) DO UPDATE SET customer_id = 2",
        ] {
            assert!(is_reject(&plan1(sql)), "{sql}");
        }
    }

    #[test]
    fn mixed_literal_and_parameter_keys_never_single_shard() {
        // A read scatters (sound superset); the write is rejected.
        assert_eq!(
            plan1("SELECT * FROM orders WHERE customer_id IN (1, $1)"),
            Plan::Scatter(catalog().all())
        );
        assert!(is_reject(&plan1(
            "DELETE FROM orders WHERE customer_id IN (1, $1)"
        )));
        assert!(is_reject(&plan1(
            "INSERT INTO orders (customer_id) VALUES (1), ($1)"
        )));
    }

    #[test]
    fn join_writes_and_subqueries_are_rejected() {
        // UPDATE ... FROM / DELETE ... USING join another (sharded) relation.
        assert!(is_reject(&plan1(
            "UPDATE orders SET total = 1 FROM line_items \
             WHERE orders.customer_id = 5 AND line_items.id = orders.id"
        )));
        assert!(is_reject(&plan1(
            "DELETE FROM orders USING line_items \
             WHERE orders.customer_id = 5 AND line_items.id = orders.id"
        )));
        // A subquery in the FROM hides a relation from the join guard.
        assert!(is_reject(&plan1(
            "SELECT * FROM orders o, (SELECT * FROM line_items) sub WHERE o.customer_id = 1"
        )));
        // A subquery in the WHERE would run only on the routed shard.
        assert!(is_reject(&plan1(
            "SELECT * FROM orders WHERE customer_id = 5 \
             AND id IN (SELECT order_id FROM line_items WHERE customer_id = 9)"
        )));
        assert!(is_reject(&plan1(
            "DELETE FROM orders WHERE customer_id = 5 \
             AND EXISTS (SELECT 1 FROM line_items WHERE line_items.id = orders.id)"
        )));
    }

    #[test]
    fn a_plain_in_list_is_not_a_subquery() {
        // `IN (values)` must not be mistaken for `IN (subquery)`.
        assert_eq!(
            plan1("SELECT * FROM orders WHERE customer_id = 5 AND status IN ('a', 'b')"),
            Plan::SingleShard(shard_of(5))
        );
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
