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
//! - Literal shard keys are coerced to the column's declared type before hashing
//!   ([`ScalarType::coerce`]), so different spellings of one value route
//!   identically — `customer_id = '1'` and `customer_id = 1` hit the same shard
//!   for an integer key. A literal that is not a valid value of the type (PG
//!   would reject it) is treated as unroutable: the read scatters and the write
//!   is rejected. When the topology does not declare the column's type, the
//!   literal is hashed in the form it was written (the pre-typing behavior).
//! - A `$n` shard key yields a [`Plan::Parameterized`]: the value is known only
//!   at Bind, so bind-time resolution is left to the executor. A predicate that
//!   mixes a literal and a parameter (`key IN (1, $1)`) is not parameterizable
//!   soundly — the read scatters and the write is rejected.
//! - Cross-shard writes, keyless writes, updates of the shard key (including via
//!   `ON CONFLICT DO UPDATE`), `UPDATE ... FROM` / `DELETE ... USING`, and
//!   MERGE/COPY are rejected with SQLSTATE `0A000`.
//! - `search_path` is not modeled: an unqualified relation defaults to `public`.
//! - Joins, subqueries, CTEs, and set operations over sharded tables are
//!   rejected, not routed: key-routing needs a single plain sharded table in the
//!   FROM, no subquery anywhere in the statement, and no `WITH` clause (which can
//!   shadow the table name or hide a cross-shard write). A `UNION`/`INTERSECT`/
//!   `EXCEPT` touching a sharded table needs a cross-shard merge and is rejected;
//!   one over only global tables runs on the system database. This keeps the
//!   qualifier-blind extractor sound (only one table is ever in scope) and never
//!   under-routes a query whose other relations live on other shards.
//! - A function body is opaque to the router. `SELECT f()`, where `f` runs
//!   `SELECT count(*) FROM sharded`, carries no AST-visible table reference, so
//!   it is treated as session-local and runs on one shard — a partial result.
//!   Functions that read sharded tables are unsupported in M1 (no parser can see
//!   through a function body without resolving it against the catalog).

pub mod catalog;
mod extract;
pub mod sequence;

use std::collections::BTreeSet;

use pg_query::NodeEnum;
use pg_query::protobuf::{
    DeleteStmt, InsertStmt, OnConflictAction, SelectStmt, SetOperation, UpdateStmt,
};

use pgshard_core::{ScalarType, ScalarValue, TableDef, TableName, VSchema, shard_function};
use pgshard_sql::{Parsed, StatementKind};

pub use catalog::{ShardCatalog, ShardId};
use extract::{
    KeyVal, analyze_from, contains_cte, contains_sublink, range_var_table, where_key_values,
};

/// SQLSTATE `feature_not_supported`: what a statement the router cannot route
/// (a cross-shard write, an unsupported form) is rejected with.
const CROSS_SHARD: &str = "0A000";
const UNDEFINED_TABLE: &str = "42P01";

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
    /// The shard-key column's type, so a bound value is coerced the same way a
    /// literal is before hashing (`resolve_bound`). `None` hashes the value as
    /// delivered — matching the untyped literal path.
    pub key_type: Option<ScalarType>,
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
/// `values` are the statement's bound parameters, 0-indexed (`$1` = `values[0]`).
/// Each shard-key parameter is coerced to the plan's [`Parameterized::key_type`]
/// and hashed with the plan's shard function — the same decision the planner
/// makes for literals, so a bound `"1"` and a bound `1` route alike for an
/// integer key. A value that is not valid for the type is rejected rather than
/// routed to a guessed shard. One shard → [`Plan::SingleShard`]; a read spanning
/// several → [`Plan::Scatter`]; a write spanning several → rejected as
/// cross-shard.
pub fn resolve_bound(param: &Parameterized, values: &[ScalarValue], shards: &ShardCatalog) -> Plan {
    // Planner-built plans always name a vschema-validated function, but this is a
    // public entry point with public fields — reject an unknown one, never panic.
    let Ok(func) = shard_function(&param.shard_function) else {
        return reject(&format!(
            "unknown shard function {:?}",
            param.shard_function
        ));
    };
    let mut hit = BTreeSet::new();
    for &idx in &param.param_indices {
        let Some(value) = values.get((idx as usize).wrapping_sub(1)) else {
            return reject(&format!(
                "bind parameter ${idx} for the shard key is missing"
            ));
        };
        let id = match param.key_type {
            Some(t) => match t.coerce(value) {
                Some(canonical) => func.keyspace_id(&canonical),
                None => {
                    return reject(&format!(
                        "bind parameter ${idx} is not a valid value for the shard key type"
                    ));
                }
            },
            None => func.keyspace_id(value),
        };
        hit.insert(shards.route(id).clone());
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

/// System catalog schemas exist in every database and hold no user rows, so a
/// read over them routes like a global table rather than being rejected as
/// unknown. Only the qualified forms are exempt: an unqualified `pg_class`
/// parses into `public` (v1 does not model `search_path`) and is treated like
/// any other unknown relation.
fn is_catalog_schema(t: &TableName) -> bool {
    t.schema == "pg_catalog" || t.schema == "information_schema"
}

/// A relation the sharding schema does not know cannot be routed by guesswork:
/// sending it to the system database would return that database's same-named
/// table if one exists — plausible wrong rows instead of an error.
fn reject_unknown(t: &TableName) -> Plan {
    Plan::Reject {
        code: UNDEFINED_TABLE,
        reason: format!(
            "relation {t} is not in the sharding configuration; qualify catalog reads with pg_catalog"
        ),
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
    /// A literal shard key that is not a valid value of the column's declared
    /// type (PostgreSQL would reject it), so it cannot be hashed. Handled like
    /// `Mixed`: the read scatters (every shard returns PG's type error) and the
    /// write is rejected rather than routed to a guessed shard.
    Uncoercible,
    /// All values are literals; carries the distinct shards they hit.
    Shards(Vec<ShardId>),
}

fn route_values(
    values: &[KeyVal],
    key_type: Option<ScalarType>,
    shard_fn: &str,
    shards: &ShardCatalog,
) -> Resolution {
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
            // Untyped columns hash the literal as written; typed columns coerce
            // first, and a value invalid for the type is unroutable (see
            // `ScalarType::coerce`).
            let id = match key_type {
                Some(t) => match t.coerce(sv) {
                    Some(canonical) => func.keyspace_id(&canonical),
                    None => return Resolution::Uncoercible,
                },
                None => func.keyspace_id(sv),
            };
            hit.insert(shards.route(id).clone());
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
    Sharded {
        key: &'a str,
        key_type: Option<ScalarType>,
        shard_fn: &'a str,
    },
    Global,
    Unknown,
}

fn placement<'a>(vschema: &'a VSchema, table: &TableName) -> Placement<'a> {
    match vschema.get(table) {
        Some(TableDef::Sharded {
            shard_key_column,
            shard_key_type,
            shard_function,
            ..
        }) => Placement::Sharded {
            key: shard_key_column,
            key_type: *shard_key_type,
            shard_fn: shard_function,
        },
        Some(TableDef::Global) => Placement::Global,
        None => Placement::Unknown,
    }
}

fn plan_select(s: &SelectStmt, vschema: &VSchema, shards: &ShardCatalog) -> Plan {
    // A set operation (UNION/INTERSECT/EXCEPT) nests its operands in larg/rarg and
    // leaves the top-level FROM empty, so the key-routing path below would see no
    // tables and mistake it for a session-local read — running the whole set
    // operation on a single shard and dropping the other shards' rows. Handle it
    // on its own.
    if s.op != SetOperation::SetopNone as i32 {
        return plan_set_op(s, vschema);
    }
    let from = analyze_from(&s.from_clause);
    // Fail loud on a relation the schema does not know (a typo, or a table
    // missing from the config): falling through to the system database could
    // return a same-named table's rows there instead of an error.
    if let Some(unknown) = from
        .tables
        .iter()
        .find(|t| !is_catalog_schema(t) && matches!(placement(vschema, t), Placement::Unknown))
    {
        return reject_unknown(unknown);
    }
    let sharded: Vec<(&str, Option<ScalarType>, &str)> = from
        .tables
        .iter()
        .filter_map(|t| match placement(vschema, t) {
            Placement::Sharded {
                key,
                key_type,
                shard_fn,
            } => Some((key, key_type, shard_fn)),
            _ => None,
        })
        .collect();

    if sharded.is_empty() {
        // No sharded table in a plain FROM. A tableless statement with no
        // subquery is session-local (`SELECT 1`, `SELECT current_setting(...)`).
        // But a subquery or CTE — or a subquery/function in the FROM — could still
        // reach a sharded table (`SELECT (SELECT count(*) FROM sharded)`); running
        // that on one arbitrary shard would silently return partial data. Route
        // anything but a plain tableless statement to the system database, where a
        // sharded reference errors loudly rather than answering wrong.
        let session_local =
            from.tables.is_empty() && from.all_plain && !contains_sublink(s) && !contains_cte(s);
        return if session_local {
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
    let [(key, key_type, shard_fn)] = sharded[..] else {
        return reject("SELECT joining multiple sharded tables is not supported in M1");
    };
    if from.tables.len() > 1 || !from.all_plain {
        return reject(
            "SELECT joining a sharded table with another relation is not supported in M1",
        );
    }
    // A `WITH` clause can *shadow* the sharded table name — `WITH orders AS
    // (SELECT ... FROM elsewhere) SELECT * FROM orders WHERE key = 5` routes by
    // the base table's key yet reads the CTE, whose query can touch other shards
    // — and a data-modifying CTE runs its writes on the routed shard regardless.
    // The extractor cannot see through a CTE, so refuse the fast path.
    if s.with_clause.is_some() {
        return reject("SELECT with a CTE (WITH) on a sharded table is not supported in M1");
    }
    // A subquery anywhere in the statement — a WHERE predicate, a target-list
    // expression, an aggregate FILTER — would run only on the routed shard yet
    // could reference rows on others; refuse the shard-key fast path.
    if contains_sublink(s) {
        return reject("SELECT with a subquery on a sharded table is not supported in M1");
    }

    let values = where_key_values(s.where_clause.as_deref(), key);
    match route_values(&values, key_type, shard_fn, shards) {
        // An uncoercible literal matches no row of the column's type; scattering
        // lets PostgreSQL return its own type error rather than guess a shard.
        Resolution::Unkeyed | Resolution::Mixed | Resolution::Uncoercible => {
            Plan::Scatter(shards.all())
        }
        Resolution::Params(param_indices) => Plan::Parameterized(Parameterized {
            shard_function: shard_fn.to_owned(),
            key_type,
            param_indices,
            write: false,
        }),
        Resolution::Shards(hit) => read_from_shards(hit),
    }
}

/// Plan a set operation (`UNION`/`INTERSECT`/`EXCEPT`). Its arms may target
/// different shards and its result needs a cross-shard merge, so M1 cannot route
/// one that touches a sharded table. A set operation purely over global tables
/// runs whole on the system database; a tableless one (`SELECT 1 UNION SELECT 2`)
/// is session-local.
fn plan_set_op(s: &SelectStmt, vschema: &VSchema) -> Plan {
    // set_op_from only sees each arm's leaf FROM, so a sharded table hidden in an
    // arm's expression subquery (`SELECT (SELECT .. FROM sharded) UNION ..`) or a
    // CTE body (`WITH x AS (SELECT .. FROM sharded) SELECT .. FROM x UNION ..`)
    // would slip past it and mis-route. Reject any subquery or CTE anywhere in the
    // set operation — the same shapes a single keyed statement rejects.
    if contains_sublink(s) {
        return reject("a set operation with a subquery is not supported in M1");
    }
    if contains_cte(s) {
        return reject("a set operation with a CTE (WITH) is not supported in M1");
    }
    let from = set_op_from(s);
    // A function in any arm's FROM could hide a sharded table; refuse to route
    // past what we cannot see (the same stance as a single SELECT).
    if !from.all_plain {
        return reject(
            "a set operation with a subquery or function in a FROM is not supported in M1",
        );
    }
    if from
        .tables
        .iter()
        .any(|t| matches!(placement(vschema, t), Placement::Sharded { .. }))
    {
        return reject(
            "a set operation (UNION/INTERSECT/EXCEPT) over a sharded table is not supported in M1",
        );
    }
    // Same unknown-relation stance as a plain SELECT: never route a relation
    // the schema does not know to the system database by default.
    if let Some(unknown) = from
        .tables
        .iter()
        .find(|t| !is_catalog_schema(t) && matches!(placement(vschema, t), Placement::Unknown))
    {
        return reject_unknown(unknown);
    }
    if from.tables.is_empty() {
        Plan::RouterLocal
    } else {
        Plan::Unsharded
    }
}

/// The FROM relations of every arm of a (possibly nested) set operation, and
/// whether every arm's FROM is plain. A set operation nests its operands in
/// `larg`/`rarg`; only the leaf arms carry a FROM clause.
fn set_op_from(s: &SelectStmt) -> extract::FromClause {
    if s.op == SetOperation::SetopNone as i32 {
        return analyze_from(&s.from_clause);
    }
    let mut merged = extract::FromClause {
        tables: Vec::new(),
        all_plain: true,
    };
    for arm in [s.larg.as_deref(), s.rarg.as_deref()].into_iter().flatten() {
        let arm = set_op_from(arm);
        merged.tables.extend(arm.tables);
        merged.all_plain &= arm.all_plain;
    }
    merged
}

fn plan_insert(s: &InsertStmt, vschema: &VSchema, shards: &ShardCatalog) -> Plan {
    let Some(rv) = s.relation.as_ref() else {
        return reject("INSERT without a target relation");
    };
    let table = range_var_table(rv);
    // A CTE (`WITH`, possibly data-modifying) or a subquery anywhere in the
    // statement (a VALUES cell, RETURNING, ON CONFLICT) can read or write rows on
    // other shards, but would run only on the shard this INSERT routes to. Reject
    // before routing rather than execute against the wrong shard — otherwise,
    // once a sequence fills the omitted id, the statement would silently succeed
    // with wrong data. (A plain `INSERT ... SELECT` is not a SubLink; the sharded
    // path rejects it separately for lacking literal shard-key values.)
    if s.with_clause.is_some() {
        return reject(&format!(
            "INSERT into {table} with a CTE (WITH) is not supported in M1"
        ));
    }
    if contains_sublink(s) {
        return reject(&format!(
            "INSERT into {table} with a subquery is not supported in M1"
        ));
    }
    match placement(vschema, &table) {
        Placement::Global => Plan::Unsharded,
        Placement::Unknown => reject(&format!("table {table} is not in the sharding schema")),
        Placement::Sharded {
            key,
            key_type,
            shard_fn,
        } => {
            // `ON CONFLICT ... DO UPDATE` runs only on the shard the INSERT
            // routes to, but the conflicting row (found by an arbitrary arbiter)
            // may live on another shard, and the SET may even move the shard key.
            // Neither is safe on a single shard, so reject any DO UPDATE. DO
            // NOTHING is only shard-local when its conflict target provably
            // includes the shard key: conflicting rows then share the key value
            // and live on the routed shard. A bare DO NOTHING (matches any
            // constraint), an ON CONSTRAINT form (columns unknown here), or an
            // arbiter without the key could match a unique constraint spanning
            // shards — a conflict on another shard is invisible to this routed
            // insert, which would then create a logical duplicate.
            if let Some(oc) = s.on_conflict_clause.as_deref() {
                if oc.action == OnConflictAction::OnconflictUpdate as i32 {
                    return reject(&format!(
                        "INSERT into sharded table {table} with ON CONFLICT DO UPDATE is not supported in M1"
                    ));
                }
                let arbiter_includes_key = oc.infer.as_deref().is_some_and(|inf| {
                    inf.conname.is_empty()
                        && inf.index_elems.iter().any(
                            |e| matches!(&e.node, Some(NodeEnum::IndexElem(el)) if el.name == key),
                        )
                });
                if !arbiter_includes_key {
                    return reject(&format!(
                        "INSERT into sharded table {table} with ON CONFLICT requires a conflict target listing the shard key column {key}"
                    ));
                }
            }
            let Some(values) = extract::insert_key_values(s, key) else {
                return reject(&format!(
                    "INSERT into sharded table {table} must list column {key} with a literal or bind-parameter value"
                ));
            };
            match route_values(&values, key_type, shard_fn, shards) {
                Resolution::Unkeyed => reject(&format!(
                    "INSERT into sharded table {table} does not set shard key {key}"
                )),
                Resolution::Mixed => reject(&format!(
                    "INSERT into sharded table {table} mixes literal and parameter shard keys across rows"
                )),
                Resolution::Uncoercible => reject(&format!(
                    "INSERT into sharded table {table} has a shard key {key} value that is not valid for the column type"
                )),
                Resolution::Params(param_indices) => Plan::Parameterized(Parameterized {
                    shard_function: shard_fn.to_owned(),
                    key_type,
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
        Placement::Sharded {
            key,
            key_type,
            shard_fn,
        } => {
            if extract::sets_column(&s.target_list, key) {
                return reject(&format!("shard key {key} of {table} is immutable"));
            }
            // `UPDATE ... FROM other` joins another relation whose rows may live
            // on other shards; running it on the target's shard alone matches the
            // wrong set. A subquery anywhere (a WHERE predicate or a SET value)
            // has the same problem.
            if !s.from_clause.is_empty() {
                return reject(&format!(
                    "UPDATE {table} ... FROM another relation is not supported in M1"
                ));
            }
            // A data-modifying CTE (`WITH d AS (DELETE FROM other ...) UPDATE ...`)
            // is not a SubLink, so it slips past the subquery check, yet its
            // writes execute against the routed shard rather than the rows'
            // real shards. Reject any CTE, as INSERT does.
            if s.with_clause.is_some() {
                return reject(&format!(
                    "UPDATE {table} with a CTE (WITH) is not supported in M1"
                ));
            }
            if contains_sublink(s) {
                return reject(&format!(
                    "UPDATE {table} with a subquery is not supported in M1"
                ));
            }
            plan_keyed_write(
                where_key_values(s.where_clause.as_deref(), key),
                key_type,
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
        Placement::Sharded {
            key,
            key_type,
            shard_fn,
        } => {
            // `DELETE ... USING other` and a subquery (in the WHERE or RETURNING)
            // both bring in rows that may live on other shards; neither is
            // routable on one shard.
            if !s.using_clause.is_empty() {
                return reject(&format!(
                    "DELETE FROM {table} ... USING another relation is not supported in M1"
                ));
            }
            // A data-modifying CTE is not a SubLink but still executes its writes
            // on the routed shard rather than the rows' real shards. Reject any
            // CTE, as INSERT does.
            if s.with_clause.is_some() {
                return reject(&format!(
                    "DELETE FROM {table} with a CTE (WITH) is not supported in M1"
                ));
            }
            if contains_sublink(s) {
                return reject(&format!(
                    "DELETE FROM {table} with a subquery is not supported in M1"
                ));
            }
            plan_keyed_write(
                where_key_values(s.where_clause.as_deref(), key),
                key_type,
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
    key_type: Option<ScalarType>,
    shard_fn: &str,
    shards: &ShardCatalog,
    table: &TableName,
    key: &str,
    verb: &str,
) -> Plan {
    match route_values(&values, key_type, shard_fn, shards) {
        Resolution::Unkeyed => reject(&format!(
            "{verb} of sharded table {table} must constrain shard key {key}"
        )),
        Resolution::Mixed => reject(&format!(
            "{verb} of sharded table {table} mixes literal and parameter shard keys"
        )),
        Resolution::Uncoercible => reject(&format!(
            "{verb} of sharded table {table} has a shard key {key} value that is not valid for the column type"
        )),
        Resolution::Params(param_indices) => Plan::Parameterized(Parameterized {
            shard_function: shard_fn.to_owned(),
            key_type,
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
        vschema_with_key_type(Some(ScalarType::Int))
    }

    fn vschema_with_key_type(key_type: Option<ScalarType>) -> VSchema {
        let sharded = || TableDef::Sharded {
            shard_key_column: "customer_id".into(),
            shard_key_type: key_type,
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

    fn plan1_with(sql: &str, key_type: Option<ScalarType>) -> Plan {
        let parsed = pgshard_sql::parse(sql).unwrap();
        plan_all(&parsed, &vschema_with_key_type(key_type), &catalog())
            .into_iter()
            .next()
            .expect("one statement")
    }

    /// The shard a text shard key routes to, computed the planner's way.
    fn text_shard(val: &str) -> ShardId {
        let f = shard_function("xxhash64_v1").unwrap();
        catalog()
            .route(f.keyspace_id(&ScalarValue::Text(val.into())))
            .clone()
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
                key_type: Some(ScalarType::Int),
                param_indices: vec![1],
                write: false,
            })
        );
    }

    fn param(indices: Vec<u32>, write: bool) -> Parameterized {
        Parameterized {
            shard_function: "xxhash64_v1".into(),
            key_type: None,
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
    fn resolve_bound_rejects_an_unknown_shard_function() {
        // A hand-built plan (public fields) with a bad function must not panic.
        let bad = Parameterized {
            shard_function: "md5".into(),
            key_type: None,
            param_indices: vec![1],
            write: false,
        };
        assert!(matches!(
            resolve_bound(&bad, &[ScalarValue::Int64(1)], &catalog()),
            Plan::Reject { .. }
        ));
    }

    #[test]
    fn resolve_bound_coerces_bound_values_to_the_key_type() {
        let cat = catalog();
        let typed = |write| Parameterized {
            shard_function: "xxhash64_v1".into(),
            key_type: Some(ScalarType::Int),
            param_indices: vec![1],
            write,
        };
        // A text-format bound value coerces to the integer, landing on the same
        // shard as the bare integer and the integer literal — no asymmetry
        // between the literal and bind paths.
        assert_eq!(
            resolve_bound(&typed(false), &[ScalarValue::Text("2".into())], &cat),
            Plan::SingleShard(shard_of(2))
        );
        assert_eq!(
            resolve_bound(&typed(false), &[ScalarValue::Int64(2)], &cat),
            Plan::SingleShard(shard_of(2))
        );
        // A bound value that is not valid for the type is rejected, not routed
        // to a guessed shard.
        assert!(is_reject(&resolve_bound(
            &typed(true),
            &[ScalarValue::Text("abc".into())],
            &cat
        )));
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
                key_type: Some(ScalarType::Int),
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
    fn insert_with_a_cte_or_a_values_subquery_is_rejected() {
        // A data-modifying CTE would run only on the INSERT's shard, executing
        // its writes against the wrong shard's rows.
        assert!(is_reject(&plan1(
            "WITH d AS (DELETE FROM orders WHERE customer_id = 1 RETURNING id) \
             INSERT INTO orders (customer_id, note) VALUES (0, 'x')"
        )));
        // A subquery in a non-key VALUES cell would read the wrong shard's data,
        // whether bare or hidden inside a CASE (or another expression node).
        assert!(is_reject(&plan1(
            "INSERT INTO orders (customer_id, note) \
             VALUES (0, (SELECT note FROM orders WHERE customer_id = 1 LIMIT 1))"
        )));
        assert!(is_reject(&plan1(
            "INSERT INTO orders (customer_id, note) VALUES \
             (0, CASE WHEN true THEN (SELECT note FROM orders WHERE customer_id = 1) ELSE 'y' END)"
        )));
        // Without either, a plain INSERT (even omitting the sequence column)
        // still routes by its shard key.
        assert_eq!(
            plan1("INSERT INTO orders (customer_id, note) VALUES (0, 'x')"),
            Plan::SingleShard(shard_of(0))
        );
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
        // DO NOTHING whose arbiter cannot be proven shard-local is rejected: a
        // non-shard-key unique constraint can conflict on ANOTHER shard, and the
        // routed insert would create a logical duplicate there invisibly.
        for sql in [
            // Bare: matches any constraint, including cross-shard ones.
            "INSERT INTO orders (customer_id) VALUES (1) ON CONFLICT DO NOTHING",
            // Arbiter without the shard key.
            "INSERT INTO orders (id, customer_id) VALUES (7, 1) ON CONFLICT (id) DO NOTHING",
            // Named constraint: its columns are unknown here.
            "INSERT INTO orders (customer_id) VALUES (1) ON CONFLICT ON CONSTRAINT orders_pk DO NOTHING",
        ] {
            assert!(is_reject(&plan1(sql)), "{sql}");
        }
        // A compound arbiter including the shard key is still shard-local.
        assert_eq!(
            plan1(
                "INSERT INTO orders (customer_id, total) VALUES (1, 5) \
                 ON CONFLICT (customer_id, total) DO NOTHING"
            ),
            Plan::SingleShard(shard_of(1))
        );
    }

    #[test]
    fn unknown_relations_are_rejected_not_routed_to_the_system_database() {
        // A typo'd or unconfigured relation must error, not read a same-named
        // table in the system database.
        for sql in [
            "SELECT * FROM orderz",
            "SELECT * FROM orders o JOIN mystery m ON m.id = o.id WHERE o.customer_id = 1",
            "SELECT * FROM settings UNION ALL SELECT * FROM mystery",
        ] {
            assert!(
                matches!(&plan1(sql), Plan::Reject { code, .. } if *code == UNDEFINED_TABLE),
                "{sql}"
            );
        }
        // Qualified catalog reads keep routing like globals (they exist in every
        // database); unqualified ones fold to public and are unknown like any
        // other relation.
        assert_eq!(plan1("SELECT * FROM pg_catalog.pg_class"), Plan::Unsharded);
        assert_eq!(
            plan1("SELECT * FROM information_schema.tables"),
            Plan::Unsharded
        );
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
    fn keyed_routes_reject_a_subquery_anywhere_not_only_in_the_where() {
        // A subquery runs only on the routed shard, so a keyed route carrying one
        // is rejected wherever it sits — not just in the WHERE. Each of these
        // hides the subquery in a spot a walk that stopped at the WHERE, or that
        // enumerated only some node types, would let through as silently wrong
        // data.
        let rejected = [
            // A scalar subquery in the target list.
            "SELECT (SELECT count(*) FROM line_items), note FROM orders WHERE customer_id = 5",
            // A subquery inside an aggregate FILTER — reached only by descending
            // a function's filter clause, which the previous hand-walk did not.
            "SELECT count(*) FILTER (WHERE total > (SELECT avg(total) FROM line_items)) \
             FROM orders WHERE customer_id = 5",
            // A subquery in HAVING.
            "SELECT count(*) FROM orders WHERE customer_id = 5 \
             HAVING count(*) > (SELECT count(*) FROM line_items)",
            // A subquery buried in an array element in the WHERE.
            "SELECT * FROM orders WHERE customer_id = 5 \
             AND status = ANY(ARRAY[(SELECT status FROM line_items LIMIT 1)])",
            // A subquery in an UPDATE's SET value rather than its WHERE.
            "UPDATE orders SET note = (SELECT note FROM line_items LIMIT 1) WHERE customer_id = 5",
            // A subquery in a RETURNING list.
            "DELETE FROM orders WHERE customer_id = 5 \
             RETURNING (SELECT count(*) FROM line_items)",
        ];
        for sql in rejected {
            assert!(is_reject(&plan1(sql)), "should reject: {sql}");
        }
        // The same statements without a subquery still route by their shard key.
        assert_eq!(
            plan1("SELECT count(*) FROM orders WHERE customer_id = 5"),
            Plan::SingleShard(shard_of(5))
        );
        assert_eq!(
            plan1("UPDATE orders SET note = 'x' WHERE customer_id = 5"),
            Plan::SingleShard(shard_of(5))
        );
    }

    #[test]
    fn keyed_routes_reject_a_cte_which_is_not_a_subquery() {
        // A CTE is a SelectStmt/DeleteStmt/…, not a SubLink, so the subquery check
        // does not see it — but it still reaches other shards. These must reject.
        let rejected = [
            // A CTE that *shadows* the sharded table name: the outer FROM looks
            // like base `orders` and routes by customer_id = 5, but PostgreSQL
            // resolves `orders` to the CTE, which reads line_items on shard 9.
            "WITH orders AS (SELECT 5 AS customer_id, note FROM line_items WHERE customer_id = 9) \
             SELECT * FROM orders WHERE customer_id = 5",
            // A read-only CTE alongside a keyed read still can't be routed.
            "WITH recent AS (SELECT id FROM line_items) \
             SELECT * FROM orders WHERE customer_id = 5",
            // Data-modifying CTEs on an UPDATE/DELETE target: the write runs on
            // the routed shard, not the rows' real shard.
            "WITH d AS (DELETE FROM line_items WHERE customer_id = 9 RETURNING id) \
             UPDATE orders SET note = 'x' WHERE customer_id = 5",
            "WITH u AS (UPDATE line_items SET note = 'z' WHERE customer_id = 9 RETURNING id) \
             DELETE FROM orders WHERE customer_id = 5",
        ];
        for sql in rejected {
            assert!(is_reject(&plan1(sql)), "should reject: {sql}");
        }
    }

    #[test]
    fn set_operations_touching_a_sharded_table_are_rejected() {
        // A set operation nests its arms with an empty top-level FROM. Its arms
        // may sit on different shards and its result needs a cross-shard merge,
        // so running it whole on one shard would silently drop the others' rows.
        let rejected = [
            "SELECT * FROM orders WHERE customer_id = 5 UNION SELECT * FROM orders WHERE customer_id = 6",
            "SELECT id FROM orders EXCEPT SELECT id FROM line_items",
            "SELECT customer_id FROM orders WHERE customer_id = 5 \
             INTERSECT SELECT customer_id FROM orders WHERE customer_id = 6",
            // A sharded table buried in a nested (second-level) set operation.
            "SELECT 1 UNION (SELECT 2 UNION SELECT customer_id FROM orders WHERE customer_id = 7)",
            // A sharded table hidden in an arm's expression subquery — invisible
            // to the leaf-FROM walk, so it must be caught as a subquery.
            "SELECT (SELECT count(*) FROM orders) UNION SELECT 0",
            // A sharded table hidden in a CTE body, both on the whole set
            // operation and nested in a single arm.
            "WITH x AS (SELECT customer_id FROM orders) SELECT customer_id FROM x UNION SELECT 0",
            "SELECT 1 UNION (WITH y AS (SELECT customer_id FROM orders) SELECT customer_id FROM y)",
        ];
        for sql in rejected {
            assert!(is_reject(&plan1(sql)), "should reject: {sql}");
        }
        // With no sharded arm, a set operation still routes: a global-only one
        // runs whole on the system database, a tableless one is session-local.
        assert_eq!(
            plan1("SELECT k, v FROM settings UNION SELECT k, v FROM settings"),
            Plan::Unsharded
        );
        assert_eq!(plan1("SELECT 1 UNION SELECT 2"), Plan::RouterLocal);
    }

    #[test]
    fn tableless_select_hiding_a_sharded_reference_is_not_run_on_one_shard() {
        // A tableless statement with no subquery is session-local.
        assert_eq!(plan1("SELECT 1"), Plan::RouterLocal);
        assert_eq!(plan1("SELECT current_setting('x')"), Plan::RouterLocal);
        // But a target-list subquery or a CTE can reach a sharded table even with
        // no FROM. Such a statement must not run on one arbitrary shard (which
        // returns partial data); it routes to the system database, where a sharded
        // reference errors loudly instead of answering wrong.
        assert_eq!(
            plan1("SELECT (SELECT count(*) FROM orders)"),
            Plan::Unsharded
        );
        assert_eq!(
            plan1("WITH x AS (SELECT customer_id FROM orders) SELECT (SELECT count(*) FROM x)"),
            Plan::Unsharded
        );
        // A subquery over a global table is answered correctly on the system db.
        assert_eq!(
            plan1("SELECT (SELECT count(*) FROM settings)"),
            Plan::Unsharded
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

    #[test]
    fn quoted_integer_key_routes_like_a_bare_integer() {
        // The fix: a string literal against an integer key coerces to the
        // integer, so `'1'` and `1` hit the same shard — read, insert, delete.
        assert_eq!(
            plan1("SELECT * FROM orders WHERE customer_id = '1'"),
            Plan::SingleShard(shard_of(1))
        );
        assert_eq!(
            plan1("INSERT INTO orders (customer_id) VALUES ('7')"),
            Plan::SingleShard(shard_of(7))
        );
        assert_eq!(
            plan1("DELETE FROM orders WHERE customer_id = '7'"),
            Plan::SingleShard(shard_of(7))
        );
    }

    #[test]
    fn non_integer_literal_against_an_integer_key_scatters_reads() {
        // 'abc' is no integer; scatter so PostgreSQL returns its own type error
        // rather than the planner hashing a shard the row could never be on.
        assert_eq!(
            plan1("SELECT * FROM orders WHERE customer_id = 'abc'"),
            Plan::Scatter(catalog().all())
        );
    }

    #[test]
    fn non_integer_literal_against_an_integer_key_rejects_writes() {
        assert!(is_reject(&plan1(
            "INSERT INTO orders (customer_id) VALUES ('abc')"
        )));
        assert!(is_reject(&plan1(
            "DELETE FROM orders WHERE customer_id = 'abc'"
        )));
    }

    #[test]
    fn a_text_key_routes_by_text_and_rejects_bare_integers() {
        let t = Some(ScalarType::Text);
        // A string literal routes by its text bytes.
        assert_eq!(
            plan1_with("SELECT * FROM orders WHERE customer_id = '42'", t),
            Plan::SingleShard(text_shard("42"))
        );
        // A bare integer has no `text = int` operator in PostgreSQL: a read
        // scatters and a write is rejected, never guessed.
        assert_eq!(
            plan1_with("SELECT * FROM orders WHERE customer_id = 42", t),
            Plan::Scatter(catalog().all())
        );
        assert!(is_reject(&plan1_with(
            "INSERT INTO orders (customer_id) VALUES (42)",
            t
        )));
    }

    #[test]
    fn an_untyped_key_hashes_the_literal_as_written() {
        // With no declared type (a topology from before the field existed), the
        // literal is hashed in the form written: '1' as text, 1 as an integer.
        assert_eq!(
            plan1_with("SELECT * FROM orders WHERE customer_id = '1'", None),
            Plan::SingleShard(text_shard("1"))
        );
        assert_eq!(
            plan1_with("SELECT * FROM orders WHERE customer_id = 1", None),
            Plan::SingleShard(shard_of(1))
        );
    }
}
