//! Pulling shard-key values and table names out of the libpg_query AST.
//!
//! The guarantee every extractor keeps: the returned value set `V` is such that
//! *every row the statement can match has its shard key in `V`*. An empty set
//! means no such guarantee (the statement is unkeyed). This is what makes it
//! safe to route a read to the union of `V`'s shards and to reject a write
//! whose `V` spans shards — the planner never under-routes.

use pg_query::NodeEnum;
use pg_query::protobuf::{
    AExpr, AExprKind, BoolExprType, ColumnRef, InsertStmt, Node, RangeVar, a_const,
};

use pgshard_core::{ScalarValue, TableName};

/// A shard-key value read from the AST.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyVal {
    /// A literal, hashable now.
    Const(ScalarValue),
    /// A `$n` placeholder (1-based); its value is known only at Bind.
    Param(u32),
}

fn as_enum(node: &Node) -> Option<&NodeEnum> {
    node.node.as_ref()
}

/// The relation name a `RangeVar` refers to, defaulting an unqualified name to
/// `public` (v1 does not model `search_path`).
pub fn range_var_table(rv: &RangeVar) -> TableName {
    let schema = if rv.schemaname.is_empty() {
        "public"
    } else {
        &rv.schemaname
    };
    TableName::new(schema, rv.relname.clone())
}

/// A scalar literal from an `A_Const`, or `None` when it is NULL or a type the
/// planner does not hash (v1 hashes integer and string literals only — an
/// out-of-range integer arrives as a `Float` string and is parsed back to i64).
fn scalar_from_const(c: &pg_query::protobuf::AConst) -> Option<ScalarValue> {
    if c.isnull {
        return None;
    }
    match c.val.as_ref()? {
        a_const::Val::Ival(i) => Some(ScalarValue::Int64(i64::from(i.ival))),
        a_const::Val::Fval(f) => f.fval.parse::<i64>().ok().map(ScalarValue::Int64),
        a_const::Val::Sval(s) => Some(ScalarValue::Text(s.sval.clone())),
        // bool / bitstring shard keys are not supported in v1.
        _ => None,
    }
}

/// A single value expression (`A_Const` or `$n`) as a [`KeyVal`], or `None` for
/// anything the planner cannot pin (function calls, expressions, unsupported
/// literal types).
fn key_val(node: &NodeEnum) -> Option<KeyVal> {
    match node {
        NodeEnum::AConst(c) => scalar_from_const(c).map(KeyVal::Const),
        NodeEnum::ParamRef(p) if p.number > 0 => Some(KeyVal::Param(p.number as u32)),
        _ => None,
    }
}

/// The last identifier of a `ColumnRef` (`t.customer_id` -> `customer_id`),
/// ignoring `*` and positional fields.
fn column_name(cr: &ColumnRef) -> Option<&str> {
    match cr.fields.last().and_then(as_enum)? {
        NodeEnum::String(s) => Some(&s.sval),
        _ => None,
    }
}

/// True when `node` is a `ColumnRef` naming `key_col`.
fn is_key_column(node: &NodeEnum, key_col: &str) -> bool {
    matches!(node, NodeEnum::ColumnRef(cr) if column_name(cr) == Some(key_col))
}

/// Collect shard-key values guaranteed to bound the matching rows, from a WHERE
/// clause (or any boolean expression). Only AND-reachable `key = value` and
/// `key IN (...)` constraints contribute; OR/NOT subtrees are not descended, so
/// the result stays a sound over-approximation. An `IN` list contributes only
/// if every element is a value the planner can read.
pub fn where_key_values(where_clause: Option<&Node>, key_col: &str) -> Vec<KeyVal> {
    let mut out = Vec::new();
    if let Some(node) = where_clause.and_then(as_enum) {
        collect_and(node, key_col, &mut out);
    }
    out
}

fn collect_and(node: &NodeEnum, key_col: &str, out: &mut Vec<KeyVal>) {
    match node {
        NodeEnum::BoolExpr(b) if b.boolop == BoolExprType::AndExpr as i32 => {
            for arg in &b.args {
                if let Some(inner) = as_enum(arg) {
                    collect_and(inner, key_col, out);
                }
            }
        }
        NodeEnum::AExpr(e) => collect_a_expr(e, key_col, out),
        _ => {}
    }
}

fn collect_a_expr(e: &AExpr, key_col: &str, out: &mut Vec<KeyVal>) {
    let lhs = e.lexpr.as_deref().and_then(as_enum);
    let rhs = e.rexpr.as_deref().and_then(as_enum);
    let (Some(lhs), Some(rhs)) = (lhs, rhs) else {
        return;
    };
    match a_expr_kind(e) {
        // `key = value` (either operand order), operator `=`.
        Some(AExprKind::AexprOp) if is_eq_operator(e) => {
            if is_key_column(lhs, key_col) {
                push_value(rhs, out);
            } else if is_key_column(rhs, key_col) {
                push_value(lhs, out);
            }
        }
        // `key IN (v1, v2, ...)`: rexpr is a List. Contributes only if the
        // column is on the left and every element is a readable value.
        Some(AExprKind::AexprIn) if is_eq_operator(e) && is_key_column(lhs, key_col) => {
            if let NodeEnum::List(list) = rhs {
                let mut vals = Vec::with_capacity(list.items.len());
                for item in &list.items {
                    match as_enum(item).and_then(key_val) {
                        Some(v) => vals.push(v),
                        // An element we cannot read means the IN set is
                        // unbounded from our view: drop the whole constraint.
                        None => return,
                    }
                }
                out.extend(vals);
            }
        }
        _ => {}
    }
}

/// A single scalar/param value pushed if readable; an unreadable operand simply
/// contributes no constraint.
fn push_value(node: &NodeEnum, out: &mut Vec<KeyVal>) {
    if let Some(v) = key_val(node) {
        out.push(v);
    }
}

fn a_expr_kind(e: &AExpr) -> Option<AExprKind> {
    AExprKind::try_from(e.kind).ok()
}

/// True when the operator name of an `A_Expr` is `=`. `IN` also uses `=` as its
/// element operator.
fn is_eq_operator(e: &AExpr) -> bool {
    e.name.len() == 1
        && matches!(
            e.name.first().and_then(as_enum),
            Some(NodeEnum::String(s)) if s.sval == "="
        )
}

/// The shard-key values of an `INSERT`, one per row, or `None` when they cannot
/// be read: no explicit column list, the shard-key column absent, an
/// `INSERT ... SELECT`, or a row whose shard-key cell is not a literal/param
/// (e.g. `DEFAULT` or an expression). `None` forces the write to be rejected
/// rather than mis-routed.
pub fn insert_key_values(ins: &InsertStmt, key_col: &str) -> Option<Vec<KeyVal>> {
    let idx = ins
        .cols
        .iter()
        .position(|c| matches!(as_enum(c), Some(NodeEnum::ResTarget(rt)) if rt.name == key_col))?;
    let NodeEnum::SelectStmt(sel) = ins.select_stmt.as_deref().and_then(as_enum)? else {
        return None;
    };
    if sel.values_lists.is_empty() {
        // INSERT ... SELECT: the values are not literals in this statement.
        return None;
    }
    let mut out = Vec::with_capacity(sel.values_lists.len());
    for row in &sel.values_lists {
        let NodeEnum::List(list) = as_enum(row)? else {
            return None;
        };
        let cell = list.items.get(idx).and_then(as_enum)?;
        out.push(key_val(cell)?);
    }
    Some(out)
}

/// True when a `SET` target list (UPDATE) assigns to `key_col`.
pub fn sets_column(target_list: &[Node], key_col: &str) -> bool {
    target_list
        .iter()
        .any(|t| matches!(as_enum(t), Some(NodeEnum::ResTarget(rt)) if rt.name == key_col))
}

/// The relations of a FROM clause and whether every item is a plain table
/// reference. `all_plain` is false when any item is a subquery, function, or
/// other non-`RangeVar` source: the planner cannot see which relations such an
/// item hides (a subquery may scan a sharded table on another shard), so it must
/// refuse to key-route past one.
pub struct FromClause {
    pub tables: Vec<TableName>,
    pub all_plain: bool,
}

pub fn analyze_from(from_clause: &[Node]) -> FromClause {
    let mut fc = FromClause {
        tables: Vec::new(),
        all_plain: true,
    };
    for item in from_clause {
        collect_from(item, &mut fc);
    }
    fc
}

fn collect_from(node: &Node, fc: &mut FromClause) {
    match as_enum(node) {
        Some(NodeEnum::RangeVar(rv)) => fc.tables.push(range_var_table(rv)),
        Some(NodeEnum::JoinExpr(j)) => {
            if let Some(l) = j.larg.as_deref() {
                collect_from(l, fc);
            }
            if let Some(r) = j.rarg.as_deref() {
                collect_from(r, fc);
            }
        }
        // A subquery, function, or anything else in the FROM: its relations are
        // invisible to shard-key extraction.
        _ => fc.all_plain = false,
    }
}

/// True when an expression subtree contains a sub-select (`SubLink`). A sharded
/// statement whose WHERE both pins a shard key and runs a subquery is rejected:
/// the subquery would execute only on the routed shard and could reference rows
/// on other shards. Covers the common boolean/comparison/function containers;
/// sub-selects buried in rarer expression nodes are conservatively not the
/// key-routing fast path's concern (documented in the crate root).
pub fn contains_sublink(clause: Option<&Node>) -> bool {
    clause.is_some_and(child_has_sublink)
}

fn child_has_sublink(node: &Node) -> bool {
    as_enum(node).is_some_and(node_has_sublink)
}

fn node_has_sublink(node: &NodeEnum) -> bool {
    match node {
        NodeEnum::SubLink(_) => true,
        NodeEnum::BoolExpr(b) => b.args.iter().any(child_has_sublink),
        NodeEnum::AExpr(e) => {
            e.lexpr.as_deref().is_some_and(child_has_sublink)
                || e.rexpr.as_deref().is_some_and(child_has_sublink)
        }
        NodeEnum::List(l) => l.items.iter().any(child_has_sublink),
        NodeEnum::FuncCall(f) => f.args.iter().any(child_has_sublink),
        NodeEnum::CoalesceExpr(c) => c.args.iter().any(child_has_sublink),
        NodeEnum::MinMaxExpr(m) => m.args.iter().any(child_has_sublink),
        NodeEnum::RowExpr(r) => r.args.iter().any(child_has_sublink),
        NodeEnum::AArrayExpr(a) => a.elements.iter().any(child_has_sublink),
        NodeEnum::NullTest(n) => n.arg.as_deref().is_some_and(child_has_sublink),
        NodeEnum::BooleanTest(b) => b.arg.as_deref().is_some_and(child_has_sublink),
        NodeEnum::TypeCast(t) => t.arg.as_deref().is_some_and(child_has_sublink),
        _ => false,
    }
}
