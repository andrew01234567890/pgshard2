//! SQL parsing for the router: real PostgreSQL grammar via libpg_query,
//! statement classification, table extraction, and the literal-insensitive
//! fingerprint that keys the routing-plan cache.
//!
//! Parser grammar tracks the newest libpg_query release (currently PG17);
//! it accepts PostgreSQL 18 SQL minus syntax introduced in 18 itself, and
//! is upgraded when libpg_query publishes the 18 grammar.

use pg_query::NodeEnum;
use pg_query::protobuf::TransactionStmtKind;

#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    #[error("syntax error: {0}")]
    Parse(pg_query::Error),
    #[error("fingerprint failed: {0}")]
    Fingerprint(pg_query::Error),
}

/// Coarse statement class; the planner refines within each class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementKind {
    Select,
    Insert,
    Update,
    Delete,
    TxnBegin,
    TxnCommit,
    TxnRollback,
    TxnOther,
    Set,
    Show,
    Ddl,
    Copy,
    PrepareExec,
    Empty,
    Other,
}

/// A parsed query string (possibly multi-statement).
pub struct Parsed {
    fingerprint: u64,
    statements: Vec<StatementKind>,
    tables: Vec<String>,
    result: pg_query::ParseResult,
}

impl Parsed {
    /// Literal-insensitive fingerprint (libpg_query semantics):
    /// `SELECT * FROM t WHERE id = 1` and `... = 42` share a value.
    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    pub fn statements(&self) -> &[StatementKind] {
        &self.statements
    }

    /// Relations referenced anywhere in the query, as written
    /// (schema-qualified when the query qualified them), deduped.
    pub fn tables(&self) -> &[String] {
        &self.tables
    }

    /// Full AST for the planner.
    pub fn result(&self) -> &pg_query::ParseResult {
        &self.result
    }
}

pub fn parse(sql: &str) -> Result<Parsed, SqlError> {
    let result = pg_query::parse(sql).map_err(SqlError::Parse)?;
    // libpg_query's fingerprint re-parses internally; that doubles the cost
    // of a plan-cache miss only. Steady-state traffic never reaches here —
    // measured in benches/parse.rs.
    let fingerprint = pg_query::fingerprint(sql).map_err(SqlError::Fingerprint)?;
    let statements = result
        .protobuf
        .stmts
        .iter()
        .map(|raw| classify(raw.stmt.as_ref().and_then(|s| s.node.as_ref())))
        .collect();
    let tables = result.tables();
    Ok(Parsed {
        fingerprint: fingerprint.value,
        statements,
        tables,
        result,
    })
}

/// libpg_query's literal-normalization (`$n` placeholders); used by tests to
/// assert fingerprint stability and by slow-query logging.
pub fn normalize(sql: &str) -> Result<String, SqlError> {
    pg_query::normalize(sql).map_err(SqlError::Parse)
}

fn classify(node: Option<&NodeEnum>) -> StatementKind {
    let Some(node) = node else {
        return StatementKind::Empty;
    };
    match node {
        NodeEnum::SelectStmt(_) => StatementKind::Select,
        NodeEnum::InsertStmt(_) => StatementKind::Insert,
        NodeEnum::UpdateStmt(_) => StatementKind::Update,
        NodeEnum::DeleteStmt(_) => StatementKind::Delete,
        NodeEnum::TransactionStmt(t) => match t.kind() {
            TransactionStmtKind::TransStmtBegin | TransactionStmtKind::TransStmtStart => {
                StatementKind::TxnBegin
            }
            TransactionStmtKind::TransStmtCommit => StatementKind::TxnCommit,
            TransactionStmtKind::TransStmtRollback => StatementKind::TxnRollback,
            _ => StatementKind::TxnOther,
        },
        NodeEnum::VariableSetStmt(_) => StatementKind::Set,
        NodeEnum::VariableShowStmt(_) => StatementKind::Show,
        NodeEnum::CopyStmt(_) => StatementKind::Copy,
        NodeEnum::PrepareStmt(_) | NodeEnum::ExecuteStmt(_) | NodeEnum::DeallocateStmt(_) => {
            StatementKind::PrepareExec
        }
        NodeEnum::CreateStmt(_)
        | NodeEnum::AlterTableStmt(_)
        | NodeEnum::IndexStmt(_)
        | NodeEnum::DropStmt(_)
        | NodeEnum::RenameStmt(_)
        | NodeEnum::TruncateStmt(_)
        | NodeEnum::CreateSchemaStmt(_)
        | NodeEnum::CreateSeqStmt(_)
        | NodeEnum::AlterSeqStmt(_)
        | NodeEnum::ViewStmt(_)
        | NodeEnum::CreateTableAsStmt(_)
        | NodeEnum::AlterDomainStmt(_)
        | NodeEnum::CreateDomainStmt(_)
        | NodeEnum::CreateFunctionStmt(_)
        | NodeEnum::AlterFunctionStmt(_)
        | NodeEnum::CreateTrigStmt(_)
        | NodeEnum::CreateEnumStmt(_)
        | NodeEnum::AlterEnumStmt(_)
        | NodeEnum::CompositeTypeStmt(_)
        | NodeEnum::CreateExtensionStmt(_)
        | NodeEnum::CreatePolicyStmt(_)
        | NodeEnum::AlterPolicyStmt(_)
        | NodeEnum::VacuumStmt(_)
        | NodeEnum::ClusterStmt(_)
        | NodeEnum::ReindexStmt(_)
        | NodeEnum::CommentStmt(_)
        | NodeEnum::GrantStmt(_)
        | NodeEnum::GrantRoleStmt(_) => StatementKind::Ddl,
        _ => StatementKind::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_ignores_literals_but_not_structure() {
        let a = parse("SELECT * FROM orders WHERE customer_id = 1").unwrap();
        let b = parse("SELECT * FROM orders WHERE customer_id = 42").unwrap();
        let c = parse("SELECT * FROM orders WHERE customer_id = $1").unwrap();
        let d = parse("SELECT id FROM orders WHERE customer_id = 1").unwrap();
        assert_eq!(a.fingerprint(), b.fingerprint());
        assert_eq!(a.fingerprint(), c.fingerprint());
        assert_ne!(a.fingerprint(), d.fingerprint());
    }

    #[test]
    fn fingerprint_matches_normalized_form() {
        let raw = "INSERT INTO t (a, b) VALUES (1, 'x')";
        let normalized = normalize(raw).unwrap();
        assert_eq!(
            parse(raw).unwrap().fingerprint(),
            parse(&normalized).unwrap().fingerprint()
        );
    }

    #[test]
    fn classifies_statements() {
        let cases: &[(&str, StatementKind)] = &[
            ("SELECT 1", StatementKind::Select),
            ("INSERT INTO t VALUES (1)", StatementKind::Insert),
            ("UPDATE t SET a = 1", StatementKind::Update),
            ("DELETE FROM t", StatementKind::Delete),
            ("BEGIN", StatementKind::TxnBegin),
            ("START TRANSACTION", StatementKind::TxnBegin),
            ("COMMIT", StatementKind::TxnCommit),
            ("ROLLBACK", StatementKind::TxnRollback),
            ("SAVEPOINT s", StatementKind::TxnOther),
            ("SET search_path = app", StatementKind::Set),
            ("SHOW server_version", StatementKind::Show),
            ("CREATE TABLE t (id int)", StatementKind::Ddl),
            ("ALTER TABLE t ADD COLUMN c text", StatementKind::Ddl),
            ("CREATE INDEX ON t (id)", StatementKind::Ddl),
            ("DROP TABLE t", StatementKind::Ddl),
            ("TRUNCATE t", StatementKind::Ddl),
            ("COPY t FROM STDIN", StatementKind::Copy),
            ("PREPARE p AS SELECT 1", StatementKind::PrepareExec),
            ("EXPLAIN SELECT 1", StatementKind::Other),
        ];
        for (sql, want) in cases {
            let parsed = parse(sql).unwrap();
            assert_eq!(parsed.statements(), &[*want], "{sql}");
        }
    }

    #[test]
    fn multi_statement_queries_classify_each() {
        let parsed = parse("BEGIN; UPDATE t SET a = 1 WHERE id = 7; COMMIT").unwrap();
        assert_eq!(
            parsed.statements(),
            &[
                StatementKind::TxnBegin,
                StatementKind::Update,
                StatementKind::TxnCommit
            ]
        );
    }

    #[test]
    fn extracts_tables_including_qualified_and_joins() {
        let parsed =
            parse("SELECT o.id FROM public.orders o JOIN customers c ON c.id = o.customer_id")
                .unwrap();
        let mut tables = parsed.tables().to_vec();
        tables.sort();
        assert_eq!(tables, vec!["customers", "public.orders"]);
    }

    #[test]
    fn syntax_errors_are_reported() {
        assert!(parse("SELEC 1").is_err());
        assert!(parse("").is_ok());
    }
}
