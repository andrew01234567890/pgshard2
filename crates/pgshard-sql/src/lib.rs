//! SQL parsing for the router: real PostgreSQL grammar via libpg_query,
//! statement classification, table extraction, and the literal-insensitive
//! fingerprint used to key the routing-plan cache.
//!
//! Grammar version: this pins `pg_query` 6.1.1, which embeds libpg_query 17
//! (the PostgreSQL 17 grammar) — the newest published Rust binding. It parses
//! the overwhelming majority of PostgreSQL 18 SQL, but syntax introduced in 18
//! itself returns a parse error until a libpg_query-18-based binding ships.
//! This is the one place the "PG18-only" product briefly runs a PG17 grammar.

use pg_query::NodeEnum;
use pg_query::protobuf::{SelectStmt, TransactionStmtKind};

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
    Merge,
    TxnBegin,
    TxnCommit,
    TxnRollback,
    TxnOther,
    Set,
    Show,
    /// DDL plus broadcast utility — schema/catalog changes and
    /// VACUUM/ANALYZE/REINDEX/CLUSTER, GRANT, COMMENT, SECURITY LABEL — that
    /// route to every shard rather than by shard key.
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
    ///
    /// It identifies query *shape*, not shard-key position: libpg_query does
    /// not distinguish column order in an INSERT target list, so
    /// `INSERT INTO t (a, b) ...` and `INSERT INTO t (b, a) ...` collide.
    /// The planner must therefore bind shard keys by column *name* from the
    /// AST — never by parameter index inferred from a fingerprint match.
    ///
    /// It also collapses variable-length constant lists: `id IN (1, 2, 3)`,
    /// `id IN (1)`, and multi-row `VALUES`/INSERT all share a fingerprint. List
    /// and row *cardinality* is not in the key — the count of shard-key values
    /// must be read from the AST, or a multi-shard query could reuse (and
    /// under-route through) a cached single-shard plan.
    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    pub fn statements(&self) -> &[StatementKind] {
        &self.statements
    }

    /// Relations referenced anywhere in the query, as written
    /// (schema-qualified when the query qualified them), deduped.
    ///
    /// libpg_query excludes CTE names parse-wide, so a real relation whose
    /// *unqualified* name equals a CTE name is dropped — even within a single
    /// statement, in any protocol: `WITH orders AS (...) SELECT FROM orders`
    /// returns no `orders`. Schema-qualifying the relation avoids it (the CTE
    /// name never carries a schema). The planner must not read an empty result
    /// as "no relations" for a query that has a WITH clause.
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
    // libpg_query's fingerprint re-parses internally, so a full parse costs
    // two parser passes. That cost falls only on a plan-cache miss; the
    // steady-state hot path is a cache hit that never calls parse.
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

/// libpg_query's literal-normalization (`$n` placeholders): the canonical text
/// a fingerprint is computed over.
pub fn normalize(sql: &str) -> Result<String, SqlError> {
    pg_query::normalize(sql).map_err(SqlError::Parse)
}

fn classify(node: Option<&NodeEnum>) -> StatementKind {
    let Some(node) = node else {
        return StatementKind::Empty;
    };
    match node {
        // `SELECT ... INTO newtable` creates a relation: it is DDL, not a read.
        NodeEnum::SelectStmt(s) if s.into_clause.is_some() => StatementKind::Ddl,
        // A data-modifying CTE (`WITH x AS (DELETE/INSERT/UPDATE/MERGE ...)
        // SELECT ...`) writes; keep it off the read path instead of labeling
        // it Select. Other is the conservative bucket — never a read signal.
        NodeEnum::SelectStmt(s) if has_writable_cte(s) => StatementKind::Other,
        NodeEnum::SelectStmt(_) => StatementKind::Select,
        NodeEnum::InsertStmt(_) => StatementKind::Insert,
        NodeEnum::UpdateStmt(_) => StatementKind::Update,
        NodeEnum::DeleteStmt(_) => StatementKind::Delete,
        NodeEnum::MergeStmt(_) => StatementKind::Merge,
        // TransactionStmt.kind is a raw i32; decode via try_from so the crate
        // builds whether or not prost regenerated the enum accessor.
        NodeEnum::TransactionStmt(t) => match TransactionStmtKind::try_from(t.kind) {
            Ok(TransactionStmtKind::TransStmtBegin | TransactionStmtKind::TransStmtStart) => {
                StatementKind::TxnBegin
            }
            Ok(TransactionStmtKind::TransStmtCommit) => StatementKind::TxnCommit,
            Ok(TransactionStmtKind::TransStmtRollback) => StatementKind::TxnRollback,
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
        | NodeEnum::RefreshMatViewStmt(_)
        | NodeEnum::CreateForeignTableStmt(_)
        | NodeEnum::CreateStatsStmt(_)
        | NodeEnum::AlterStatsStmt(_)
        | NodeEnum::AlterDomainStmt(_)
        | NodeEnum::CreateDomainStmt(_)
        | NodeEnum::CreateFunctionStmt(_)
        | NodeEnum::AlterFunctionStmt(_)
        | NodeEnum::CreateTrigStmt(_)
        | NodeEnum::CreateEnumStmt(_)
        | NodeEnum::AlterEnumStmt(_)
        | NodeEnum::CompositeTypeStmt(_)
        | NodeEnum::AlterTypeStmt(_)
        | NodeEnum::DefineStmt(_)
        | NodeEnum::CreateExtensionStmt(_)
        | NodeEnum::AlterExtensionStmt(_)
        | NodeEnum::AlterExtensionContentsStmt(_)
        | NodeEnum::CreatePolicyStmt(_)
        | NodeEnum::AlterPolicyStmt(_)
        | NodeEnum::RuleStmt(_)
        | NodeEnum::CreateCastStmt(_)
        | NodeEnum::CreateTableSpaceStmt(_)
        | NodeEnum::CreateAmStmt(_)
        | NodeEnum::CreateFdwStmt(_)
        | NodeEnum::AlterFdwStmt(_)
        | NodeEnum::CreateForeignServerStmt(_)
        | NodeEnum::AlterForeignServerStmt(_)
        | NodeEnum::CreateUserMappingStmt(_)
        | NodeEnum::AlterUserMappingStmt(_)
        | NodeEnum::DropUserMappingStmt(_)
        | NodeEnum::ImportForeignSchemaStmt(_)
        | NodeEnum::CreatePublicationStmt(_)
        | NodeEnum::AlterPublicationStmt(_)
        | NodeEnum::CreateSubscriptionStmt(_)
        | NodeEnum::AlterSubscriptionStmt(_)
        | NodeEnum::DropSubscriptionStmt(_)
        | NodeEnum::CreateRoleStmt(_)
        | NodeEnum::AlterRoleStmt(_)
        | NodeEnum::DropRoleStmt(_)
        | NodeEnum::AlterObjectSchemaStmt(_)
        | NodeEnum::AlterOwnerStmt(_)
        | NodeEnum::SecLabelStmt(_)
        | NodeEnum::VacuumStmt(_)
        | NodeEnum::ClusterStmt(_)
        | NodeEnum::ReindexStmt(_)
        | NodeEnum::CommentStmt(_)
        | NodeEnum::GrantStmt(_)
        | NodeEnum::GrantRoleStmt(_) => StatementKind::Ddl,
        _ => StatementKind::Other,
    }
}

/// True when a SELECT carries a data-modifying CTE (`WITH x AS (INSERT/UPDATE/
/// DELETE/MERGE ...) SELECT ...`). Such a statement writes, so it must not be
/// classified as a plain read.
fn has_writable_cte(select: &SelectStmt) -> bool {
    let Some(with) = select.with_clause.as_ref() else {
        return false;
    };
    with.ctes.iter().any(|node| {
        let Some(NodeEnum::CommonTableExpr(cte)) = node.node.as_ref() else {
            return false;
        };
        matches!(
            cte.ctequery.as_deref().and_then(|q| q.node.as_ref()),
            Some(
                NodeEnum::InsertStmt(_)
                    | NodeEnum::UpdateStmt(_)
                    | NodeEnum::DeleteStmt(_)
                    | NodeEnum::MergeStmt(_)
            )
        )
    })
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
            // SELECT ... INTO creates a relation — DDL, not a read.
            ("SELECT * INTO dst FROM src", StatementKind::Ddl),
            ("REFRESH MATERIALIZED VIEW mv", StatementKind::Ddl),
            ("CREATE STATISTICS st ON a, b FROM t", StatementKind::Ddl),
            ("ALTER STATISTICS st SET STATISTICS 100", StatementKind::Ddl),
            (
                "CREATE FOREIGN TABLE ft (id int) SERVER srv",
                StatementKind::Ddl,
            ),
            (
                "IMPORT FOREIGN SCHEMA remote LIMIT TO (t) FROM SERVER srv INTO local",
                StatementKind::Ddl,
            ),
            (
                "DROP USER MAPPING FOR CURRENT_USER SERVER srv",
                StatementKind::Ddl,
            ),
            ("CREATE PUBLICATION p FOR ALL TABLES", StatementKind::Ddl),
            (
                "CREATE SUBSCRIPTION s CONNECTION 'x' PUBLICATION p",
                StatementKind::Ddl,
            ),
            (
                "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DO NOTHING",
                StatementKind::Merge,
            ),
            // A data-modifying CTE writes; it must not be classified as a read.
            (
                "WITH d AS (DELETE FROM orders WHERE id = 1 RETURNING id) SELECT count(*) FROM d",
                StatementKind::Other,
            ),
            // A read-only CTE stays a Select.
            (
                "WITH recent AS (SELECT id FROM orders LIMIT 10) SELECT count(*) FROM recent",
                StatementKind::Select,
            ),
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
