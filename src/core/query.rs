//! SQL Query Analysis with WHERE Evaluation

use crate::core::row::{RowData, RowValue};
use dashmap::DashMap;
use rustc_hash::{FxBuildHasher, FxHasher};
use serde_json::Value;
use sqlparser::ast::*;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};

// Use Arc to avoid cloning QueryAnalysis on cache hit
static CACHE: LazyLock<DashMap<u64, Arc<QueryAnalysis>, FxBuildHasher>> =
    LazyLock::new(|| DashMap::with_hasher(FxBuildHasher));

/// Filter extracted from WHERE clause  
/// Uses Box<str> for column names - smaller enum size, faster cloning
#[derive(Debug, Clone)]
pub enum WhereFilter {
    /// col = val
    Eq { col: Box<str>, val: FilterValue },
    /// col != val  
    Ne { col: Box<str>, val: FilterValue },
    /// col > val
    Gt { col: Box<str>, val: FilterValue },
    /// col >= val
    Gte { col: Box<str>, val: FilterValue },
    /// col < val
    Lt { col: Box<str>, val: FilterValue },
    /// col <= val
    Lte { col: Box<str>, val: FilterValue },
    /// col IN (val1, val2, ...)
    In {
        col: Box<str>,
        vals: Box<[FilterValue]>,
    },
    /// col IS NULL
    IsNull { col: Box<str> },
    /// col IS NOT NULL
    IsNotNull { col: Box<str> },
    /// AND of multiple filters
    And(Box<[WhereFilter]>),
    /// OR of multiple filters  
    Or(Box<[WhereFilter]>),
    /// Complex filter we can't evaluate
    Complex,
    /// No WHERE clause (matches all)
    None,
}

/// Literal value for comparison - Box<str> for strings
#[derive(Debug, Clone, PartialEq)]
pub enum FilterValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(Box<str>),
}

/// Result of filter evaluation
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EvalResult {
    /// Row matches the filter
    Match,
    /// Row doesn't match the filter
    NoMatch,
    /// Can't determine (need requery)
    Unknown,
}

#[derive(Debug, Clone)]
pub struct QueryAnalysis {
    pub is_valid: bool,
    pub error: Option<String>,
    pub tables: Vec<String>,
    pub filter: WhereFilter,
    pub is_simple: bool, // Single table, no JOINs/subqueries
}

/// Analyze SQL query - cached for performance
#[inline]
pub fn analyze(q: &str) -> QueryAnalysis {
    let h = hash(q);

    // Fast path: cache hit - clone from Arc (QueryAnalysis is small)
    if let Some(cached) = CACHE.get(&h) {
        return QueryAnalysis::clone(&cached);
    }

    // Slow path: parse and cache
    let result = analyze_inner(q);
    if result.is_valid {
        // Simple cache size management - evict 10% when full
        const MAX_CACHE: usize = 1000;
        const EVICT_COUNT: usize = 100;

        if CACHE.len() >= MAX_CACHE {
            // Evict oldest entries (first 10%)
            let to_remove: Vec<u64> = CACHE.iter().take(EVICT_COUNT).map(|e| *e.key()).collect();
            for k in to_remove {
                CACHE.remove(&k);
            }
        }
        CACHE.insert(h, Arc::new(result.clone()));
    }
    result
}

fn analyze_inner(q: &str) -> QueryAnalysis {
    let stmts = match Parser::parse_sql(&PostgreSqlDialect {}, q) {
        Ok(s) if !s.is_empty() => s,
        Ok(_) => return err("Empty"),
        Err(e) => return err(&format!("Parse: {e}")),
    };

    let ast = match &stmts[0] {
        Statement::Query(q) => q,
        _ => return err("Only SELECT"),
    };

    let mut tables = Vec::with_capacity(2);
    let mut has_join = false;
    let mut has_subq = false;
    extract(ast, &mut tables, &mut has_join, &mut has_subq);

    let filter = extract_where(ast);
    let is_simple =
        tables.len() == 1 && !has_join && !has_subq && !matches!(filter, WhereFilter::Complex);

    QueryAnalysis {
        is_valid: true,
        error: None,
        tables,
        filter,
        is_simple,
    }
}

#[inline]
fn err(s: &str) -> QueryAnalysis {
    QueryAnalysis {
        is_valid: false,
        error: Some(s.into()),
        tables: vec![],
        filter: WhereFilter::None,
        is_simple: false,
    }
}

fn extract(q: &Query, t: &mut Vec<String>, has_join: &mut bool, has_subq: &mut bool) {
    if let Some(w) = &q.with {
        for c in &w.cte_tables {
            extract(&c.query, t, has_join, has_subq);
        }
    }
    extract_set(&q.body, t, has_join, has_subq);
}

fn extract_set(e: &SetExpr, t: &mut Vec<String>, has_join: &mut bool, has_subq: &mut bool) {
    match e {
        SetExpr::Select(s) => {
            for f in &s.from {
                extract_join_info(f, t, has_join, has_subq);
            }
        }
        SetExpr::Query(q) => {
            *has_subq = true;
            extract(q, t, has_join, has_subq);
        }
        SetExpr::SetOperation { left, right, .. } => {
            extract_set(left, t, has_join, has_subq);
            extract_set(right, t, has_join, has_subq);
        }
        _ => {}
    }
}

fn extract_join_info(
    j: &TableWithJoins,
    t: &mut Vec<String>,
    has_join: &mut bool,
    has_subq: &mut bool,
) {
    extract_factor(&j.relation, t, has_subq);
    if !j.joins.is_empty() {
        *has_join = true;
    }
    for x in &j.joins {
        extract_factor(&x.relation, t, has_subq);
    }
}

fn extract_factor(f: &TableFactor, t: &mut Vec<String>, has_subq: &mut bool) {
    match f {
        TableFactor::Table { name, .. } => {
            if let Some(i) = name.0.last()
                && let Some(ident) = i.as_ident()
            {
                t.push(ident.value.to_lowercase());
            }
        }
        TableFactor::Derived { subquery, .. } => {
            *has_subq = true;
            let mut hj = false;
            extract(subquery, t, &mut hj, has_subq);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            let mut hj = false;
            extract_join_info(table_with_joins, t, &mut hj, has_subq);
        }
        _ => {}
    }
}

// === WHERE Filter Extraction ===

fn extract_where(q: &Query) -> WhereFilter {
    if let SetExpr::Select(s) = q.body.as_ref()
        && let Some(sel) = s.selection.as_ref()
    {
        return parse_expr(sel);
    }
    WhereFilter::None
}

fn parse_expr(e: &Expr) -> WhereFilter {
    match e {
        // col = val
        Expr::BinaryOp { left, op, right } => parse_binop(left, op, right),
        // col IS NULL / IS NOT NULL
        Expr::IsNull(e) => {
            col_name(e).map_or(WhereFilter::Complex, |c| WhereFilter::IsNull { col: c })
        }
        Expr::IsNotNull(e) => {
            col_name(e).map_or(WhereFilter::Complex, |c| WhereFilter::IsNotNull { col: c })
        }
        // col IN (1, 2, 3)
        Expr::InList {
            expr,
            list,
            negated,
        } if !negated => {
            let col = col_name(expr);
            let vals: Option<Vec<_>> = list.iter().map(parse_value).collect();
            match (col, vals) {
                (Some(c), Some(v)) if !v.is_empty() => WhereFilter::In {
                    col: c,
                    vals: v.into_boxed_slice(),
                },
                _ => WhereFilter::Complex,
            }
        }
        // NOT, nested
        Expr::Nested(inner) => parse_expr(inner),
        _ => WhereFilter::Complex,
    }
}

fn parse_binop(left: &Expr, op: &BinaryOperator, right: &Expr) -> WhereFilter {
    match op {
        BinaryOperator::And => {
            let l = parse_expr(left);
            let r = parse_expr(right);
            match (&l, &r) {
                (WhereFilter::Complex, _) | (_, WhereFilter::Complex) => WhereFilter::Complex,
                (WhereFilter::And(a), WhereFilter::And(b)) => {
                    let mut v = Vec::with_capacity(a.len() + b.len());
                    v.extend_from_slice(a);
                    v.extend_from_slice(b);
                    WhereFilter::And(v.into_boxed_slice())
                }
                (WhereFilter::And(a), _) => {
                    let mut v = Vec::with_capacity(a.len() + 1);
                    v.extend_from_slice(a);
                    v.push(r);
                    WhereFilter::And(v.into_boxed_slice())
                }
                (_, WhereFilter::And(b)) => {
                    let mut v = Vec::with_capacity(1 + b.len());
                    v.push(l);
                    v.extend_from_slice(b);
                    WhereFilter::And(v.into_boxed_slice())
                }
                _ => WhereFilter::And(Box::new([l, r])),
            }
        }
        BinaryOperator::Or => {
            let l = parse_expr(left);
            let r = parse_expr(right);
            if matches!(l, WhereFilter::Complex) || matches!(r, WhereFilter::Complex) {
                return WhereFilter::Complex;
            }
            WhereFilter::Or(Box::new([l, r]))
        }
        BinaryOperator::Eq => cmp_filter(left, right, |c, v| WhereFilter::Eq { col: c, val: v }),
        BinaryOperator::NotEq => cmp_filter(left, right, |c, v| WhereFilter::Ne { col: c, val: v }),
        BinaryOperator::Gt => cmp_filter(left, right, |c, v| WhereFilter::Gt { col: c, val: v }),
        BinaryOperator::GtEq => cmp_filter(left, right, |c, v| WhereFilter::Gte { col: c, val: v }),
        BinaryOperator::Lt => cmp_filter(left, right, |c, v| WhereFilter::Lt { col: c, val: v }),
        BinaryOperator::LtEq => cmp_filter(left, right, |c, v| WhereFilter::Lte { col: c, val: v }),
        _ => WhereFilter::Complex,
    }
}

fn cmp_filter<F>(left: &Expr, right: &Expr, f: F) -> WhereFilter
where
    F: FnOnce(Box<str>, FilterValue) -> WhereFilter,
{
    // Try col op val
    if let (Some(c), Some(v)) = (col_name(left), parse_value(right)) {
        return f(c, v);
    }
    // Try val op col (reversed)
    if let (Some(v), Some(c)) = (parse_value(left), col_name(right)) {
        return f(c, v);
    }
    WhereFilter::Complex
}

fn col_name(e: &Expr) -> Option<Box<str>> {
    match e {
        Expr::Identifier(id) => Some(id.value.to_lowercase().into_boxed_str()),
        Expr::CompoundIdentifier(ids) => {
            ids.last().map(|i| i.value.to_lowercase().into_boxed_str())
        }
        _ => None,
    }
}

fn parse_value(e: &Expr) -> Option<FilterValue> {
    match e {
        Expr::Value(v) => match &v.value {
            sqlparser::ast::Value::Null => Some(FilterValue::Null),
            sqlparser::ast::Value::Boolean(b) => Some(FilterValue::Bool(*b)),
            sqlparser::ast::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Some(FilterValue::Int(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Some(FilterValue::Float(f))
                } else {
                    None
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s) => {
                Some(FilterValue::Str(s.clone().into_boxed_str()))
            }
            _ => None,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            if let Some(FilterValue::Int(i)) = parse_value(expr) {
                Some(FilterValue::Int(-i))
            } else if let Some(FilterValue::Float(f)) = parse_value(expr) {
                Some(FilterValue::Float(-f))
            } else {
                None
            }
        }
        _ => None,
    }
}

// === WHERE Filter Evaluation ===

impl WhereFilter {
    /// Evaluate filter against a JSON row
    #[allow(dead_code)]
    pub fn eval(&self, row: &Value) -> EvalResult {
        match self {
            WhereFilter::None => EvalResult::Match,
            WhereFilter::Complex => EvalResult::Unknown,

            WhereFilter::Eq { col, val } => match row.get(col.as_ref()) {
                Some(v) => {
                    if val.matches(v) {
                        EvalResult::Match
                    } else {
                        EvalResult::NoMatch
                    }
                }
                None => EvalResult::Unknown,
            },
            WhereFilter::Ne { col, val } => match row.get(col.as_ref()) {
                Some(v) => {
                    if val.matches(v) {
                        EvalResult::NoMatch
                    } else {
                        EvalResult::Match
                    }
                }
                None => EvalResult::Unknown,
            },
            WhereFilter::Gt { col, val } => {
                cmp_eval(row, col.as_ref(), val, |o| o == std::cmp::Ordering::Greater)
            }
            WhereFilter::Gte { col, val } => {
                cmp_eval(row, col.as_ref(), val, |o| o != std::cmp::Ordering::Less)
            }
            WhereFilter::Lt { col, val } => {
                cmp_eval(row, col.as_ref(), val, |o| o == std::cmp::Ordering::Less)
            }
            WhereFilter::Lte { col, val } => {
                cmp_eval(row, col.as_ref(), val, |o| o != std::cmp::Ordering::Greater)
            }

            WhereFilter::In { col, vals } => match row.get(col.as_ref()) {
                Some(v) => {
                    if vals.iter().any(|fv| fv.matches(v)) {
                        EvalResult::Match
                    } else {
                        EvalResult::NoMatch
                    }
                }
                None => EvalResult::Unknown,
            },
            WhereFilter::IsNull { col } => match row.get(col.as_ref()) {
                Some(v) => {
                    if v.is_null() {
                        EvalResult::Match
                    } else {
                        EvalResult::NoMatch
                    }
                }
                None => EvalResult::Unknown,
            },
            WhereFilter::IsNotNull { col } => match row.get(col.as_ref()) {
                Some(v) => {
                    if v.is_null() {
                        EvalResult::NoMatch
                    } else {
                        EvalResult::Match
                    }
                }
                None => EvalResult::Unknown,
            },

            WhereFilter::And(filters) => {
                let mut has_unknown = false;
                for f in filters {
                    match f.eval(row) {
                        EvalResult::NoMatch => return EvalResult::NoMatch,
                        EvalResult::Unknown => has_unknown = true,
                        EvalResult::Match => {}
                    }
                }
                if has_unknown {
                    EvalResult::Unknown
                } else {
                    EvalResult::Match
                }
            }
            WhereFilter::Or(filters) => {
                let mut has_unknown = false;
                for f in filters {
                    match f.eval(row) {
                        EvalResult::Match => return EvalResult::Match,
                        EvalResult::Unknown => has_unknown = true,
                        EvalResult::NoMatch => {}
                    }
                }
                if has_unknown {
                    EvalResult::Unknown
                } else {
                    EvalResult::NoMatch
                }
            }
        }
    }

    /// Evaluate filter against typed row
    pub fn eval_row(&self, row: &RowData) -> EvalResult {
        match self {
            WhereFilter::None => EvalResult::Match,
            WhereFilter::Complex => EvalResult::Unknown,

            WhereFilter::Eq { col, val } => eq_row_eval(row, col.as_ref(), val, false),
            WhereFilter::Ne { col, val } => eq_row_eval(row, col.as_ref(), val, true),
            WhereFilter::Gt { col, val } => {
                cmp_row_eval(row, col.as_ref(), val, |o| o == std::cmp::Ordering::Greater)
            }
            WhereFilter::Gte { col, val } => {
                cmp_row_eval(row, col.as_ref(), val, |o| o != std::cmp::Ordering::Less)
            }
            WhereFilter::Lt { col, val } => {
                cmp_row_eval(row, col.as_ref(), val, |o| o == std::cmp::Ordering::Less)
            }
            WhereFilter::Lte { col, val } => {
                cmp_row_eval(row, col.as_ref(), val, |o| o != std::cmp::Ordering::Greater)
            }

            WhereFilter::In { col, vals } => match row.get(col.as_ref()) {
                Some(v) => {
                    if vals.iter().any(|fv| eq_row_value(v, fv).unwrap_or(false)) {
                        EvalResult::Match
                    } else {
                        EvalResult::NoMatch
                    }
                }
                None => EvalResult::Unknown,
            },
            WhereFilter::IsNull { col } => match row.get(col.as_ref()) {
                Some(RowValue::Null) => EvalResult::Match,
                Some(_) => EvalResult::NoMatch,
                None => EvalResult::Unknown,
            },
            WhereFilter::IsNotNull { col } => match row.get(col.as_ref()) {
                Some(RowValue::Null) => EvalResult::NoMatch,
                Some(_) => EvalResult::Match,
                None => EvalResult::Unknown,
            },

            WhereFilter::And(filters) => {
                let mut has_unknown = false;
                for f in filters {
                    match f.eval_row(row) {
                        EvalResult::NoMatch => return EvalResult::NoMatch,
                        EvalResult::Unknown => has_unknown = true,
                        EvalResult::Match => {}
                    }
                }
                if has_unknown {
                    EvalResult::Unknown
                } else {
                    EvalResult::Match
                }
            }
            WhereFilter::Or(filters) => {
                let mut has_unknown = false;
                for f in filters {
                    match f.eval_row(row) {
                        EvalResult::Match => return EvalResult::Match,
                        EvalResult::Unknown => has_unknown = true,
                        EvalResult::NoMatch => {}
                    }
                }
                if has_unknown {
                    EvalResult::Unknown
                } else {
                    EvalResult::NoMatch
                }
            }
        }
    }
}

#[inline(always)]
fn eq_row_eval(row: &RowData, col: &str, val: &FilterValue, neg: bool) -> EvalResult {
    match row.get(col) {
        Some(v) => match eq_row_value(v, val) {
            Some(m) => {
                if (m && !neg) || (!m && neg) {
                    EvalResult::Match
                } else {
                    EvalResult::NoMatch
                }
            }
            None => EvalResult::Unknown,
        },
        None => EvalResult::Unknown,
    }
}

#[inline(always)]
fn cmp_row_eval<F>(row: &RowData, col: &str, val: &FilterValue, pred: F) -> EvalResult
where
    F: FnOnce(std::cmp::Ordering) -> bool,
{
    match row.get(col) {
        Some(v) => match cmp_row_value(v, val) {
            Some(ord) => {
                if pred(ord) {
                    EvalResult::Match
                } else {
                    EvalResult::NoMatch
                }
            }
            None => EvalResult::Unknown,
        },
        None => EvalResult::Unknown,
    }
}

#[inline(always)]
fn eq_row_value(v: &RowValue, val: &FilterValue) -> Option<bool> {
    match (v, val) {
        (RowValue::Null, FilterValue::Null) => Some(true),
        (RowValue::Bool(a), FilterValue::Bool(b)) => Some(a == b),
        (RowValue::Int(a), FilterValue::Int(b)) => Some(a == b),
        (RowValue::Float(a), FilterValue::Float(b)) => Some(a == b),
        (RowValue::Int(a), FilterValue::Float(b)) => Some((*a as f64) == *b),
        (RowValue::Float(a), FilterValue::Int(b)) => Some(*a == *b as f64),
        (RowValue::Str(a), FilterValue::Str(b)) => Some(a.as_ref() == b.as_ref()),
        (RowValue::Str(a), FilterValue::Int(b)) => a.as_ref().parse::<i64>().ok().map(|i| i == *b),
        (RowValue::Int(a), FilterValue::Str(b)) => Some(a.to_string() == b.as_ref()),
        _ => None,
    }
}

#[inline(always)]
fn cmp_row_value(v: &RowValue, val: &FilterValue) -> Option<std::cmp::Ordering> {
    match (v, val) {
        (RowValue::Int(a), FilterValue::Int(b)) => Some(a.cmp(b)),
        (RowValue::Float(a), FilterValue::Float(b)) => a.partial_cmp(b),
        (RowValue::Int(a), FilterValue::Float(b)) => (*a as f64).partial_cmp(b),
        (RowValue::Float(a), FilterValue::Int(b)) => a.partial_cmp(&(*b as f64)),
        (RowValue::Str(a), FilterValue::Str(b)) => Some(a.as_ref().cmp(b.as_ref())),
        _ => None,
    }
}

#[allow(dead_code)]
fn cmp_eval<F>(row: &Value, col: &str, val: &FilterValue, pred: F) -> EvalResult
where
    F: FnOnce(std::cmp::Ordering) -> bool,
{
    match row.get(col) {
        Some(v) => match val.cmp_json(v) {
            Some(ord) => {
                if pred(ord) {
                    EvalResult::Match
                } else {
                    EvalResult::NoMatch
                }
            }
            None => EvalResult::Unknown,
        },
        None => EvalResult::Unknown,
    }
}

impl FilterValue {
    #[inline]
    #[allow(dead_code)]
    fn matches(&self, v: &Value) -> bool {
        match (self, v) {
            (FilterValue::Null, Value::Null) => true,
            (FilterValue::Bool(a), Value::Bool(b)) => a == b,
            (FilterValue::Int(a), Value::Number(n)) => n.as_i64() == Some(*a),
            (FilterValue::Float(a), Value::Number(n)) => n.as_f64() == Some(*a),
            (FilterValue::Str(a), Value::String(b)) => a.as_ref() == b,
            // Handle string comparison with int (e.g., id stored as string)
            (FilterValue::Int(a), Value::String(s)) => s.parse::<i64>().ok() == Some(*a),
            (FilterValue::Str(a), Value::Number(n)) => n.to_string() == a.as_ref(),
            _ => false,
        }
    }

    #[inline]
    #[allow(dead_code)]
    fn cmp_json(&self, v: &Value) -> Option<std::cmp::Ordering> {
        match (self, v) {
            (FilterValue::Int(a), Value::Number(n)) => n.as_i64().map(|b| b.cmp(a)),
            (FilterValue::Float(a), Value::Number(n)) => n.as_f64().and_then(|b| b.partial_cmp(a)),
            (FilterValue::Str(a), Value::String(b)) => Some(b.as_str().cmp(a.as_ref())),
            _ => None,
        }
    }
}

/// Fast hash - normalize whitespace and case
#[inline]
fn hash(s: &str) -> u64 {
    let mut h = FxHasher::default();
    let mut prev_space = true;
    for c in s.bytes() {
        if c.is_ascii_whitespace() {
            if !prev_space {
                b' '.hash(&mut h);
                prev_space = true;
            }
        } else {
            c.to_ascii_lowercase().hash(&mut h);
            prev_space = false;
        }
    }
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_simple_select() {
        let a = analyze("SELECT * FROM users");
        assert!(a.is_valid);
        assert_eq!(a.tables, vec!["users"]);
        assert!(a.is_simple);
        assert!(matches!(a.filter, WhereFilter::None));
    }

    #[test]
    fn test_join() {
        let a = analyze("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");
        assert!(a.is_valid);
        assert_eq!(a.tables.len(), 2);
        assert!(!a.is_simple); // JOIN = not simple
    }

    #[test]
    fn test_schema_qualified() {
        let a = analyze("SELECT * FROM public.users");
        assert!(a.is_valid);
        assert_eq!(a.tables, vec!["users"]);
    }

    #[test]
    fn test_where_eq() {
        let a = analyze("SELECT * FROM users WHERE id = 5");
        assert!(a.is_simple);
        assert!(
            matches!(&a.filter, WhereFilter::Eq { col, val: FilterValue::Int(5) } if col.as_ref() == "id")
        );

        assert_eq!(a.filter.eval(&json!({"id": 5})), EvalResult::Match);
        assert_eq!(a.filter.eval(&json!({"id": 3})), EvalResult::NoMatch);
        assert_eq!(a.filter.eval(&json!({"name": "x"})), EvalResult::Unknown);
    }

    #[test]
    fn test_where_string() {
        let a = analyze("SELECT * FROM users WHERE status = 'active'");
        assert!(a.is_simple);

        assert_eq!(
            a.filter.eval(&json!({"status": "active"})),
            EvalResult::Match
        );
        assert_eq!(
            a.filter.eval(&json!({"status": "inactive"})),
            EvalResult::NoMatch
        );
    }

    #[test]
    fn test_where_and() {
        let a = analyze("SELECT * FROM orders WHERE user_id = 1 AND status = 'pending'");
        assert!(a.is_simple);
        assert!(matches!(a.filter, WhereFilter::And(_)));

        assert_eq!(
            a.filter.eval(&json!({"user_id": 1, "status": "pending"})),
            EvalResult::Match
        );
        assert_eq!(
            a.filter.eval(&json!({"user_id": 1, "status": "done"})),
            EvalResult::NoMatch
        );
        assert_eq!(
            a.filter.eval(&json!({"user_id": 2, "status": "pending"})),
            EvalResult::NoMatch
        );
    }

    #[test]
    fn test_where_or() {
        let a = analyze("SELECT * FROM users WHERE role = 'admin' OR role = 'moderator'");
        assert!(a.is_simple);

        assert_eq!(a.filter.eval(&json!({"role": "admin"})), EvalResult::Match);
        assert_eq!(
            a.filter.eval(&json!({"role": "moderator"})),
            EvalResult::Match
        );
        assert_eq!(a.filter.eval(&json!({"role": "user"})), EvalResult::NoMatch);
    }

    #[test]
    fn test_where_in() {
        let a = analyze("SELECT * FROM users WHERE id IN (1, 2, 3)");
        assert!(a.is_simple);

        assert_eq!(a.filter.eval(&json!({"id": 1})), EvalResult::Match);
        assert_eq!(a.filter.eval(&json!({"id": 2})), EvalResult::Match);
        assert_eq!(a.filter.eval(&json!({"id": 5})), EvalResult::NoMatch);
    }

    #[test]
    fn test_where_comparison() {
        let a = analyze("SELECT * FROM users WHERE age > 18");
        assert!(a.is_simple);

        assert_eq!(a.filter.eval(&json!({"age": 25})), EvalResult::Match);
        assert_eq!(a.filter.eval(&json!({"age": 18})), EvalResult::NoMatch);
        assert_eq!(a.filter.eval(&json!({"age": 10})), EvalResult::NoMatch);
    }

    #[test]
    fn test_where_is_null() {
        let a = analyze("SELECT * FROM users WHERE deleted_at IS NULL");
        assert!(a.is_simple);

        assert_eq!(
            a.filter.eval(&json!({"deleted_at": null})),
            EvalResult::Match
        );
        assert_eq!(
            a.filter.eval(&json!({"deleted_at": "2024-01-01"})),
            EvalResult::NoMatch
        );
    }

    #[test]
    fn test_where_complex() {
        // Subquery = complex
        let a = analyze("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)");
        assert!(!a.is_simple);
        assert!(matches!(a.filter, WhereFilter::Complex));
        assert_eq!(a.filter.eval(&json!({"id": 1})), EvalResult::Unknown);
    }
}
