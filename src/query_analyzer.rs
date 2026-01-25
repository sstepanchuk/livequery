//! SQL Query Analyzer using sqlparser-rs AST.

use serde::{Deserialize, Serialize};
use sqlparser::ast::*;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryAnalysis {
    pub query: String,
    pub is_valid: bool,
    pub ivm_compatible: bool,
    pub incompatibility_reason: Option<String>,
    pub referenced_tables: Vec<TableReference>,
    pub has_join: bool,
    pub join_types: Vec<String>,
    pub has_group_by: bool,
    pub has_aggregation: bool,
    pub aggregation_functions: Vec<String>,
    pub has_window_functions: bool,
    pub has_subqueries: bool,
    pub has_cte: bool,
    pub has_distinct: bool,
    pub has_order_by: bool,
    pub has_limit: bool,
    pub select_columns: Vec<String>,
    pub complexity_score: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableReference {
    pub schema: Option<String>,
    pub table: String,
    pub alias: Option<String>,
}

pub fn analyze_query(query: &str) -> QueryAnalysis {
    let mut a = QueryAnalysis { query: query.into(), ..Default::default() };
    
    let stmts = match Parser::parse_sql(&PostgreSqlDialect {}, query) {
        Ok(s) if !s.is_empty() => s,
        Ok(_) => { a.incompatibility_reason = Some("Empty query".into()); return a; }
        Err(e) => { a.incompatibility_reason = Some(format!("Parse error: {}", e)); return a; }
    };
    
    let query_ast = match &stmts[0] {
        Statement::Query(q) => q,
        _ => { a.incompatibility_reason = Some("Only SELECT queries supported".into()); return a; }
    };
    
    a.is_valid = true;
    analyze_query_ast(&mut a, query_ast);
    
    // IVM compatibility check
    let mut reasons: Vec<&str> = Vec::new();
    if a.has_window_functions { reasons.push("WINDOW functions not supported"); }
    if a.join_types.iter().any(|t| matches!(t.as_str(), "LEFT" | "RIGHT" | "FULL")) {
        reasons.push("OUTER JOINs not fully supported");
    }
    if a.has_subqueries { reasons.push("Subqueries have limited support"); }
    if a.has_cte { reasons.push("CTEs have limited support"); }
    
    a.ivm_compatible = reasons.is_empty();
    if !reasons.is_empty() { a.incompatibility_reason = Some(reasons.join("; ")); }
    a.complexity_score = calculate_complexity(&a);
    a
}

fn analyze_query_ast(a: &mut QueryAnalysis, query: &Query) {
    if let Some(with) = &query.with {
        a.has_cte = true;
        with.cte_tables.iter().for_each(|cte| analyze_query_ast(a, &cte.query));
    }
    a.has_order_by = query.order_by.is_some();
    a.has_limit = query.limit.is_some();
    analyze_set_expr(a, &query.body);
}

fn analyze_set_expr(a: &mut QueryAnalysis, expr: &SetExpr) {
    match expr {
        SetExpr::Select(s) => analyze_select(a, s),
        SetExpr::Query(q) => { a.has_subqueries = true; analyze_query_ast(a, q); }
        SetExpr::SetOperation { left, right, .. } => {
            analyze_set_expr(a, left);
            analyze_set_expr(a, right);
        }
        _ => {}
    }
}

fn analyze_select(a: &mut QueryAnalysis, select: &Select) {
    a.has_distinct = select.distinct.is_some();
    
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                analyze_expr(a, e);
                if let Expr::Identifier(id) = e { a.select_columns.push(id.value.clone()); }
            }
            SelectItem::Wildcard(_) => a.select_columns.push("*".into()),
            SelectItem::QualifiedWildcard(n, _) => a.select_columns.push(format!("{}.*", n)),
        }
    }
    
    select.from.iter().for_each(|t| analyze_table_with_joins(a, t));
    
    a.has_group_by = match &select.group_by {
        GroupByExpr::All(_) => true,
        GroupByExpr::Expressions(e, _) => !e.is_empty(),
    };
    
    if let Some(w) = &select.selection { analyze_expr(a, w); }
    if let Some(h) = &select.having { analyze_expr(a, h); }
}

fn analyze_table_with_joins(a: &mut QueryAnalysis, twj: &TableWithJoins) {
    analyze_table_factor(a, &twj.relation);
    
    for join in &twj.joins {
        a.has_join = true;
        let jt = match &join.join_operator {
            JoinOperator::Inner(_) => "INNER",
            JoinOperator::LeftOuter(_) | JoinOperator::LeftSemi(_) | JoinOperator::LeftAnti(_) => "LEFT",
            JoinOperator::RightOuter(_) | JoinOperator::RightSemi(_) | JoinOperator::RightAnti(_) => "RIGHT",
            JoinOperator::FullOuter(_) => "FULL",
            JoinOperator::CrossJoin => "CROSS",
            JoinOperator::CrossApply | JoinOperator::OuterApply => "APPLY",
            _ => "INNER",
        };
        if !a.join_types.contains(&jt.to_string()) { a.join_types.push(jt.into()); }
        analyze_table_factor(a, &join.relation);
    }
}

fn analyze_table_factor(a: &mut QueryAnalysis, tf: &TableFactor) {
    match tf {
        TableFactor::Table { name, alias, .. } => {
            let parts: Vec<_> = name.0.iter().map(|i| i.value.as_str()).collect();
            let (schema, table) = if parts.len() >= 2 {
                (Some(parts[parts.len() - 2].into()), parts.last().unwrap().to_string())
            } else {
                (None, parts.last().unwrap_or(&"").to_string())
            };
            a.referenced_tables.push(TableReference {
                schema, table, alias: alias.as_ref().map(|x| x.name.value.clone()),
            });
        }
        TableFactor::Derived { subquery, alias, .. } => {
            a.has_subqueries = true;
            analyze_query_ast(a, subquery);
            if let Some(al) = alias {
                a.referenced_tables.push(TableReference {
                    schema: None, table: "(subquery)".into(), alias: Some(al.name.value.clone()),
                });
            }
        }
        TableFactor::NestedJoin { table_with_joins, .. } => analyze_table_with_joins(a, table_with_joins),
        _ => {}
    }
}

const AGG_FUNCS: &[&str] = &[
    "COUNT", "SUM", "AVG", "MIN", "MAX", "ARRAY_AGG", "STRING_AGG",
    "BOOL_AND", "BOOL_OR", "BIT_AND", "BIT_OR", "EVERY", "JSONB_AGG",
    "JSON_AGG", "XMLAGG", "STDDEV", "VARIANCE", "COVAR_POP", "COVAR_SAMP",
    "CORR", "REGR_SLOPE", "PERCENTILE_CONT", "PERCENTILE_DISC", "MODE",
];

fn analyze_expr(a: &mut QueryAnalysis, expr: &Expr) {
    match expr {
        Expr::Function(f) => {
            let name = f.name.to_string().to_uppercase();
            if f.over.is_some() { a.has_window_functions = true; }
            if AGG_FUNCS.iter().any(|&af| name.starts_with(af)) {
                a.has_aggregation = true;
                if !a.aggregation_functions.contains(&name) { a.aggregation_functions.push(name); }
            }
            if let FunctionArguments::List(args) = &f.args {
                for arg in &args.args {
                    let e = match arg {
                        FunctionArg::Named { arg, .. } => arg,
                        FunctionArg::Unnamed(a) => a,
                    };
                    if let FunctionArgExpr::Expr(ex) = e { analyze_expr(a, ex); }
                }
            }
        }
        Expr::Subquery(q) | Expr::InSubquery { subquery: q, .. } | Expr::Exists { subquery: q, .. } => {
            a.has_subqueries = true;
            analyze_query_ast(a, q);
        }
        Expr::BinaryOp { left, right, .. } => { analyze_expr(a, left); analyze_expr(a, right); }
        Expr::UnaryOp { expr: e, .. } | Expr::Nested(e) | Expr::Cast { expr: e, .. } => analyze_expr(a, e),
        Expr::Case { operand, conditions, results, else_result, .. } => {
            operand.iter().for_each(|o| analyze_expr(a, o));
            conditions.iter().for_each(|c| analyze_expr(a, c));
            results.iter().for_each(|r| analyze_expr(a, r));
            else_result.iter().for_each(|e| analyze_expr(a, e));
        }
        _ => {}
    }
}

fn calculate_complexity(a: &QueryAnalysis) -> u32 {
    let mut s = 10u32;
    s += a.referenced_tables.len() as u32 * 10;
    if a.has_join { s += 15 + a.join_types.len() as u32 * 5; }
    if a.has_aggregation { s += 10 + a.aggregation_functions.len() as u32 * 5; }
    if a.has_group_by { s += 10; }
    if a.has_window_functions { s += 25; }
    if a.has_subqueries { s += 20; }
    if a.has_cte { s += 15; }
    s.min(100)
}


#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_simple_select() {
        let analysis = analyze_query("SELECT * FROM users");
        assert!(analysis.is_valid);
        assert!(!analysis.has_join);
        assert!(!analysis.has_aggregation);
    }
    
    #[test]
    fn test_join_detection() {
        let analysis = analyze_query(
            "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id"
        );
        assert!(analysis.has_join);
        assert!(analysis.join_types.contains(&"INNER".to_string()));
    }
    
    #[test]
    fn test_aggregation_detection() {
        let analysis = analyze_query(
            "SELECT user_id, COUNT(*), SUM(amount) FROM orders GROUP BY user_id"
        );
        assert!(analysis.has_aggregation);
        assert!(analysis.has_group_by);
        assert!(analysis.aggregation_functions.contains(&"COUNT".to_string()));
        assert!(analysis.aggregation_functions.contains(&"SUM".to_string()));
    }
    
    #[test]
    fn test_window_function_detection() {
        let analysis = analyze_query(
            "SELECT user_id, ROW_NUMBER() OVER (PARTITION BY user_id) FROM orders"
        );
        assert!(analysis.has_window_functions);
        assert!(!analysis.ivm_compatible);
    }
}
