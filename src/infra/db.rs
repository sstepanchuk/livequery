//! Database Pool with Metrics

use anyhow::{Context, Result};
use deadpool_postgres::{Config, ManagerConfig, Pool, PoolConfig, RecyclingMethod, Runtime};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_postgres::{types::Type, NoTls, Row};
use tracing::{debug, warn};

use crate::core::config;
use crate::core::row::{RowData, RowValue};

// Column name interning for reuse across queries
static COL_NAMES: std::sync::LazyLock<
    dashmap::DashMap<String, Arc<str>, rustc_hash::FxBuildHasher>,
> = std::sync::LazyLock::new(|| dashmap::DashMap::with_hasher(rustc_hash::FxBuildHasher));

/// Intern column name for reuse across queries (public for wal.rs)
#[inline]
pub fn intern_col_name(name: &str) -> Arc<str> {
    if let Some(arc) = COL_NAMES.get(name) {
        return arc.clone();
    }
    let arc: Arc<str> = Arc::from(name);
    COL_NAMES.insert(name.to_string(), arc.clone());
    arc
}

/// Database pool with query metrics
pub struct DbPool {
    pool: Pool,
    queries: AtomicU64,
    errors: AtomicU64,
    total_ms: AtomicU64,
}

#[inline(always)]
fn row_to_typed(row: &Row, cols: Arc<[Arc<str>]>, col_types: &[Type], use_index: bool) -> RowData {
    let mut values = Vec::with_capacity(cols.len());
    for (i, col_type) in col_types.iter().enumerate() {
        let v = match *col_type {
            Type::INT2 => row
                .try_get::<_, Option<i16>>(i)
                .ok()
                .flatten()
                .map(|n| RowValue::Int(n as i64)),
            Type::INT4 => row
                .try_get::<_, Option<i32>>(i)
                .ok()
                .flatten()
                .map(|n| RowValue::Int(n as i64)),
            Type::INT8 => row
                .try_get::<_, Option<i64>>(i)
                .ok()
                .flatten()
                .map(RowValue::Int),

            Type::FLOAT4 => row
                .try_get::<_, Option<f32>>(i)
                .ok()
                .flatten()
                .map(|f| RowValue::Float(f as f64)),
            Type::FLOAT8 => row
                .try_get::<_, Option<f64>>(i)
                .ok()
                .flatten()
                .map(RowValue::Float),

            Type::BOOL => row
                .try_get::<_, Option<bool>>(i)
                .ok()
                .flatten()
                .map(RowValue::Bool),

            Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::NUMERIC => row
                .try_get::<_, Option<String>>(i)
                .ok()
                .flatten()
                .map(|s| RowValue::intern_str(&s)),

            Type::UUID => row
                .try_get::<_, Option<uuid::Uuid>>(i)
                .ok()
                .flatten()
                .map(|u| RowValue::intern_str(&u.to_string())),

            Type::JSON | Type::JSONB => row
                .try_get::<_, Option<Value>>(i)
                .ok()
                .flatten()
                .map(RowValue::Json),

            Type::TIMESTAMP => row
                .try_get::<_, Option<chrono::NaiveDateTime>>(i)
                .ok()
                .flatten()
                .map(|t| RowValue::intern_str(&t.to_string())),
            Type::TIMESTAMPTZ => row
                .try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(i)
                .ok()
                .flatten()
                .map(|t| RowValue::intern_str(&t.to_rfc3339())),
            Type::DATE => row
                .try_get::<_, Option<chrono::NaiveDate>>(i)
                .ok()
                .flatten()
                .map(|d| RowValue::intern_str(&d.to_string())),
            Type::TIME | Type::TIMETZ => row
                .try_get::<_, Option<chrono::NaiveTime>>(i)
                .ok()
                .flatten()
                .map(|t| RowValue::intern_str(&t.to_string())),

            Type::BYTEA => row
                .try_get::<_, Option<Vec<u8>>>(i)
                .ok()
                .flatten()
                .map(RowValue::Bytes),

            Type::BOOL_ARRAY => row
                .try_get::<_, Option<Vec<bool>>>(i)
                .ok()
                .flatten()
                .map(|a| {
                    let mut out = Vec::with_capacity(a.len());
                    for v in a {
                        out.push(RowValue::Bool(v));
                    }
                    RowValue::Array(out)
                }),
            Type::INT4_ARRAY => row
                .try_get::<_, Option<Vec<i32>>>(i)
                .ok()
                .flatten()
                .map(|a| {
                    let mut out = Vec::with_capacity(a.len());
                    for n in a {
                        out.push(RowValue::Int(n as i64));
                    }
                    RowValue::Array(out)
                }),
            Type::INT8_ARRAY => row
                .try_get::<_, Option<Vec<i64>>>(i)
                .ok()
                .flatten()
                .map(|a| {
                    let mut out = Vec::with_capacity(a.len());
                    for n in a {
                        out.push(RowValue::Int(n));
                    }
                    RowValue::Array(out)
                }),
            Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => row
                .try_get::<_, Option<Vec<String>>>(i)
                .ok()
                .flatten()
                .map(|a| {
                    let mut out = Vec::with_capacity(a.len());
                    for s in a {
                        out.push(RowValue::intern_str(&s));
                    }
                    RowValue::Array(out)
                }),

            _ => row
                .try_get::<_, Option<String>>(i)
                .ok()
                .flatten()
                .map(|s| RowValue::intern_str(&s)),
        };
        values.push(v.unwrap_or(RowValue::Null));
    }
    // Use indexed RowData for rows with many columns (O(1) lookup vs O(n))
    if use_index {
        RowData::new_indexed(cols, values)
    } else {
        RowData::new(cols, values)
    }
}

impl DbPool {
    pub fn new(cfg: &config::Config) -> Result<Self> {
        let mut c = Config::new();
        c.url = Some(cfg.db_url.clone());
        c.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        c.pool = Some(PoolConfig {
            max_size: cfg.db_pool_size as usize,
            timeouts: deadpool_postgres::Timeouts {
                wait: Some(cfg.db_timeout()),
                create: Some(cfg.db_timeout()),
                recycle: Some(Duration::from_secs(5)),
            },
            ..Default::default()
        });
        Ok(Self {
            pool: c.create_pool(Some(Runtime::Tokio1), NoTls)?,
            queries: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            total_ms: AtomicU64::new(0),
        })
    }

    /// Query and return typed rows (no serde_json conversion until needed)
    pub async fn query_rows_typed(&self, q: &str) -> Result<Vec<RowData>> {
        let start = Instant::now();
        let c = self.pool.get().await.context("pool exhausted")?;
        let result = c.query(q, &[]).await;
        let ms = start.elapsed().as_millis() as u64;
        self.total_ms.fetch_add(ms, Relaxed);
        self.queries.fetch_add(1, Relaxed);

        match result {
            Ok(rows) => {
                if ms > 100 {
                    debug!("Slow query {}ms: {:.60}", ms, q);
                }
                if rows.is_empty() {
                    return Ok(Vec::new());
                }

                let cols_meta = rows[0].columns();
                let cols_arc: Arc<[Arc<str>]> = Arc::from(
                    cols_meta
                        .iter()
                        .map(|c| intern_col_name(c.name()))
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                );
                let col_types: Vec<Type> = cols_meta.iter().map(|c| c.type_().clone()).collect();

                // Use indexed rows if >5 columns for faster WHERE eval
                let use_index = cols_arc.len() > 5;
                let mut out = Vec::with_capacity(rows.len());
                for row in &rows {
                    out.push(row_to_typed(row, cols_arc.clone(), &col_types, use_index));
                }
                Ok(out)
            }
            Err(e) => {
                self.errors.fetch_add(1, Relaxed);
                warn!("Query error: {} - {:.60}", e, q);
                Err(e.into())
            }
        }
    }

    #[inline]
    pub async fn ping(&self) -> Result<()> {
        self.pool.get().await?.simple_query("SELECT 1").await?;
        Ok(())
    }

    /// Pool stats: (active, available, max)
    #[inline]
    pub fn pool_status(&self) -> (usize, usize, usize) {
        let s = self.pool.status();
        (s.size - s.available, s.available, s.max_size)
    }

    /// Query stats: (total, errors, avg_ms)
    #[inline]
    pub fn query_stats(&self) -> (u64, u64, u64) {
        let q = self.queries.load(Relaxed);
        let e = self.errors.load(Relaxed);
        let avg = if q > 0 {
            self.total_ms.load(Relaxed) / q
        } else {
            0
        };
        (q, e, avg)
    }
}
