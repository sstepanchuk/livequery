//! Performance Benchmarks - run with: cargo bench

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use rustc_hash::FxHasher;
use serde_json::{json, Value};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use livequery_server::core::event::SubscribeEvent;
use livequery_server::core::query::analyze;
use livequery_server::core::row::{RowData, RowValue};
use livequery_server::core::subscription::Snapshot;

// === HEX encoding ===

const HEX_TABLE: &[u8; 16] = b"0123456789abcdef";

fn hex_format(b: &[u8]) -> String {
    let mut s = String::with_capacity(2 + b.len() * 2);
    s.push_str("\\x");
    for byte in b { s.push_str(&format!("{:02x}", byte)); }
    s
}

fn hex_lookup(b: &[u8]) -> String {
    let mut s = String::with_capacity(2 + b.len() * 2);
    s.push_str("\\x");
    for &byte in b {
        s.push(HEX_TABLE[(byte >> 4) as usize] as char);
        s.push(HEX_TABLE[(byte & 0xF) as usize] as char);
    }
    s
}

// === RowData helpers ===

fn typed_rows(count: usize) -> Vec<RowData> {
    (0..count)
        .map(|i| {
            RowData::from_value(&json!({
                "id": i,
                "name": format!("User {}", i),
                "active": i % 2 == 0,
                "age": 20 + (i % 50)
            }))
        })
        .collect()
}

fn typed_rows_changed(mut rows: Vec<RowData>, changes: usize) -> Vec<RowData> {
    for i in 0..changes.min(rows.len()) {
        rows[i] = RowData::from_value(&json!({
            "id": i,
            "name": format!("Updated {}", i),
            "active": i % 2 == 0,
            "age": 20 + (i % 50)
        }));
    }
    rows
}

fn rowdata_pair(cols_count: usize) -> (RowData, RowData) {
    let mut cols = Vec::with_capacity(cols_count);
    let mut values = Vec::with_capacity(cols_count);
    for i in 0..cols_count {
        cols.push(Arc::<str>::from(format!("col{i}")));
        values.push(RowValue::Int(i as i64));
    }
    let cols: Arc<[Arc<str>]> = Arc::from(cols.into_boxed_slice());
    let values_indexed = values.clone();
    (RowData::new(cols.clone(), values), RowData::new_indexed(cols, values_indexed))
}

// === Query hash ===

fn qhash_format(q: &str) -> String {
    let mut h = FxHasher::default();
    let mut sp = true;
    for c in q.bytes() {
        if c.is_ascii_whitespace() { if !sp { b' '.hash(&mut h); sp = true; } }
        else { c.to_ascii_lowercase().hash(&mut h); sp = false; }
    }
    format!("{:016x}", h.finish())
}

fn qhash_lookup(q: &str) -> String {
    let mut h = FxHasher::default();
    let mut sp = true;
    for c in q.bytes() {
        if c.is_ascii_whitespace() { if !sp { b' '.hash(&mut h); sp = true; } }
        else { c.to_ascii_lowercase().hash(&mut h); sp = false; }
    }
    let n = h.finish();
    let mut s = String::with_capacity(16);
    for i in (0..16).rev() {
        s.push(HEX_TABLE[((n >> (i * 4)) & 0xF) as usize] as char);
    }
    s
}

// === Row hashing ===

#[inline(always)]
fn hval(v: &Value, h: &mut FxHasher) {
    match v {
        Value::Null => 0u8.hash(h),
        Value::Bool(b) => { 1u8.hash(h); b.hash(h); }
        Value::Number(n) => { 
            2u8.hash(h); 
            if let Some(i) = n.as_i64() { i.hash(h) } 
            else if let Some(u) = n.as_u64() { u.hash(h) } 
            else if let Some(f) = n.as_f64() { f.to_bits().hash(h) } 
        }
        Value::String(s) => { 3u8.hash(h); s.hash(h); }
        Value::Array(a) => { 4u8.hash(h); a.iter().for_each(|x| hval(x, h)); }
        Value::Object(m) => { 5u8.hash(h); m.iter().for_each(|(k, x)| { k.hash(h); hval(x, h); }); }
    }
}

fn hrow_option(r: &Value, cols: &Option<Vec<String>>) -> u64 {
    let mut h = FxHasher::default();
    match cols {
        Some(c) => { for col in c { if let Some(v) = r.get(col) { hval(v, &mut h); } } }
        None => { if let Some(o) = r.as_object() { for (k, v) in o { k.hash(&mut h); hval(v, &mut h); } } }
    }
    h.finish()
}

fn hrow_slice(r: &Value, cols: Option<&[String]>) -> u64 {
    let mut h = FxHasher::default();
    match cols {
        Some(c) => { for col in c { if let Some(v) = r.get(col) { hval(v, &mut h); } } }
        None => { if let Some(o) = r.as_object() { for (k, v) in o { k.hash(&mut h); hval(v, &mut h); } } }
    }
    h.finish()
}

// === WAL parsing ===

fn parse_wal_split(s: &str) -> Option<(String, Value)> {
    let rest = s.strip_prefix("table ")?;
    let (table_part, data_part) = rest.split_once(':')?;
    let table = table_part.rsplit('.').next()?.to_lowercase();
    let data_part = data_part.trim();
    let (_, cols_part) = data_part.split_once(':')?;
    
    let mut obj = serde_json::Map::new();
    for part in cols_part.trim().split_whitespace() {
        if let Some((col_type, val)) = part.split_once("]:") {
            if let Some((col, _)) = col_type.split_once('[') {
                obj.insert(col.to_string(), parse_val(val));
            }
        }
    }
    if obj.is_empty() { return None; }
    Some((table, Value::Object(obj)))
}

fn parse_wal_find(s: &str) -> Option<(String, Value)> {
    let s = s.strip_prefix("table ")?;
    let colon1 = memchr::memchr(b':', s.as_bytes())?;
    let table_part = &s[..colon1];
    let dot = memchr::memrchr(b'.', table_part.as_bytes());
    let table_start = dot.map(|i| i + 1).unwrap_or(0);
    let table: String = table_part[table_start..].to_ascii_lowercase();
    
    let rest = &s[colon1 + 1..];
    let colon2 = memchr::memchr(b':', rest.as_bytes())?;
    let cols_part = &rest[colon2 + 1..];
    
    let mut obj = serde_json::Map::new();
    for part in cols_part.split_ascii_whitespace() {
        if let Some(bracket) = memchr::memchr(b'[', part.as_bytes()) {
            if let Some(close) = part[bracket..].find("]:") {
                let col = &part[..bracket];
                let val = &part[bracket + close + 2..];
                obj.insert(col.to_string(), parse_val(val));
            }
        }
    }
    if obj.is_empty() { return None; }
    Some((table, Value::Object(obj)))
}

fn parse_val(s: &str) -> Value {
    match s {
        "null" => Value::Null,
        "true" | "t" => Value::Bool(true),
        "false" | "f" => Value::Bool(false),
        _ => {
            if let Some(inner) = s.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')) {
                return Value::String(inner.to_string());
            }
            if let Ok(i) = s.parse::<i64>() { return Value::Number(i.into()); }
            Value::String(s.to_string())
        }
    }
}

// === Snapshot diff - measure clone vs swap ===

fn diff_with_clone(old: &mut std::collections::HashMap<u64, Value>, new_rows: Vec<Value>) -> Vec<i8> {
    let mut events = Vec::new();
    let mut new_map: std::collections::HashMap<u64, Value> = std::collections::HashMap::new();
    
    for r in new_rows {
        let h = {
            let mut hasher = FxHasher::default();
            if let Some(o) = r.as_object() { 
                for (k, v) in o { k.hash(&mut hasher); hval(v, &mut hasher); } 
            }
            hasher.finish()
        };
        new_map.insert(h, r);
    }
    
    for (h, _) in old.iter() { if !new_map.contains_key(h) { events.push(-1); } }
    for (h, _) in &new_map {
        if !old.contains_key(h) { events.push(1); }
    }
    *old = new_map;
    events
}

// === Benchmarks ===

fn bench_hex(c: &mut Criterion) {
    let mut group = c.benchmark_group("hex_encode");
    let data: Vec<u8> = (0..100).collect();
    group.throughput(Throughput::Bytes(data.len() as u64));
    
    group.bench_function("format", |b| b.iter(|| hex_format(black_box(&data))));
    group.bench_function("lookup", |b| b.iter(|| hex_lookup(black_box(&data))));
    group.finish();
}

fn bench_snapshot_init(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_init");
    let rows = typed_rows(500);
    let cols = None;

    group.bench_function("init_rows_events", |b| {
        b.iter_batched(
            Snapshot::new,
            |mut snap| snap.init_rows(black_box(rows.clone()), &cols),
            BatchSize::SmallInput,
        )
    });

    group.bench_function("init_rows_snapshot", |b| {
        b.iter_batched(
            Snapshot::new,
            |mut snap| snap.init_rows_snapshot(black_box(rows.clone()), &cols),
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_snapshot_get_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_get_all");
    let rows = typed_rows(500);
    let cols = None;
    let mut snap = Snapshot::new();
    snap.init_rows(rows, &cols);

    group.bench_function("get_all_rows", |b| b.iter(|| snap.get_all_rows()));
    group.finish();
}

fn bench_qhash(c: &mut Criterion) {
    let mut group = c.benchmark_group("qhash");
    let q = "SELECT id, name, email FROM users WHERE status = 'active' AND created_at > '2024-01-01'";
    group.throughput(Throughput::Bytes(q.len() as u64));
    
    group.bench_function("format", |b| b.iter(|| qhash_format(black_box(q))));
    group.bench_function("lookup", |b| b.iter(|| qhash_lookup(black_box(q))));
    group.finish();
}

fn bench_hrow(c: &mut Criterion) {
    let mut group = c.benchmark_group("hrow");
    let row = json!({"id": 12345, "name": "John Doe", "email": "john@example.com", "age": 30, "active": true});
    
    group.bench_function("option_none", |b| {
        let cols: Option<Vec<String>> = None;
        b.iter(|| hrow_option(black_box(&row), black_box(&cols)))
    });
    group.bench_function("slice_none", |b| {
        b.iter(|| hrow_slice(black_box(&row), black_box(None)))
    });
    
    group.bench_function("option_some", |b| {
        let cols = Some(vec!["id".to_string()]);
        b.iter(|| hrow_option(black_box(&row), black_box(&cols)))
    });
    group.bench_function("slice_some", |b| {
        let cols = vec!["id".to_string()];
        b.iter(|| hrow_slice(black_box(&row), black_box(Some(cols.as_slice()))))
    });
    group.finish();
}

fn bench_wal_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_parse");
    let wal = "table public.users: UPDATE: id[integer]:5 name[text]:'John' age[integer]:30 status[text]:'active'";
    group.throughput(Throughput::Bytes(wal.len() as u64));
    
    group.bench_function("split", |b| b.iter(|| parse_wal_split(black_box(wal))));
    group.bench_function("memchr", |b| b.iter(|| parse_wal_find(black_box(wal))));
    group.finish();
}

fn bench_snapshot_diff(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_diff");
    
    // 100 rows, 10% change
    let rows: Vec<Value> = (0..100).map(|i| json!({"id": i, "name": format!("User {}", i)})).collect();
    let mut changed_rows: Vec<Value> = rows.clone();
    for i in 0..10 { changed_rows[i] = json!({"id": i, "name": format!("Updated {}", i)}); }
    
    group.bench_function("100_rows_10pct_change", |b| {
        b.iter_batched(
            || {
                let mut old = std::collections::HashMap::new();
                for r in &rows {
                    let h = {
                        let mut hasher = FxHasher::default();
                        if let Some(o) = r.as_object() { 
                            for (k, v) in o { k.hash(&mut hasher); hval(v, &mut hasher); } 
                        }
                        hasher.finish()
                    };
                    old.insert(h, r.clone());
                }
                old
            },
            |mut old| diff_with_clone(&mut old, black_box(changed_rows.clone())),
            BatchSize::SmallInput
        )
    });
    group.finish();
}

// === New: Query analysis + filter eval ===

fn bench_query_analyze(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_analyze");
    let q = "SELECT id, name FROM users WHERE status = 'active' AND age > 18";
    let q2 = "SELECT id, name FROM users WHERE status = 'inactive' AND age > 18";
    let q3 = "SELECT id, name FROM users WHERE status = 'pending' AND age > 18";

    group.bench_function("cache_hit", |b| b.iter(|| analyze(black_box(q))));
    group.bench_function("cache_miss", |b| {
        let mut idx = 0usize;
        let variants = [q, q2, q3];
        b.iter(|| {
            let q = variants[idx % variants.len()];
            idx = idx.wrapping_add(1);
            analyze(black_box(q))
        })
    });
    group.finish();
}

fn bench_filter_eval(c: &mut Criterion) {
    let mut group = c.benchmark_group("where_eval_row");
    let analysis = analyze("SELECT * FROM users WHERE status = 'active' AND age > 18");
    let row = RowData::from_value(&json!({"id": 1, "status": "active", "age": 25, "name": "Test"}));
    group.bench_function("eval_row", |b| b.iter(|| analysis.filter.eval_row(black_box(&row))));
    group.finish();
}

fn bench_filter_eval_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("where_eval_json");
    let analysis = analyze("SELECT * FROM users WHERE status = 'active' AND age > 18");
    let row = json!({"id": 1, "status": "active", "age": 25, "name": "Test"});
    group.bench_function("eval_json", |b| b.iter(|| analysis.filter.eval(black_box(&row))));
    group.finish();
}

// === New: RowData hashing + to_value ===

fn bench_rowdata(c: &mut Criterion) {
    let mut group = c.benchmark_group("rowdata");
    let row = RowData::from_value(&json!({"id": 1, "name": "Alice", "active": true, "age": 30}));
    let row_value = json!({"id": 1, "name": "Alice", "active": true, "age": 30});

    group.bench_function("hash_content", |b| b.iter(|| row.hash_content()));
    group.bench_function("to_value", |b| b.iter(|| row.to_value()));
    group.bench_function("from_value", |b| b.iter(|| RowData::from_value(black_box(&row_value))));
    group.finish();
}

fn bench_rowdata_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("rowdata_get");
    let (row_linear, row_indexed) = rowdata_pair(12);
    let key = "col10";
    let missing = "missing";

    group.bench_function("linear_hit", |b| b.iter(|| row_linear.get(black_box(key))));
    group.bench_function("linear_miss", |b| b.iter(|| row_linear.get(black_box(missing))));
    group.bench_function("indexed_hit", |b| b.iter(|| row_indexed.get(black_box(key))));
    group.bench_function("indexed_miss", |b| b.iter(|| row_indexed.get(black_box(missing))));
    group.finish();
}

fn bench_rowvalue(c: &mut Criterion) {
    let mut group = c.benchmark_group("rowvalue");
    let json_val = json!({"id": 1, "name": "Alice", "active": true, "age": 30, "tags": ["a", "b"]});
    let short = "short";
    let long = "this_is_a_long_string_value_that_should_skip_intern";
    let row_value = RowValue::from_value(&json_val);

    group.bench_function("from_value", |b| b.iter(|| RowValue::from_value(black_box(&json_val))));
    group.bench_function("from_str_short", |b| b.iter(|| RowValue::from_str(black_box(short))));
    group.bench_function("from_str_long", |b| b.iter(|| RowValue::from_str(black_box(long))));
    group.bench_function("to_value", |b| b.iter(|| row_value.to_value()));
    group.bench_function("hash_into", |b| {
        b.iter(|| {
            let mut h = FxHasher::default();
            row_value.hash_into(&mut h);
            black_box(h.finish())
        })
    });
    group.finish();
}

// === New: Snapshot diff using real Snapshot ===

fn bench_snapshot_diff_typed(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_diff_typed");
    let rows = typed_rows(500);
    let changed = typed_rows_changed(rows.clone(), 50);
    let cols = None;

    group.bench_function("500_rows_10pct_change", |b| {
        b.iter_batched(
            || {
                let mut snap = Snapshot::new();
                snap.init_rows(rows.clone(), &cols);
                snap
            },
            |mut snap| {
                let ev: Vec<SubscribeEvent> = snap.diff_rows(black_box(changed.clone()), &cols);
                black_box(ev)
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_hex,
    bench_qhash,
    bench_hrow,
    bench_wal_parse,
    bench_snapshot_diff,
    bench_query_analyze,
    bench_filter_eval,
    bench_filter_eval_json,
    bench_rowdata,
    bench_rowdata_get,
    bench_rowvalue,
    bench_snapshot_init,
    bench_snapshot_get_all,
    bench_snapshot_diff_typed
);
criterion_main!(benches);
