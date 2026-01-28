use dashmap::DashMap;
use rustc_hash::{FxBuildHasher, FxHashMap, FxHasher};
use serde_json::{Map, Number, Value};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock, OnceLock};

const INTERN_MAX_LEN: usize = 32;
static STR_INTERN: LazyLock<DashMap<String, Arc<str>, FxBuildHasher>> =
    LazyLock::new(|| DashMap::with_hasher(FxBuildHasher));

#[derive(Debug, Clone, PartialEq)]
pub enum RowValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(Arc<str>),
    Bytes(Vec<u8>),
    Json(Value),
    Array(Vec<RowValue>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RowData {
    cols: Arc<[Arc<str>]>,
    values: Vec<RowValue>,
    // Cached column index for O(1) lookup (built lazily on first get)
    col_idx: OnceLock<Arc<FxHashMap<Arc<str>, usize>>>,
}

impl RowData {
    pub fn new(cols: Arc<[Arc<str>]>, values: Vec<RowValue>) -> Self {
        Self {
            cols,
            values,
            col_idx: OnceLock::new(),
        }
    }

    /// Create with pre-built index for faster lookups
    pub fn new_indexed(cols: Arc<[Arc<str>]>, values: Vec<RowValue>) -> Self {
        let idx = build_col_index(&cols);
        let col_idx = OnceLock::new();
        let _ = col_idx.set(Arc::new(idx));
        Self {
            cols,
            values,
            col_idx,
        }
    }

    /// Create RowData from JSON Value (for benchmarks and testing)
    #[allow(dead_code)] // Used in benchmarks
    pub fn from_value(v: &Value) -> Self {
        let obj = v.as_object();
        let (cols, values): (Vec<Arc<str>>, Vec<RowValue>) = match obj {
            Some(m) => m
                .iter()
                .map(|(k, v)| (Arc::<str>::from(k.as_str()), RowValue::from_value(v)))
                .unzip(),
            None => (vec![], vec![]),
        };
        Self::new(Arc::from(cols.into_boxed_slice()), values)
    }

    /// Create indexed RowData from JSON Value
    #[allow(dead_code)]
    pub fn from_value_indexed(v: &Value) -> Self {
        let obj = v.as_object();
        let (cols, values): (Vec<Arc<str>>, Vec<RowValue>) = match obj {
            Some(m) => m
                .iter()
                .map(|(k, v)| (Arc::<str>::from(k.as_str()), RowValue::from_value(v)))
                .unzip(),
            None => (vec![], vec![]),
        };
        Self::new_indexed(Arc::from(cols.into_boxed_slice()), values)
    }

    /// O(1) lookup with cached index, O(n) fallback for small rows
    #[inline]
    pub fn get(&self, name: &str) -> Option<&RowValue> {
        // Fast path: use cached index if available
        if let Some(idx) = self.col_idx.get() {
            return idx.get(name).and_then(|&i| self.values.get(i));
        }
        // For small rows (<=8 cols), linear search is faster than building/using hash index
        if self.cols.len() <= 8 {
            return self
                .cols
                .iter()
                .position(|c| c.as_ref() == name)
                .and_then(|i| self.values.get(i));
        }
        // Build index once for wider rows
        let idx = self
            .col_idx
            .get_or_init(|| Arc::new(build_col_index(&self.cols)));
        idx.get(name).and_then(|&i| self.values.get(i))
    }

    #[inline]
    pub fn hash_content(&self) -> u64 {
        let mut h = FxHasher::default();
        // Hash column count as discriminator (columns are same for query results)
        self.cols.len().hash(&mut h);
        for v in self.values.iter() {
            v.hash_into(&mut h);
        }
        h.finish()
    }

    #[inline]
    pub fn to_value(&self) -> Value {
        let mut map = Map::with_capacity(self.cols.len());
        for (k, v) in self.cols.iter().zip(self.values.iter()) {
            map.insert(k.to_string(), v.to_value());
        }
        Value::Object(map)
    }
}

impl RowValue {
    /// Create RowValue from JSON Value
    pub fn from_value(v: &Value) -> RowValue {
        match v {
            Value::Null => RowValue::Null,
            Value::Bool(b) => RowValue::Bool(*b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    RowValue::Int(i)
                } else if let Some(f) = n.as_f64() {
                    RowValue::Float(f)
                } else {
                    RowValue::intern_str(&n.to_string())
                }
            }
            Value::String(s) => RowValue::intern_str(s),
            Value::Array(a) => RowValue::Array(a.iter().map(RowValue::from_value).collect()),
            Value::Object(_) => RowValue::Json(v.clone()),
        }
    }

    #[inline(always)]
    pub fn intern_str(s: &str) -> RowValue {
        if s.len() <= INTERN_MAX_LEN {
            let arc = STR_INTERN
                .entry(s.to_string())
                .or_insert_with(|| Arc::from(s))
                .clone();
            RowValue::Str(arc)
        } else {
            RowValue::Str(Arc::from(s))
        }
    }

    #[inline(always)]
    pub fn hash_into(&self, h: &mut FxHasher) {
        match self {
            RowValue::Null => 0u8.hash(h),
            RowValue::Bool(b) => {
                1u8.hash(h);
                b.hash(h);
            }
            RowValue::Int(i) => {
                2u8.hash(h);
                i.hash(h);
            }
            RowValue::Float(f) => {
                3u8.hash(h);
                f.to_bits().hash(h);
            }
            RowValue::Str(s) => {
                4u8.hash(h);
                s.hash(h);
            }
            RowValue::Bytes(b) => {
                5u8.hash(h);
                b.hash(h);
            }
            RowValue::Json(v) => {
                6u8.hash(h);
                let mut hv = FxHasher::default();
                hash_value(v, &mut hv);
                hv.finish().hash(h);
            }
            RowValue::Array(a) => {
                7u8.hash(h);
                for v in a {
                    v.hash_into(h);
                }
            }
        }
    }

    #[inline]
    pub fn to_value(&self) -> Value {
        match self {
            RowValue::Null => Value::Null,
            RowValue::Bool(b) => Value::Bool(*b),
            RowValue::Int(i) => Value::Number(Number::from(*i)),
            RowValue::Float(f) => Number::from_f64(*f)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            RowValue::Str(s) => Value::String(s.to_string()),
            RowValue::Bytes(b) => Value::String(hex_bytes(b)),
            RowValue::Json(v) => v.clone(),
            RowValue::Array(a) => {
                let mut out = Vec::with_capacity(a.len());
                for v in a {
                    out.push(v.to_value());
                }
                Value::Array(out)
            }
        }
    }
}

#[inline]
fn hex_bytes(b: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(2 + b.len() * 2);
    s.push_str("\\x");
    for &byte in b {
        s.push(HEX[(byte >> 4) as usize] as char);
        s.push(HEX[(byte & 0xF) as usize] as char);
    }
    s
}

/// Build column name -> index map for O(1) lookup
#[inline]
fn build_col_index(cols: &[Arc<str>]) -> FxHashMap<Arc<str>, usize> {
    let mut map = FxHashMap::with_capacity_and_hasher(cols.len(), Default::default());
    for (i, c) in cols.iter().enumerate() {
        map.insert(c.clone(), i);
    }
    map
}

#[inline]
fn hash_value(v: &Value, h: &mut FxHasher) {
    match v {
        Value::Null => 0u8.hash(h),
        Value::Bool(b) => {
            1u8.hash(h);
            b.hash(h);
        }
        Value::Number(n) => {
            2u8.hash(h);
            if let Some(i) = n.as_i64() {
                i.hash(h)
            } else if let Some(u) = n.as_u64() {
                u.hash(h)
            } else if let Some(f) = n.as_f64() {
                f.to_bits().hash(h)
            }
        }
        Value::String(s) => {
            3u8.hash(h);
            s.hash(h);
        }
        Value::Array(a) => {
            4u8.hash(h);
            for x in a {
                hash_value(x, h);
            }
        }
        Value::Object(m) => {
            5u8.hash(h);
            for (k, v) in m {
                k.hash(h);
                hash_value(v, h);
            }
        }
    }
}
