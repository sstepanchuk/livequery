//! pgoutput binary protocol decoder - optimized for minimal allocations

use crate::core::row::{RowData, RowValue};
use crate::infra::intern_col_name;
use rustc_hash::FxHashMap;
use std::sync::Arc;

/// Column metadata (interned name + type OID for parsing)
#[derive(Clone)]
pub struct ColMeta {
    pub name: Arc<str>,
    pub oid: u32,
}

/// Decoded WAL change - simplified enum for hot path
#[derive(Debug)]
pub enum WalChange {
    Begin,
    Commit,
    Insert { rel: u32, row: RowData },
    Update { rel: u32, row: RowData },
    Delete { rel: u32 },
    Truncate { rels: Vec<u32> },
    Other,
}

/// Relation cache entry
struct RelCache {
    table: Arc<str>,
    cols: Arc<[ColMeta]>,
    /// Pre-computed column names for RowData creation (avoids re-cloning per row)
    col_names: Arc<[Arc<str>]>,
}

/// pgoutput decoder with relation cache
pub struct PgOutputDecoder {
    rels: FxHashMap<u32, RelCache>,
}

impl PgOutputDecoder {
    #[inline]
    pub fn new() -> Self {
        Self {
            rels: FxHashMap::default(),
        }
    }

    #[inline]
    pub fn get_table(&self, rel: u32) -> Option<Arc<str>> {
        self.rels.get(&rel).map(|r| r.table.clone())
    }

    /// Decode pgoutput binary message - optimized hot path
    #[inline]
    pub fn decode(&mut self, data: &[u8]) -> Option<WalChange> {
        let b = data.first()?;
        match b {
            b'B' => (data.len() >= 21).then_some(WalChange::Begin),
            b'C' => (data.len() >= 26).then_some(WalChange::Commit),
            b'R' => self.parse_relation(data),
            b'I' => self.parse_insert(data),
            b'U' => self.parse_update(data),
            b'D' => self.parse_delete(data),
            b'T' => self.parse_truncate(data),
            _ => Some(WalChange::Other),
        }
    }

    fn parse_relation(&mut self, data: &[u8]) -> Option<WalChange> {
        let mut p = 1;
        let rel_id = read_u32(data, &mut p)?;

        // Skip schema (null-terminated)
        skip_cstr(data, &mut p)?;

        // Table name
        let table = read_cstr(data, &mut p)?;
        let table_arc = intern_col_name(&table.to_ascii_lowercase());

        // Skip replica identity
        p += 1;
        if p + 2 > data.len() {
            return None;
        }

        let n_cols = read_u16(data, &mut p)? as usize;
        let mut cols = Vec::with_capacity(n_cols);

        for _ in 0..n_cols {
            p += 1; // flags
            let name = read_cstr(data, &mut p)?;
            let oid = read_u32(data, &mut p)?;
            p += 4; // skip type_modifier
            cols.push(ColMeta {
                name: intern_col_name(&name.to_ascii_lowercase()),
                oid,
            });
        }

        let col_names: Arc<[Arc<str>]> =
            Arc::from(cols.iter().map(|c| c.name.clone()).collect::<Vec<_>>());
        self.rels.insert(
            rel_id,
            RelCache {
                table: table_arc,
                cols: Arc::from(cols),
                col_names,
            },
        );
        Some(WalChange::Other)
    }

    fn parse_insert(&self, data: &[u8]) -> Option<WalChange> {
        let mut p = 1;
        let rel = read_u32(data, &mut p)?;
        if data.get(p)? != &b'N' {
            return None;
        }
        p += 1;
        let cache = self.rels.get(&rel)?;
        let row = parse_tuple(data, &mut p, &cache.cols, &cache.col_names)?;
        Some(WalChange::Insert { rel, row })
    }

    fn parse_update(&self, data: &[u8]) -> Option<WalChange> {
        let mut p = 1;
        let rel = read_u32(data, &mut p)?;
        let cache = self.rels.get(&rel)?;

        // Skip old tuple if present
        if matches!(data.get(p), Some(b'K') | Some(b'O')) {
            p += 1;
            skip_tuple(data, &mut p)?;
        }

        if data.get(p)? != &b'N' {
            return None;
        }
        p += 1;
        let row = parse_tuple(data, &mut p, &cache.cols, &cache.col_names)?;
        Some(WalChange::Update { rel, row })
    }

    fn parse_delete(&self, data: &[u8]) -> Option<WalChange> {
        let mut p = 1;
        let rel = read_u32(data, &mut p)?;
        // We don't need old row data, just trigger requery
        Some(WalChange::Delete { rel })
    }

    fn parse_truncate(&self, data: &[u8]) -> Option<WalChange> {
        let mut p = 1;
        let n = read_u32(data, &mut p)? as usize;
        p += 1; // options
        let mut rels = Vec::with_capacity(n);
        for _ in 0..n {
            rels.push(read_u32(data, &mut p)?);
        }
        Some(WalChange::Truncate { rels })
    }
}

// ============ Optimized binary helpers ============

#[inline(always)]
fn read_u32(data: &[u8], p: &mut usize) -> Option<u32> {
    let v = u32::from_be_bytes(data.get(*p..*p + 4)?.try_into().ok()?);
    *p += 4;
    Some(v)
}

#[inline(always)]
fn read_u16(data: &[u8], p: &mut usize) -> Option<u16> {
    let v = u16::from_be_bytes(data.get(*p..*p + 2)?.try_into().ok()?);
    *p += 2;
    Some(v)
}

#[inline(always)]
fn read_cstr<'a>(data: &'a [u8], p: &mut usize) -> Option<&'a str> {
    let end = data[*p..].iter().position(|&b| b == 0)? + *p;
    let s = std::str::from_utf8(&data[*p..end]).ok()?;
    *p = end + 1;
    Some(s)
}

#[inline(always)]
fn skip_cstr(data: &[u8], p: &mut usize) -> Option<()> {
    let end = data[*p..].iter().position(|&b| b == 0)? + *p;
    *p = end + 1;
    Some(())
}

/// Parse tuple into RowData - uses pre-computed col_names for zero-copy
#[inline]
fn parse_tuple(
    data: &[u8],
    p: &mut usize,
    cols: &[ColMeta],
    col_names: &Arc<[Arc<str>]>,
) -> Option<RowData> {
    let n = read_u16(data, p)? as usize;
    let mut vals: Vec<RowValue> = Vec::with_capacity(n);

    for i in 0..n {
        let col = cols.get(i)?;

        match *data.get(*p)? {
            b'n' | b'u' => {
                *p += 1;
                vals.push(RowValue::Null);
            }
            b't' => {
                *p += 1;
                let len = read_u32(data, p)? as usize;
                let text = std::str::from_utf8(data.get(*p..*p + len)?).ok()?;
                *p += len;
                vals.push(parse_val(text, col.oid));
            }
            b'b' => {
                *p += 1;
                let len = read_u32(data, p)? as usize;
                vals.push(RowValue::Bytes(data.get(*p..*p + len)?.to_vec()));
                *p += len;
            }
            _ => return None,
        }
    }
    // Use pre-computed col_names - just clone the Arc (cheap)
    Some(RowData::new(col_names.clone(), vals))
}

/// Skip tuple without parsing (for old row in update)
#[inline]
fn skip_tuple(data: &[u8], p: &mut usize) -> Option<()> {
    let n = read_u16(data, p)? as usize;
    for _ in 0..n {
        match *data.get(*p)? {
            b'n' | b'u' => {
                *p += 1;
            }
            b't' | b'b' => {
                *p += 1;
                let len = read_u32(data, p)? as usize;
                *p += len;
            }
            _ => return None,
        }
    }
    Some(())
}

/// Parse value by OID - optimized with fast paths
#[inline(always)]
fn parse_val(s: &str, oid: u32) -> RowValue {
    match oid {
        16 => match s.as_bytes().first() {
            Some(b't') => RowValue::Bool(true),
            Some(b'f') => RowValue::Bool(false),
            _ => RowValue::intern_str(s),
        },
        20 | 21 | 23 => s
            .parse()
            .map(RowValue::Int)
            .unwrap_or_else(|_| RowValue::intern_str(s)),
        700 | 701 => s
            .parse()
            .map(RowValue::Float)
            .unwrap_or_else(|_| RowValue::intern_str(s)),
        114 | 3802 => serde_json::from_str(s)
            .map(RowValue::Json)
            .unwrap_or_else(|_| RowValue::intern_str(s)),
        _ => RowValue::intern_str(s),
    }
}

impl Default for PgOutputDecoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_begin() {
        let mut decoder = PgOutputDecoder::new();
        let mut data = vec![b'B'];
        data.extend_from_slice(&100u64.to_be_bytes());
        data.extend_from_slice(&1234567890i64.to_be_bytes());
        data.extend_from_slice(&42u32.to_be_bytes());
        assert!(matches!(decoder.decode(&data), Some(WalChange::Begin)));
    }

    #[test]
    fn test_parse_val() {
        assert!(matches!(parse_val("t", 16), RowValue::Bool(true)));
        assert!(matches!(parse_val("f", 16), RowValue::Bool(false)));
        assert!(matches!(parse_val("42", 23), RowValue::Int(42)));
        assert!(matches!(parse_val("3.14", 701), RowValue::Float(_)));
    }
}
