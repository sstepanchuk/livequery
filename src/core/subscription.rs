//! Subscription Manager - Client-provided IDs with shared query optimization

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use parking_lot::RwLock;
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet, FxHasher};
use smallvec::SmallVec;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

use crate::core::event::{ts_millis, EventBatch, SubscribeEvent, SubscriptionMode};
use crate::core::query::{self, WhereFilter};
use crate::core::row::RowData;

type Map<K, V> = DashMap<K, V, FxBuildHasher>;

/// Manages subscriptions and shared queries
pub struct SubscriptionManager {
    /// subscription_id → Subscription
    subs: Map<Arc<str>, Subscription>,
    /// query_hash → SharedQuery  
    queries: Map<Arc<str>, SharedQuery>,
    /// table → [query_hash]
    table_idx: Map<Arc<str>, FxHashSet<Arc<str>>>,
    max_subs: usize,
    subs_count: AtomicUsize,
}

/// Individual subscription (client-provided ID)
pub struct Subscription {
    pub id: Arc<str>,
    pub query_id: Arc<str>,
    pub mode: SubscriptionMode,
    pub last_activity: RwLock<Instant>,
}

/// Shared query state (optimized - one per unique query)
pub struct SharedQuery {
    pub query: Arc<str>,
    pub cols: Option<Arc<[Arc<str>]>>,
    pub tables: Arc<[String]>,
    pub filter: WhereFilter,
    pub is_simple: bool,
    pub snap: RwLock<Snapshot>,
    /// Sequence counter for events
    pub seq: AtomicU64,
    /// Atomic refcount for safe concurrent removal
    pub refcount: AtomicUsize,
    /// subscription_ids using this query (for iteration only)
    pub subscribers: RwLock<FxHashSet<Arc<str>>>,
}

#[derive(Default)]
pub struct Snapshot {
    rows: FxHashMap<u64, RowEntry>,
}

#[derive(Clone)]
struct RowEntry {
    hash: u64,
    data: Arc<serde_json::Value>,
}

/// Returns (identity_hash, content_hash). When cols is None, both are the same value (computed once).
#[inline(always)]
fn row_hashes(r: &RowData, cols: &Option<Arc<[Arc<str>]>>) -> (u64, u64) {
    let content = r.hash_content();
    match cols {
        None => (content, content), // Same hash, computed once
        Some(c) => {
            let mut h = FxHasher::default();
            if c.len() == 1 {
                if let Some(v) = r.get(&c[0]) {
                    v.hash_into(&mut h);
                }
            } else {
                for col in c.iter() {
                    if let Some(v) = r.get(col) {
                        v.hash_into(&mut h);
                    }
                }
            }
            (h.finish(), content)
        }
    }
}

pub struct SubscribeResult {
    pub subscription_id: Arc<str>,
    pub query_id: Arc<str>,
    pub is_new_query: bool,
    pub seq: u64,
}

impl SubscriptionManager {
    pub fn new(max_subs: usize) -> Self {
        Self {
            subs: Map::with_hasher(FxBuildHasher),
            queries: Map::with_hasher(FxBuildHasher),
            table_idx: Map::with_hasher(FxBuildHasher),
            max_subs,
            subs_count: AtomicUsize::new(0),
        }
    }

    /// Subscribe with client-provided subscription_id (atomic)
    pub fn subscribe(
        &self,
        sub_id: &str,
        q: &str,
        cols: Option<Vec<String>>,
        mode: SubscriptionMode,
    ) -> Result<SubscribeResult, String> {
        // Atomic check-and-insert for subscription
        let sub_id: Arc<str> = sub_id.into();
        match self.subs.entry(sub_id.clone()) {
            Entry::Occupied(_) => Err(format!("Subscription '{}' already exists", sub_id)),
            Entry::Vacant(entry) => {
                if self.subs_count.load(Relaxed) >= self.max_subs {
                    return Err("Max subscriptions reached".into());
                }

                // Analyze query
                let a = query::analyze(q);
                if !a.is_valid {
                    return Err(a.error.unwrap_or_else(|| "Invalid query".into()));
                }
                if a.tables.is_empty() {
                    return Err("No table in query".into());
                }

                let query_id: Arc<str> = qhash(q).into();

                // Atomic get-or-create SharedQuery
                let is_new_query = match self.queries.entry(query_id.clone()) {
                    Entry::Occupied(qe) => {
                        // Existing query - increment refcount atomically
                        qe.get().refcount.fetch_add(1, Relaxed);
                        qe.get().subscribers.write().insert(sub_id.clone());
                        info!("+Sub [{}] → Q{}", sub_id, &query_id[..8]);
                        false
                    }
                    Entry::Vacant(qe) => {
                        // New query
                        let cols_arc = cols.map(|c| {
                            Arc::from(
                                c.into_iter()
                                    .map(|s| Arc::<str>::from(s.as_str()))
                                    .collect::<Vec<_>>()
                                    .into_boxed_slice(),
                            )
                        });

                        // Index by tables
                        for t in &a.tables {
                            self.table_idx
                                .entry(Arc::<str>::from(t.as_str()))
                                .or_default()
                                .insert(query_id.clone());
                        }

                        let mut subscribers = FxHashSet::default();
                        subscribers.insert(sub_id.clone());

                        qe.insert(SharedQuery {
                            query: q.into(),
                            cols: cols_arc,
                            tables: Arc::from(a.tables.into_boxed_slice()),
                            filter: a.filter,
                            is_simple: a.is_simple,
                            snap: RwLock::new(Snapshot::new()),
                            seq: AtomicU64::new(0),
                            refcount: AtomicUsize::new(1),
                            subscribers: RwLock::new(subscribers),
                        });
                        info!("New query Q{} + sub [{}]", &query_id[..8], sub_id);
                        true
                    }
                };

                // Create subscription (already in vacant entry)
                entry.insert(Subscription {
                    id: sub_id.clone(),
                    query_id: query_id.clone(),
                    mode,
                    last_activity: RwLock::new(Instant::now()),
                });
                self.subs_count.fetch_add(1, Relaxed);

                // Avoid double lookup: seq is 0 for new queries, fetch from existing
                let seq = if is_new_query {
                    0
                } else {
                    self.queries
                        .get(&query_id)
                        .map(|q| q.seq.load(Relaxed))
                        .unwrap_or(0)
                };

                Ok(SubscribeResult {
                    subscription_id: sub_id,
                    query_id,
                    is_new_query,
                    seq,
                })
            }
        }
    }

    /// Unsubscribe by subscription_id (atomic with refcount)
    pub fn unsubscribe(&self, sub_id: &str) -> bool {
        let Some((_, sub)) = self.subs.remove(sub_id) else {
            return false;
        };
        self.subs_count.fetch_sub(1, Relaxed);
        info!("-Sub [{}]", sub_id);

        // Decrement refcount and remove query if zero
        if let Some(sq) = self.queries.get(&sub.query_id) {
            sq.subscribers.write().remove(sub_id);
            // Atomic decrement - only remove if we hit zero
            if sq.refcount.fetch_sub(1, Relaxed) == 1 {
                drop(sq);
                self.remove_query(&sub.query_id);
            }
        }
        true
    }

    fn remove_query(&self, query_id: &str) {
        // Double-check refcount is still zero before removal (handles race)
        if let Entry::Occupied(e) = self.queries.entry(query_id.into()) {
            if e.get().refcount.load(Relaxed) == 0 {
                let sq = e.remove();
                for t in sq.tables.iter() {
                    if let Some(mut x) = self.table_idx.get_mut(t.as_str()) {
                        x.remove(query_id);
                    }
                }
                info!("~Query Q{}", &query_id[..8]);
            }
        }
    }

    #[inline]
    pub fn heartbeat(&self, sub_id: &str) -> bool {
        self.subs
            .get(sub_id)
            .map(|s| *s.last_activity.write() = Instant::now())
            .is_some()
    }

    /// Cleanup stale subscriptions
    pub fn cleanup(&self, timeout: Duration) -> Vec<Arc<str>> {
        let now = Instant::now();
        let mut stale: SmallVec<[Arc<str>; 16]> = SmallVec::new();

        for sub in self.subs.iter() {
            if now.duration_since(*sub.last_activity.read()) >= timeout {
                stale.push(sub.id.clone());
            }
        }

        for sub_id in &stale {
            warn!("Timeout [{}]", sub_id);
            self.unsubscribe(sub_id);
        }

        stale.into_vec()
    }

    // === Getters ===

    #[inline]
    pub fn get_sub(
        &self,
        sub_id: &str,
    ) -> Option<dashmap::mapref::one::Ref<'_, Arc<str>, Subscription>> {
        self.subs.get(sub_id)
    }

    #[inline]
    pub fn get_query(
        &self,
        query_id: &str,
    ) -> Option<dashmap::mapref::one::Ref<'_, Arc<str>, SharedQuery>> {
        self.queries.get(query_id)
    }

    /// Check if any query uses this table
    #[inline]
    pub fn has_table(&self, t: &str) -> bool {
        self.table_idx.contains_key(t)
    }

    /// Iterate query_ids for a table
    #[inline]
    pub fn for_table_queries<F>(&self, t: &str, mut f: F)
    where
        F: FnMut(&Arc<str>),
    {
        if let Some(qids) = self.table_idx.get(t) {
            for qid in qids.iter() {
                f(qid);
            }
        }
    }

    /// Stats: (subscriptions, queries)
    #[inline]
    pub fn stats(&self) -> (usize, usize) {
        (self.subs_count.load(Relaxed), self.queries.len())
    }
}

impl SharedQuery {
    /// Create batch from events, incrementing sequence
    #[inline]
    pub fn make_batch(&self, ev: Vec<SubscribeEvent>) -> Option<EventBatch> {
        if ev.is_empty() {
            return None;
        }
        let seq = self.seq.fetch_add(1, Relaxed) + 1;
        Some(EventBatch::new(seq, ev))
    }
}

impl Snapshot {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Initialize with typed rows - returns events for Events mode
    pub fn init_rows(
        &mut self,
        rows: Vec<RowData>,
        cols: &Option<Arc<[Arc<str>]>>,
    ) -> Vec<SubscribeEvent> {
        let t = ts();
        let len = rows.len();
        let mut ev = Vec::with_capacity(len);
        self.rows.clear();
        self.rows.reserve(len);
        for row in rows {
            let (id_hash, content_hash) = row_hashes(&row, cols);
            let val = Arc::new(row.to_value());
            ev.push(SubscribeEvent::insert_arc(t, val.clone()));
            self.rows.insert(
                id_hash,
                RowEntry {
                    hash: content_hash,
                    data: val,
                },
            );
        }
        ev
    }

    /// Initialize with typed rows - returns row data for Snapshot mode
    pub fn init_rows_snapshot(
        &mut self,
        rows: Vec<RowData>,
        cols: &Option<Arc<[Arc<str>]>>,
    ) -> Vec<Arc<serde_json::Value>> {
        let len = rows.len();
        self.rows.clear();
        self.rows.reserve(len);
        let mut out = Vec::with_capacity(len);
        for row in rows {
            let (id_hash, content_hash) = row_hashes(&row, cols);
            let val = Arc::new(row.to_value());
            out.push(val.clone());
            self.rows.insert(
                id_hash,
                RowEntry {
                    hash: content_hash,
                    data: val,
                },
            );
        }
        out
    }

    /// Get all current rows as Arc<Value> for Snapshot mode publish
    #[inline]
    pub fn get_all_rows(&self) -> Vec<Arc<serde_json::Value>> {
        self.rows.values().map(|e| e.data.clone()).collect()
    }

    #[inline]
    pub fn diff_rows(
        &mut self,
        rows: Vec<RowData>,
        cols: &Option<Arc<[Arc<str>]>>,
    ) -> Vec<SubscribeEvent> {
        let t = ts();
        let mut old = std::mem::take(&mut self.rows);
        let mut new: FxHashMap<u64, RowEntry> =
            FxHashMap::with_capacity_and_hasher(rows.len(), Default::default());

        // Pre-size for typical 5% change rate
        let est = (old.len().max(rows.len()) / 20).max(4);
        let mut ev = Vec::with_capacity(est);

        for row in rows {
            let (id_hash, content_hash) = row_hashes(&row, cols);
            if let Some(prev) = old.remove(&id_hash) {
                // Fast path: content hash match means unchanged
                if prev.hash == content_hash {
                    new.insert(
                        id_hash,
                        RowEntry {
                            hash: content_hash,
                            data: prev.data,
                        },
                    );
                } else {
                    ev.push(SubscribeEvent::delete_arc(t, prev.data));
                    let val = Arc::new(row.to_value());
                    ev.push(SubscribeEvent::insert_arc(t, val.clone()));
                    new.insert(
                        id_hash,
                        RowEntry {
                            hash: content_hash,
                            data: val,
                        },
                    );
                }
            } else {
                let val = Arc::new(row.to_value());
                ev.push(SubscribeEvent::insert_arc(t, val.clone()));
                new.insert(
                    id_hash,
                    RowEntry {
                        hash: content_hash,
                        data: val,
                    },
                );
            }
        }

        // Remaining are deletes
        ev.reserve(old.len());
        for (_, old_row) in old {
            ev.push(SubscribeEvent::delete_arc(t, old_row.data));
        }
        self.rows = new;
        ev
    }
}

#[inline(always)]
fn ts() -> i64 {
    ts_millis() as i64
}

const HEX: &[u8; 16] = b"0123456789abcdef";

#[inline]
fn qhash(q: &str) -> String {
    let mut h = FxHasher::default();
    let mut sp = true;
    for c in q.bytes() {
        if c.is_ascii_whitespace() {
            if !sp {
                b' '.hash(&mut h);
                sp = true;
            }
        } else {
            c.to_ascii_lowercase().hash(&mut h);
            sp = false;
        }
    }
    // Avoid format! allocation - use lookup table
    let n = h.finish();
    let mut s = String::with_capacity(16);
    for i in (0..16).rev() {
        s.push(HEX[((n >> (i * 4)) & 0xF) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_query() {
        let m = SubscriptionManager::new(1000);
        let r1 = m
            .subscribe(
                "sub-1",
                "SELECT * FROM users",
                None,
                SubscriptionMode::Events,
            )
            .unwrap();
        assert!(r1.is_new_query);
        let r2 = m
            .subscribe(
                "sub-2",
                "SELECT * FROM users",
                None,
                SubscriptionMode::Events,
            )
            .unwrap();
        assert!(!r2.is_new_query); // Same query, not new
        assert_eq!(r1.query_id, r2.query_id); // Same query_id
        assert_ne!(r1.subscription_id, r2.subscription_id); // Different sub_ids
        assert_eq!(m.stats(), (2, 1)); // 2 subs, 1 query
    }

    #[test]
    fn test_unsubscribe() {
        let m = SubscriptionManager::new(1000);
        m.subscribe(
            "sub-1",
            "SELECT * FROM users",
            None,
            SubscriptionMode::Events,
        )
        .unwrap();
        m.subscribe(
            "sub-2",
            "SELECT * FROM users",
            None,
            SubscriptionMode::Events,
        )
        .unwrap();
        assert_eq!(m.stats(), (2, 1));

        m.unsubscribe("sub-1");
        assert_eq!(m.stats(), (1, 1)); // Query still exists

        m.unsubscribe("sub-2");
        assert_eq!(m.stats(), (0, 0)); // Query removed
    }

    #[test]
    fn test_qhash() {
        assert_eq!(qhash("SELECT * FROM users"), qhash("select * from users"));
        assert_eq!(
            qhash("SELECT  *  FROM  users"),
            qhash("SELECT * FROM users")
        );
    }
}
