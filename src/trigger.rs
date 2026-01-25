//! Shared trigger implementation - captures table changes and fans out to subscribers.

use pgrx::prelude::*;
use pgrx::heap_tuple::PgHeapTuple;
use pgrx::trigger_support::{PgTrigger, PgTriggerError, PgTriggerOperation};
use pgrx::WhoAllocated;

use crate::event::SubscribeEvent;
use crate::shmem;
use crate::streaming::get_current_timestamp;
use crate::types::tuple_attr_to_json;

const SHARED_TRIGGER_PREFIX: &str = "_pgsub_shared_";

#[inline]
pub fn get_shared_trigger_name(table_name: &str) -> String {
    format!("{}{}", SHARED_TRIGGER_PREFIX, 
        table_name.replace(".", "_").replace("\"", "").replace(" ", "_"))
}

#[inline]
fn send_notify(channel: &str, event: &SubscribeEvent) {
    let payload = serde_json::to_string(event).unwrap_or_default();
    let payload = if payload.len() > 7900 {
        format!("{{\"truncated\":true,\"mz_timestamp\":{},\"mz_diff\":{}}}", 
                event.mz_timestamp, event.mz_diff)
    } else { payload };
    let _ = Spi::run(&format!("SELECT pg_notify('{}', '{}')", channel, payload.replace("'", "''")));
}


#[inline]
fn heap_tuple_to_json<'a, A: WhoAllocated>(tuple: &PgHeapTuple<'a, A>, relation: &pgrx::PgRelation) -> serde_json::Value {
    let tupdesc = relation.tuple_desc();
    let mut map = serde_json::Map::new();
    for i in 0..tupdesc.len() {
        if let Some(attr) = tupdesc.get(i) {
            if !attr.is_dropped() {
                map.insert(attr.name().to_string(), tuple_attr_to_json(tuple, i + 1, attr.type_oid().value()));
            }
        }
    }
    serde_json::Value::Object(map)
}

/// Broadcast event to all interested slots
#[inline]
fn broadcast_event(event: &SubscribeEvent, interested_slots: &[usize]) {
    for &slot_index in interested_slots {
        let _ = shmem::push_event(slot_index, event);
        if let Some(info) = shmem::get_slot_info(slot_index) {
            send_notify(&format!("pgsub_{}", info.get_subscription_id().replace("-", "")), event);
        }
    }
}

/// Install a SHARED trigger on a table (idempotent)
/// 
/// This creates ONE trigger per table that calls pg_subscribe_shared_trigger.
/// The trigger fans out events to ALL subscriptions interested in this table.
/// 
/// Returns Ok(true) if trigger was newly installed, Ok(false) if already existed.
pub fn install_shared_trigger(table_name: &str) -> Result<bool, String> {
    // Check if trigger already installed (in shared memory registry)
    if shmem::is_trigger_installed(table_name) {
        return Ok(false);
    }
    
    let trigger_name = get_shared_trigger_name(table_name);
    
    let create_result = Spi::connect(|mut client| {
        // First check if trigger exists in database (might exist from previous session)
        let check_sql = format!(
            r#"SELECT 1 FROM pg_trigger t
               JOIN pg_class c ON t.tgrelid = c.oid
               WHERE t.tgname = '{}'
               LIMIT 1"#,
            trigger_name
        );
        
        if let Ok(result) = client.select(&check_sql, None, None) {
            if result.len() > 0 {
                // Trigger exists, mark as installed and return
                shmem::mark_trigger_installed(table_name);
                return Ok(false);
            }
        }
        
        // Create the shared trigger
        // Pass table_name as argument so trigger knows which table changed
        let trigger_sql = format!(
            r#"CREATE TRIGGER "{}"
            AFTER INSERT OR UPDATE OR DELETE ON {}
            FOR EACH ROW
            EXECUTE FUNCTION pg_subscribe_shared_trigger('{}')
            "#,
            trigger_name, table_name, table_name
        );
        
        match client.update(&trigger_sql, None, None) {
            Ok(_) => {
                pgrx::info!("pg_subscribe: Installed shared trigger {} on {}", trigger_name, table_name);
                Ok(true)
            }
            Err(e) => Err(format!("Failed to create shared trigger: {:?}", e)),
        }
    })?;
    
    // Mark trigger as installed in shared memory
    shmem::mark_trigger_installed(table_name);
    
    Ok(create_result)
}

/// Register a subscription's interest in a table and install shared trigger if needed
pub fn register_subscription_for_table(
    table_name: &str,
    slot_index: usize,
) -> Result<(), String> {
    // Register interest in shared memory
    let needs_trigger = shmem::register_table_interest(table_name, slot_index);
    
    // Install shared trigger if this is a new table
    if needs_trigger {
        install_shared_trigger(table_name)?;
    }
    
    Ok(())
}


/// Remove the shared trigger from a table
pub fn remove_shared_trigger(table_name: &str) -> Result<(), String> {
    let trigger_name = get_shared_trigger_name(table_name);
    
    Spi::connect(|mut client| {
        let drop_sql = format!(
            r#"DROP TRIGGER IF EXISTS "{}" ON {} CASCADE"#,
            trigger_name, table_name
        );
        
        match client.update(&drop_sql, None, None) {
            Ok(_) => {
                pgrx::info!("pg_subscribe: Removed shared trigger {} from {}", trigger_name, table_name);
                Ok(())
            }
            Err(e) => {
                pgrx::warning!("Failed to drop shared trigger: {:?}", e);
                Ok(()) // Don't fail on drop errors
            }
        }
    })
}

#[pg_trigger]
fn pg_subscribe_shared_trigger<'a>(
    trigger: &'a PgTrigger<'a>,
) -> Result<Option<PgHeapTuple<'a, impl WhoAllocated>>, PgTriggerError> {
    let args = trigger.extra_args().unwrap_or_default();
    let table_name = if args.is_empty() {
        let rel = trigger.relation()?;
        let (schema, table) = (rel.namespace(), rel.name());
        if schema == "public" { table.to_string() } else { format!("{}.{}", schema, table) }
    } else { args[0].clone() };
    
    let slots = shmem::get_interested_slots_for_table(&table_name);
    if slots.is_empty() { return Ok(trigger.new()); }
    
    let ts = get_current_timestamp();
    let rel = trigger.relation()?;
    
    match trigger.op()? {
        PgTriggerOperation::Insert => {
            if let Some(t) = trigger.new() {
                broadcast_event(&SubscribeEvent::insert(ts, heap_tuple_to_json(&t, &rel)), &slots);
            }
        }
        PgTriggerOperation::Delete => {
            if let Some(t) = trigger.old() {
                broadcast_event(&SubscribeEvent::delete(ts, heap_tuple_to_json(&t, &rel)), &slots);
            }
        }
        PgTriggerOperation::Update => {
            if let Some(old) = trigger.old() {
                broadcast_event(&SubscribeEvent::delete(ts, heap_tuple_to_json(&old, &rel)), &slots);
            }
            if let Some(new) = trigger.new() {
                broadcast_event(&SubscribeEvent::insert(ts, heap_tuple_to_json(&new, &rel)), &slots);
            }
        }
        PgTriggerOperation::Truncate => {}
    }
    Ok(trigger.new())
}

/// Cleanup all shared trigger registrations for a slot
pub fn cleanup_shared_triggers_for_slot(slot_index: usize) -> Result<(), String> {
    let tables_to_cleanup = shmem::unregister_slot_from_all_tables(slot_index);
    
    for table_name in tables_to_cleanup {
        if let Err(e) = remove_shared_trigger(&table_name) {
            pgrx::warning!("Failed to remove shared trigger from {}: {}", table_name, e);
        }
    }
    
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_shared_trigger_name() {
        assert_eq!(get_shared_trigger_name("users"), "_pgsub_shared_users");
        assert_eq!(get_shared_trigger_name("public.orders"), "_pgsub_shared_public_orders");
    }
}
