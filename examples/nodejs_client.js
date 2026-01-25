#!/usr/bin/env node
/**
 * Example Node.js client for pg_subscribe extension.
 * 
 * This demonstrates how to consume streaming subscription events
 * using the pg library with cursor support.
 */

const { Client } = require('pg');
const Cursor = require('pg-cursor');

async function subscribeToQuery(connectionString, query) {
    const client = new Client({ connectionString });
    
    try {
        await client.connect();
        console.log(`Subscribing to: ${query}`);
        console.log('-'.repeat(60));
        
        const escapedQuery = query.replace(/'/g, "''");
        const subscribeSQL = `SELECT * FROM subscribe('${escapedQuery}')`;
        
        const cursor = client.query(new Cursor(subscribeSQL));
        
        const processRows = async () => {
            while (true) {
                const rows = await new Promise((resolve, reject) => {
                    cursor.read(100, (err, rows) => {
                        if (err) reject(err);
                        else resolve(rows);
                    });
                });
                
                if (rows.length === 0) {
                    // Connection closed
                    break;
                }
                
                for (const row of rows) {
                    const { mz_timestamp, mz_diff, mz_progressed, data } = row;
                    
                    if (mz_progressed) {
                        console.log(`[${mz_timestamp}] ♥ heartbeat`);
                    } else if (mz_diff > 0) {
                        console.log(`[${mz_timestamp}] + INSERT:`, JSON.stringify(data, null, 2));
                    } else {
                        console.log(`[${mz_timestamp}] - DELETE:`, JSON.stringify(data, null, 2));
                    }
                }
            }
        };
        
        await processRows();
        
    } catch (err) {
        if (err.message.includes('Connection terminated')) {
            console.log('\nSubscription ended');
        } else {
            console.error('Error:', err);
        }
    } finally {
        await client.end();
    }
}

// Handle Ctrl+C gracefully
process.on('SIGINT', () => {
    console.log('\nInterrupted, closing connection...');
    process.exit(0);
});

// Main
const connectionString = process.env.DATABASE_URL || 'postgres://postgres@localhost/postgres';
const query = process.argv[2] || 'SELECT * FROM users';

subscribeToQuery(connectionString, query);
