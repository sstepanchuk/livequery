const http = require('http');
const { Client } = require('pg');

const client = new Client({
  connectionString: process.env.DATABASE_URL || 'postgres://livequery:livequery@postgres:5432/livequery'
});

client.connect().then(() => console.log('Connected to PostgreSQL'));

const server = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }
  
  if (req.method === 'POST' && req.url === '/sql') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', async () => {
      try {
        const { sql } = JSON.parse(body);
        if (!sql) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'SQL required' }));
          return;
        }
        
        console.log('Executing:', sql.slice(0, 100));
        const result = await client.query(sql);
        
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ 
          success: true, 
          rowCount: result.rowCount,
          rows: result.rows?.slice(0, 100)
        }));
      } catch (err) {
        console.error('SQL Error:', err.message);
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      }
    });
  } else {
    res.writeHead(404);
    res.end('Not found');
  }
});

server.listen(3001, () => console.log('SQL API running on :3001'));
