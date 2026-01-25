-- Test database setup for pg_subscribe
-- Run this script to create test tables and data

-- Create extension
CREATE EXTENSION IF NOT EXISTS pg_subscribe;

-- Create test tables
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS products CASCADE;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    stock INTEGER DEFAULT 0,
    category VARCHAR(50)
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL DEFAULT 1,
    amount NUMERIC(10, 2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_created_at ON orders(created_at);

-- Insert sample data
INSERT INTO users (name, email) VALUES
    ('Alice Johnson', 'alice@example.com'),
    ('Bob Smith', 'bob@example.com'),
    ('Charlie Brown', 'charlie@example.com'),
    ('Diana Prince', 'diana@example.com'),
    ('Eve Wilson', 'eve@example.com');

INSERT INTO products (name, price, stock, category) VALUES
    ('Laptop', 999.99, 50, 'Electronics'),
    ('Mouse', 29.99, 200, 'Electronics'),
    ('Keyboard', 79.99, 150, 'Electronics'),
    ('Monitor', 299.99, 75, 'Electronics'),
    ('Headphones', 149.99, 100, 'Electronics'),
    ('Desk', 199.99, 30, 'Furniture'),
    ('Chair', 249.99, 40, 'Furniture');

INSERT INTO orders (user_id, product_id, quantity, amount, status) VALUES
    (1, 1, 1, 999.99, 'completed'),
    (1, 2, 2, 59.98, 'completed'),
    (2, 3, 1, 79.99, 'pending'),
    (2, 4, 1, 299.99, 'pending'),
    (3, 5, 1, 149.99, 'completed'),
    (3, 1, 1, 999.99, 'shipped'),
    (4, 6, 2, 399.98, 'pending'),
    (5, 7, 1, 249.99, 'completed');

-- Verify setup
SELECT 'Users:' as table_name, COUNT(*) as count FROM users
UNION ALL
SELECT 'Products:', COUNT(*) FROM products
UNION ALL
SELECT 'Orders:', COUNT(*) FROM orders;

-- Example queries to try with subscribe()
COMMENT ON TABLE users IS 'Example: SELECT * FROM subscribe(''SELECT * FROM users WHERE active = true'')';
COMMENT ON TABLE orders IS 'Example: SELECT * FROM subscribe(''SELECT user_id, COUNT(*) as cnt FROM orders GROUP BY user_id'')';

-- Test query analysis
SELECT pg_subscribe_analyze_query('SELECT user_id, COUNT(*) FROM orders GROUP BY user_id');
SELECT pg_subscribe_analyze_query('SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id');

-- Show extension stats
SELECT * FROM pg_subscribe_statistics;

\echo 'Setup complete! Try these subscription examples:'
\echo ''
\echo '-- Simple subscription:'
\echo 'SELECT * FROM subscribe(''SELECT * FROM users'');'
\echo ''
\echo '-- Aggregation subscription:'
\echo 'SELECT * FROM subscribe(''SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id'');'
\echo ''
\echo '-- JOIN subscription:'
\echo 'SELECT * FROM subscribe(''SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id'');'
