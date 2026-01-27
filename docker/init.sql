-- ============================================
-- LiveQuery Test Database Initialization
-- ============================================

-- Create test tables
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    product VARCHAR(100) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    stock INTEGER DEFAULT 0,
    category VARCHAR(50),
    active BOOLEAN DEFAULT true
);

-- ============================================
-- Replication Setup
-- ============================================

-- Create publication first
CREATE PUBLICATION livequery_pub FOR ALL TABLES;

-- Create replication slot for streaming mode (pgoutput - recommended)
-- This uses the native binary protocol for better performance
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'livequery_slot') THEN
        PERFORM pg_create_logical_replication_slot('livequery_slot', 'pgoutput');
        RAISE NOTICE 'Created replication slot with pgoutput (streaming mode)';
    END IF;
END
$$;

-- ============================================
-- Sample Data
-- ============================================

INSERT INTO users (name, email, status) VALUES
    ('Alice', 'alice@example.com', 'active'),
    ('Bob', 'bob@example.com', 'active'),
    ('Charlie', 'charlie@example.com', 'inactive'),
    ('Diana', 'diana@example.com', 'active');

INSERT INTO products (name, price, stock, category, active) VALUES
    ('Laptop Pro', 1299.99, 50, 'electronics', true),
    ('Wireless Mouse', 29.99, 200, 'electronics', true),
    ('Mechanical Keyboard', 89.99, 150, 'electronics', true),
    ('4K Monitor', 399.99, 75, 'electronics', true),
    ('USB-C Hub', 49.99, 300, 'accessories', true),
    ('Webcam HD', 79.99, 100, 'electronics', true),
    ('Desk Lamp', 34.99, 80, 'accessories', false);

INSERT INTO orders (user_id, product, amount, status) VALUES
    (1, 'Laptop Pro', 1299.99, 'pending'),
    (1, 'Wireless Mouse', 29.99, 'completed'),
    (2, 'Mechanical Keyboard', 89.99, 'pending'),
    (2, 'USB-C Hub', 49.99, 'shipped'),
    (3, '4K Monitor', 399.99, 'shipped'),
    (4, 'Webcam HD', 79.99, 'pending');
