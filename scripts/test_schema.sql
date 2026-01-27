-- Test schema for LiveQuery Server
-- Realistic e-commerce database

-- Users
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    role VARCHAR(50) DEFAULT 'customer',
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'
);

-- Categories
CREATE TABLE IF NOT EXISTS categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    parent_id INT REFERENCES categories(id),
    description TEXT
);

-- Products
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price NUMERIC(10, 2) NOT NULL,
    stock INT DEFAULT 0,
    category_id INT REFERENCES categories(id),
    active BOOLEAN DEFAULT true,
    tags TEXT[],
    attributes JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW()
);

-- Orders
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    status VARCHAR(50) DEFAULT 'pending',
    total NUMERIC(10, 2),
    shipping_address JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Order Items
CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(id),
    product_id INT REFERENCES products(id),
    quantity INT NOT NULL,
    price NUMERIC(10, 2) NOT NULL
);

-- Reviews
CREATE TABLE IF NOT EXISTS reviews (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    product_id INT REFERENCES products(id),
    rating INT CHECK (rating >= 1 AND rating <= 5),
    comment TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert test data
INSERT INTO users (email, name, role, active, metadata) VALUES
    ('alice@test.com', 'Alice Johnson', 'admin', true, '{"department": "IT"}'),
    ('bob@test.com', 'Bob Smith', 'customer', true, '{"loyalty_level": "gold"}'),
    ('charlie@test.com', 'Charlie Brown', 'customer', true, '{}'),
    ('diana@test.com', 'Diana Ross', 'customer', false, '{}'),
    ('eve@test.com', 'Eve Wilson', 'manager', true, '{"department": "Sales"}')
ON CONFLICT (email) DO NOTHING;

INSERT INTO categories (name, parent_id, description) VALUES
    ('Electronics', NULL, 'Electronic devices and accessories'),
    ('Phones', 1, 'Mobile phones and smartphones'),
    ('Laptops', 1, 'Laptop computers'),
    ('Clothing', NULL, 'Apparel and fashion'),
    ('Books', NULL, 'Books and publications')
ON CONFLICT DO NOTHING;

INSERT INTO products (name, description, price, stock, category_id, active, tags, attributes) VALUES
    ('iPhone 15', 'Latest Apple smartphone', 999.99, 50, 2, true, ARRAY['apple', 'smartphone', 'premium'], '{"color": "black", "storage": "256GB"}'),
    ('Samsung Galaxy S24', 'Android flagship phone', 899.99, 30, 2, true, ARRAY['samsung', 'android', 'flagship'], '{"color": "white", "storage": "128GB"}'),
    ('MacBook Pro 16"', 'Professional laptop', 2499.99, 20, 3, true, ARRAY['apple', 'laptop', 'pro'], '{"ram": "32GB", "ssd": "1TB"}'),
    ('ThinkPad X1', 'Business laptop', 1599.99, 15, 3, true, ARRAY['lenovo', 'business'], '{"ram": "16GB", "ssd": "512GB"}'),
    ('T-Shirt Basic', 'Cotton t-shirt', 19.99, 200, 4, true, ARRAY['cotton', 'basic'], '{"sizes": ["S", "M", "L", "XL"]}'),
    ('Clean Code', 'Programming book by Robert Martin', 39.99, 100, 5, true, ARRAY['programming', 'bestseller'], '{"pages": 464, "author": "Robert C. Martin"}'),
    ('Old Phone', 'Discontinued model', 199.99, 0, 2, false, ARRAY['old'], '{}')
ON CONFLICT DO NOTHING;

INSERT INTO orders (user_id, status, total, shipping_address) VALUES
    (2, 'completed', 1099.98, '{"city": "New York", "zip": "10001"}'),
    (2, 'pending', 2499.99, '{"city": "New York", "zip": "10001"}'),
    (3, 'completed', 59.98, '{"city": "Los Angeles", "zip": "90001"}'),
    (3, 'shipped', 899.99, '{"city": "Los Angeles", "zip": "90001"}'),
    (5, 'pending', 1599.99, '{"city": "Chicago", "zip": "60601"}')
ON CONFLICT DO NOTHING;

INSERT INTO order_items (order_id, product_id, quantity, price) VALUES
    (1, 1, 1, 999.99),
    (1, 5, 5, 19.99),
    (2, 3, 1, 2499.99),
    (3, 5, 2, 19.99),
    (3, 6, 1, 39.99),
    (4, 2, 1, 899.99),
    (5, 4, 1, 1599.99)
ON CONFLICT DO NOTHING;

INSERT INTO reviews (user_id, product_id, rating, comment) VALUES
    (2, 1, 5, 'Amazing phone, love it!'),
    (3, 1, 4, 'Great but expensive'),
    (2, 3, 5, 'Best laptop ever'),
    (5, 6, 5, 'Must read for developers'),
    (3, 5, 3, 'Basic quality, as expected')
ON CONFLICT DO NOTHING;

-- Verify data
SELECT 'users' as table_name, COUNT(*) as count FROM users
UNION ALL SELECT 'categories', COUNT(*) FROM categories
UNION ALL SELECT 'products', COUNT(*) FROM products
UNION ALL SELECT 'orders', COUNT(*) FROM orders
UNION ALL SELECT 'order_items', COUNT(*) FROM order_items
UNION ALL SELECT 'reviews', COUNT(*) FROM reviews;
