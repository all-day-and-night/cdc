-- **********************************************************
-- 1. Customers 테이블 생성 및 데이터 삽입 (Static Data)
-- **********************************************************

CREATE TABLE public.customers (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    registered_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);
-- **********************************************************
-- 2. Products 테이블 생성 및 데이터 삽입 (Static Data)
-- **********************************************************

CREATE TABLE public.products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    stock_quantity INTEGER NOT NULL,
    category VARCHAR(50)
);

-- **********************************************************
-- 3. Orders 테이블 생성 및 데이터 삽입 (Transactional Data)
-- **********************************************************

CREATE TABLE public.orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES public.customers(id),  -- 고객 ID (FK)
    product_id INTEGER NOT NULL REFERENCES public.products(id),    -- 상품 ID (FK)
    order_date TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    quantity INTEGER NOT NULL,
    total_price NUMERIC(10, 2) NOT NULL,
    status VARCHAR(50) DEFAULT 'Pending'
);
