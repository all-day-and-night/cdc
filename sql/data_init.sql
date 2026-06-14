-- 초기 고객 데이터 3명 삽입
INSERT INTO public.customers (first_name, last_name, email) VALUES
('김', '철수', 'chulsoo.kim@example.com'),
('이', '영희', 'younghee.lee@example.com'),
('박', '민준', 'minjun.park@example.com');


-- 초기 상품 데이터 4개 삽입
INSERT INTO public.products (name, price, stock_quantity, category) VALUES
('고성능 노트북 Pro', 1500.00, 50, 'Electronics'),
('기계식 키보드', 120.50, 200, 'Accessories'),
('무선 마우스', 45.99, 500, 'Accessories'),
('4K 모니터 32인치', 550.00, 80, 'Electronics');



-- 초기 주문 데이터 5건 삽입
INSERT INTO public.orders (customer_id, product_id, quantity, total_price) VALUES
-- 김철수 (ID: 1)의 주문
(1, 1, 1, 1500.00), -- 노트북 1개
(1, 3, 2, 91.98),  -- 마우스 2개

-- 이영희 (ID: 2)의 주문
(2, 2, 1, 120.50), -- 키보드 1개

-- 박민준 (ID: 3)의 주문
(3, 4, 1, 550.00), -- 모니터 1개
(3, 3, 1, 45.99);  -- 마우스 1개
