# Retail E-commerce Data Lakehouse Design

## Overview
This document outlines the design for a comprehensive Retail E-commerce dataset that will be stored across MySQL and MongoDB, with real-time streaming processing through Apache Flink. The design includes complex business relationships, multiple data sources, and hierarchical aggregated views for analytics.

## Database Architecture

### MySQL Database: `retail_ecommerce_mysql`

#### Core Business Tables

**1. Customers**
```sql
CREATE TABLE customers (
    customer_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    customer_uuid VARCHAR(36) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    date_of_birth DATE,
    gender ENUM('M', 'F', 'O'),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    postal_code VARCHAR(20),
    customer_segment ENUM('BRONZE', 'SILVER', 'GOLD', 'PLATINUM'),
    loyalty_points INT DEFAULT 0,
    total_spent DECIMAL(15,2) DEFAULT 0.00,
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login_date TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_email (email),
    INDEX idx_customer_segment (customer_segment),
    INDEX idx_registration_date (registration_date)
);
```

**2. Products**
```sql
CREATE TABLE products (
    product_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    product_uuid VARCHAR(36) UNIQUE NOT NULL,
    sku VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    brand VARCHAR(100),
    category_id BIGINT,
    subcategory_id BIGINT,
    price DECIMAL(10,2) NOT NULL,
    cost_price DECIMAL(10,2),
    weight_kg DECIMAL(8,3),
    dimensions_cm VARCHAR(50),
    color VARCHAR(50),
    size VARCHAR(20),
    material VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    stock_quantity INT DEFAULT 0,
    reorder_level INT DEFAULT 10,
    supplier_id BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_sku (sku),
    INDEX idx_category (category_id),
    INDEX idx_brand (brand),
    INDEX idx_is_active (is_active)
);
```

**3. Categories**
```sql
CREATE TABLE categories (
    category_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    parent_category_id BIGINT,
    level INT DEFAULT 1,
    sort_order INT DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (parent_category_id) REFERENCES categories(category_id)
);
```

**4. Orders**
```sql
CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    order_uuid VARCHAR(36) UNIQUE NOT NULL,
    customer_id BIGINT NOT NULL,
    order_number VARCHAR(50) UNIQUE NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status ENUM('PENDING', 'CONFIRMED', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'REFUNDED'),
    total_amount DECIMAL(15,2) NOT NULL,
    tax_amount DECIMAL(15,2) DEFAULT 0.00,
    shipping_amount DECIMAL(10,2) DEFAULT 0.00,
    discount_amount DECIMAL(10,2) DEFAULT 0.00,
    payment_method ENUM('CREDIT_CARD', 'DEBIT_CARD', 'PAYPAL', 'BANK_TRANSFER', 'CASH_ON_DELIVERY'),
    payment_status ENUM('PENDING', 'PAID', 'FAILED', 'REFUNDED'),
    shipping_address_id BIGINT,
    billing_address_id BIGINT,
    coupon_code VARCHAR(50),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    INDEX idx_order_number (order_number),
    INDEX idx_customer_id (customer_id),
    INDEX idx_order_date (order_date),
    INDEX idx_status (status),
    INDEX idx_payment_status (payment_status)
);
```

**5. Order Items**
```sql
CREATE TABLE order_items (
    order_item_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_price DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2) DEFAULT 0.00,
    tax_amount DECIMAL(10,2) DEFAULT 0.00,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    INDEX idx_order_id (order_id),
    INDEX idx_product_id (product_id)
);
```

**6. Inventory**
```sql
CREATE TABLE inventory (
    inventory_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    product_id BIGINT NOT NULL,
    warehouse_id BIGINT NOT NULL,
    quantity_available INT DEFAULT 0,
    quantity_reserved INT DEFAULT 0,
    quantity_damaged INT DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    UNIQUE KEY uk_product_warehouse (product_id, warehouse_id),
    INDEX idx_product_id (product_id),
    INDEX idx_warehouse_id (warehouse_id)
);
```

**7. Warehouses**
```sql
CREATE TABLE warehouses (
    warehouse_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    postal_code VARCHAR(20),
    phone VARCHAR(20),
    email VARCHAR(255),
    manager_name VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**8. Suppliers**
```sql
CREATE TABLE suppliers (
    supplier_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    contact_person VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    address TEXT,
    country VARCHAR(100),
    payment_terms VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**9. Coupons**
```sql
CREATE TABLE coupons (
    coupon_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    code VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    discount_type ENUM('PERCENTAGE', 'FIXED_AMOUNT'),
    discount_value DECIMAL(10,2) NOT NULL,
    minimum_order_amount DECIMAL(10,2) DEFAULT 0.00,
    maximum_discount_amount DECIMAL(10,2),
    usage_limit INT,
    used_count INT DEFAULT 0,
    valid_from TIMESTAMP,
    valid_until TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_code (code),
    INDEX idx_valid_from (valid_from),
    INDEX idx_valid_until (valid_until)
);
```

**10. Customer Addresses**
```sql
CREATE TABLE customer_addresses (
    address_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    customer_id BIGINT NOT NULL,
    address_type ENUM('BILLING', 'SHIPPING', 'BOTH'),
    address_line1 VARCHAR(255) NOT NULL,
    address_line2 VARCHAR(255),
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100),
    country VARCHAR(100) NOT NULL,
    postal_code VARCHAR(20),
    phone VARCHAR(20),
    is_default BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    INDEX idx_customer_id (customer_id),
    INDEX idx_address_type (address_type)
);
```

#### Audit and Logging Tables

**11. Order Status History**
```sql
CREATE TABLE order_status_history (
    history_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    order_id BIGINT NOT NULL,
    status ENUM('PENDING', 'CONFIRMED', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'REFUNDED'),
    changed_by VARCHAR(100),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    INDEX idx_order_id (order_id),
    INDEX idx_created_at (created_at)
);
```

**12. Inventory Transactions**
```sql
CREATE TABLE inventory_transactions (
    transaction_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    inventory_id BIGINT NOT NULL,
    transaction_type ENUM('PURCHASE', 'SALE', 'RETURN', 'ADJUSTMENT', 'DAMAGE'),
    quantity_change INT NOT NULL,
    reference_type ENUM('ORDER', 'PURCHASE_ORDER', 'ADJUSTMENT', 'RETURN'),
    reference_id BIGINT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (inventory_id) REFERENCES inventory(inventory_id),
    INDEX idx_inventory_id (inventory_id),
    INDEX idx_transaction_type (transaction_type),
    INDEX idx_created_at (created_at)
);
```

### MongoDB Database: `retail_ecommerce_mongodb`

#### User Behavior and Analytics Collections

**1. User Sessions**
```javascript
{
  "_id": ObjectId,
  "session_id": String,
  "customer_id": Number,
  "customer_uuid": String,
  "start_time": Date,
  "end_time": Date,
  "duration_seconds": Number,
  "device_type": String, // "MOBILE", "DESKTOP", "TABLET"
  "browser": String,
  "operating_system": String,
  "ip_address": String,
  "user_agent": String,
  "referrer": String,
  "landing_page": String,
  "exit_page": String,
  "pages_visited": [
    {
      "url": String,
      "timestamp": Date,
      "time_spent_seconds": Number
    }
  ],
  "events": [
    {
      "event_type": String, // "PAGE_VIEW", "CLICK", "ADD_TO_CART", "PURCHASE"
      "event_data": Object,
      "timestamp": Date
    }
  ],
  "created_at": Date,
  "updated_at": Date
}
```

**2. Product Views and Interactions**
```javascript
{
  "_id": ObjectId,
  "customer_id": Number,
  "customer_uuid": String,
  "product_id": Number,
  "product_uuid": String,
  "session_id": String,
  "view_timestamp": Date,
  "time_spent_seconds": Number,
  "interaction_type": String, // "VIEW", "CLICK", "ADD_TO_WISHLIST", "ADD_TO_CART"
  "source_page": String,
  "device_type": String,
  "location": {
    "country": String,
    "city": String,
    "coordinates": [Number, Number]
  },
  "referrer": String,
  "search_query": String,
  "filters_applied": Object,
  "created_at": Date
}
```

**3. Shopping Cart**
```javascript
{
  "_id": ObjectId,
  "cart_id": String,
  "customer_id": Number,
  "customer_uuid": String,
  "session_id": String,
  "items": [
    {
      "product_id": Number,
      "product_uuid": String,
      "sku": String,
      "name": String,
      "quantity": Number,
      "unit_price": Number,
      "added_at": Date,
      "updated_at": Date
    }
  ],
  "total_items": Number,
  "total_amount": Number,
  "discount_amount": Number,
  "tax_amount": Number,
  "shipping_amount": Number,
  "final_amount": Number,
  "coupon_code": String,
  "created_at": Date,
  "updated_at": Date,
  "expires_at": Date
}
```

**4. Wishlist**
```javascript
{
  "_id": ObjectId,
  "customer_id": Number,
  "customer_uuid": String,
  "items": [
    {
      "product_id": Number,
      "product_uuid": String,
      "added_at": Date,
      "notes": String
    }
  ],
  "total_items": Number,
  "created_at": Date,
  "updated_at": Date
}
```

**5. Product Reviews and Ratings**
```javascript
{
  "_id": ObjectId,
  "review_id": String,
  "product_id": Number,
  "product_uuid": String,
  "customer_id": Number,
  "customer_uuid": String,
  "order_id": Number,
  "rating": Number, // 1-5
  "title": String,
  "review_text": String,
  "verified_purchase": Boolean,
  "helpful_votes": Number,
  "unhelpful_votes": Number,
  "images": [String],
  "tags": [String],
  "status": String, // "PENDING", "APPROVED", "REJECTED"
  "moderator_notes": String,
  "created_at": Date,
  "updated_at": Date
}
```

**6. Search Analytics**
```javascript
{
  "_id": ObjectId,
  "search_id": String,
  "customer_id": Number,
  "session_id": String,
  "query": String,
  "filters": Object,
  "sort_by": String,
  "results_count": Number,
  "clicked_products": [Number],
  "purchased_products": [Number],
  "search_timestamp": Date,
  "time_to_first_click": Number,
  "device_type": String,
  "location": Object,
  "created_at": Date
}
```

**7. Customer Behavior Patterns**
```javascript
{
  "_id": ObjectId,
  "customer_id": Number,
  "customer_uuid": String,
  "behavior_pattern": {
    "preferred_categories": [String],
    "preferred_brands": [String],
    "average_order_value": Number,
    "purchase_frequency": Number,
    "preferred_payment_method": String,
    "preferred_shipping_method": String,
    "typical_browsing_time": Number,
    "cart_abandonment_rate": Number,
    "return_rate": Number
  },
  "engagement_metrics": {
    "total_sessions": Number,
    "total_page_views": Number,
    "total_purchases": Number,
    "last_purchase_date": Date,
    "days_since_last_purchase": Number,
    "lifetime_value": Number
  },
  "preferences": {
    "preferred_device": String,
    "preferred_time": String,
    "preferred_day": String,
    "email_subscription": Boolean,
    "sms_subscription": Boolean,
    "push_notification": Boolean
  },
  "created_at": Date,
  "updated_at": Date
}
```

**8. Product Performance Analytics**
```javascript
{
  "_id": ObjectId,
  "product_id": Number,
  "product_uuid": String,
  "sku": String,
  "analytics_period": String, // "DAILY", "WEEKLY", "MONTHLY"
  "period_start": Date,
  "period_end": Date,
  "metrics": {
    "views": Number,
    "unique_views": Number,
    "clicks": Number,
    "add_to_cart": Number,
    "purchases": Number,
    "revenue": Number,
    "conversion_rate": Number,
    "average_rating": Number,
    "review_count": Number,
    "return_rate": Number
  },
  "traffic_sources": {
    "organic_search": Number,
    "paid_search": Number,
    "social_media": Number,
    "email": Number,
    "direct": Number,
    "referral": Number
  },
  "geographic_performance": [
    {
      "country": String,
      "views": Number,
      "purchases": Number,
      "revenue": Number
    }
  ],
  "device_performance": [
    {
      "device_type": String,
      "views": Number,
      "purchases": Number,
      "conversion_rate": Number
    }
  ],
  "created_at": Date,
  "updated_at": Date
}
```

**9. Real-time Inventory Events**
```javascript
{
  "_id": ObjectId,
  "event_id": String,
  "product_id": Number,
  "warehouse_id": Number,
  "event_type": String, // "STOCK_IN", "STOCK_OUT", "RESERVED", "RELEASED", "DAMAGED"
  "quantity_change": Number,
  "previous_quantity": Number,
  "new_quantity": Number,
  "order_id": Number,
  "supplier_id": Number,
  "notes": String,
  "timestamp": Date,
  "created_at": Date
}
```

**10. Customer Support Tickets**
```javascript
{
  "_id": ObjectId,
  "ticket_id": String,
  "customer_id": Number,
  "customer_uuid": String,
  "order_id": Number,
  "product_id": Number,
  "category": String, // "ORDER_ISSUE", "PRODUCT_ISSUE", "PAYMENT_ISSUE", "SHIPPING_ISSUE"
  "priority": String, // "LOW", "MEDIUM", "HIGH", "URGENT"
  "status": String, // "OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"
  "subject": String,
  "description": String,
  "attachments": [String],
  "assigned_to": String,
  "resolution": String,
  "satisfaction_rating": Number,
  "created_at": Date,
  "updated_at": Date,
  "resolved_at": Date
}
```

## Complex Business Relationships

### MySQL-MySQL Relationships

1. **Customer → Orders → Order Items → Products**
   - One customer can have multiple orders
   - Each order can have multiple order items
   - Each order item references a specific product

2. **Products → Categories (Hierarchical)**
   - Products belong to categories
   - Categories can have parent categories (self-referencing)

3. **Products → Suppliers**
   - Products are supplied by specific suppliers
   - One supplier can supply multiple products

4. **Products → Inventory → Warehouses**
   - Products have inventory across multiple warehouses
   - Each warehouse tracks its own inventory levels

5. **Orders → Customer Addresses**
   - Orders reference billing and shipping addresses
   - One customer can have multiple addresses

6. **Orders → Coupons**
   - Orders can use discount coupons
   - Coupons have usage limits and validity periods

### MongoDB-MongoDB Relationships

1. **User Sessions → Product Views → Shopping Cart**
   - Sessions contain multiple product interactions
   - Product views can lead to cart additions

2. **Product Views → Product Performance Analytics**
   - Individual views aggregate into performance metrics
   - Real-time analytics based on user behavior

3. **Customer Behavior Patterns → User Sessions**
   - Behavior patterns derived from session data
   - Historical analysis influences current behavior

4. **Product Reviews → Product Performance Analytics**
   - Reviews contribute to product performance metrics
   - Rating aggregation affects product recommendations

### MySQL-MongoDB Cross-Database Relationships

1. **Customers (MySQL) ↔ User Sessions (MongoDB)**
   - Customer ID links MySQL customer data with MongoDB session data
   - Enables comprehensive customer journey analysis

2. **Products (MySQL) ↔ Product Views (MongoDB)**
   - Product ID connects inventory/order data with user behavior
   - Enables inventory optimization based on user interest

3. **Orders (MySQL) ↔ Shopping Cart (MongoDB)**
   - Order ID links completed purchases with cart behavior
   - Enables cart abandonment analysis

4. **Products (MySQL) ↔ Product Performance Analytics (MongoDB)**
   - Product ID connects sales data with behavioral analytics
   - Enables comprehensive product performance analysis

## Hierarchical Aggregation Views (3-Level Architecture)

### Level 1: Base Aggregations (Direct from Fact/Dimension Tables)

**1. Customer Order Summary (Level 1)**
```sql
-- Aggregates order data per customer
CREATE VIEW v_customer_order_summary_l1 AS
SELECT 
    customer_id,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(total_amount) as total_spent,
    AVG(total_amount) as avg_order_value,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date,
    COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) as cancelled_orders,
    COUNT(CASE WHEN status IN ('DELIVERED', 'SHIPPED') THEN 1 END) as completed_orders
FROM orders
GROUP BY customer_id;
```

**2. Product Sales Summary (Level 1)**
```sql
-- Aggregates sales data per product
CREATE VIEW v_product_sales_summary_l1 AS
SELECT 
    p.product_id,
    p.sku,
    p.name,
    p.category_id,
    p.brand,
    p.price,
    COUNT(oi.order_item_id) as total_sales_count,
    SUM(oi.quantity) as total_units_sold,
    SUM(oi.total_price) as total_revenue,
    AVG(oi.unit_price) as avg_selling_price,
    COUNT(DISTINCT o.customer_id) as unique_customers
FROM products p
LEFT JOIN order_items oi ON p.product_id = oi.product_id
LEFT JOIN orders o ON oi.order_id = o.order_id
GROUP BY p.product_id, p.sku, p.name, p.category_id, p.brand, p.price;
```

**3. Category Performance Summary (Level 1)**
```sql
-- Aggregates performance data per category
CREATE VIEW v_category_performance_l1 AS
SELECT 
    c.category_id,
    c.name as category_name,
    COUNT(DISTINCT p.product_id) as total_products,
    COUNT(oi.order_item_id) as total_sales,
    SUM(oi.quantity) as total_units_sold,
    SUM(oi.total_price) as total_revenue,
    AVG(p.price) as avg_product_price
FROM categories c
LEFT JOIN products p ON c.category_id = p.category_id
LEFT JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY c.category_id, c.name;
```

**4. Customer Behavior Summary (Level 1)**
```sql
-- Aggregates customer behavior from MongoDB
CREATE VIEW v_customer_behavior_l1 AS
SELECT 
    customer_id,
    COUNT(DISTINCT session_id) as total_sessions,
    SUM(duration_seconds) as total_session_time,
    COUNT(DISTINCT CASE WHEN event_type = 'PURCHASE' THEN session_id END) as sessions_with_purchase,
    AVG(duration_seconds) as avg_session_duration,
    COUNT(CASE WHEN event_type = 'ADD_TO_CART' THEN 1 END) as cart_additions,
    COUNT(CASE WHEN event_type = 'PAGE_VIEW' THEN 1 END) as page_views
FROM user_sessions
GROUP BY customer_id;
```

**5. Product Interaction Summary (Level 1)**
```sql
-- Aggregates product interaction data from MongoDB
CREATE VIEW v_product_interaction_l1 AS
SELECT 
    product_id,
    COUNT(*) as total_views,
    COUNT(DISTINCT customer_id) as unique_viewers,
    COUNT(CASE WHEN interaction_type = 'ADD_TO_CART' THEN 1 END) as cart_additions,
    COUNT(CASE WHEN interaction_type = 'CLICK' THEN 1 END) as clicks,
    AVG(time_spent_seconds) as avg_view_time
FROM product_views
GROUP BY product_id;
```

### Level 2: Intermediate Aggregations (Based on Level 1 Views)

**1. Customer Value Analysis (Level 2)**
```sql
-- Combines customer order data with behavior data
CREATE VIEW v_customer_value_analysis_l2 AS
SELECT 
    cos.customer_id,
    cos.total_orders,
    cos.total_spent,
    cos.avg_order_value,
    cos.completed_orders,
    cos.cancelled_orders,
    cbs.total_sessions,
    cbs.avg_session_duration,
    cbs.cart_additions,
    cbs.page_views,
    -- Calculated metrics
    (cos.total_spent / NULLIF(cos.total_orders, 0)) as customer_lifetime_value,
    (cbs.sessions_with_purchase / NULLIF(cbs.total_sessions, 0)) as conversion_rate,
    (cos.total_spent / NULLIF(cbs.total_sessions, 0)) as revenue_per_session
FROM v_customer_order_summary_l1 cos
LEFT JOIN v_customer_behavior_l1 cbs ON cos.customer_id = cbs.customer_id;
```

**2. Product Performance Analysis (Level 2)**
```sql
-- Combines product sales data with interaction data
CREATE VIEW v_product_performance_l2 AS
SELECT 
    pss.product_id,
    pss.sku,
    pss.name,
    pss.category_id,
    pss.brand,
    pss.total_sales_count,
    pss.total_revenue,
    pss.avg_selling_price,
    pis.total_views,
    pis.unique_viewers,
    pis.cart_additions,
    pis.clicks,
    pis.avg_view_time,
    -- Calculated metrics
    (pis.cart_additions / NULLIF(pis.total_views, 0)) as view_to_cart_rate,
    (pss.total_sales_count / NULLIF(pis.cart_additions, 0)) as cart_to_purchase_rate,
    (pss.total_revenue / NULLIF(pis.total_views, 0)) as revenue_per_view
FROM v_product_sales_summary_l1 pss
LEFT JOIN v_product_interaction_l1 pis ON pss.product_id = pis.product_id;
```

**3. Category Performance Analysis (Level 2)**
```sql
-- Combines category performance with product-level data
CREATE VIEW v_category_performance_l2 AS
SELECT 
    cps.category_id,
    cps.category_name,
    cps.total_products,
    cps.total_revenue,
    cps.avg_product_price,
    -- Product interaction metrics
    SUM(pis.total_views) as total_category_views,
    SUM(pis.cart_additions) as total_category_cart_additions,
    AVG(pis.avg_view_time) as avg_category_view_time,
    -- Calculated metrics
    (cps.total_revenue / NULLIF(SUM(pis.total_views), 0)) as revenue_per_view,
    (SUM(pis.cart_additions) / NULLIF(SUM(pis.total_views), 0)) as category_conversion_rate
FROM v_category_performance_l1 cps
LEFT JOIN v_product_interaction_l1 pis ON cps.category_id = (
    SELECT category_id FROM products WHERE product_id = pis.product_id
)
GROUP BY cps.category_id, cps.category_name, cps.total_products, cps.total_revenue, cps.avg_product_price;
```

### Level 3: High-Level Business Intelligence Views

**1. Executive Dashboard (Level 3)**
```sql
-- High-level business metrics for executive dashboard
CREATE VIEW v_executive_dashboard_l3 AS
SELECT 
    -- Overall business metrics
    COUNT(DISTINCT cva.customer_id) as total_customers,
    SUM(cva.total_spent) as total_revenue,
    AVG(cva.customer_lifetime_value) as avg_customer_lifetime_value,
    AVG(cva.conversion_rate) as overall_conversion_rate,
    -- Product performance
    COUNT(DISTINCT ppa.product_id) as total_products,
    SUM(ppa.total_revenue) as total_product_revenue,
    AVG(ppa.view_to_cart_rate) as avg_view_to_cart_rate,
    -- Category performance
    COUNT(DISTINCT cpa.category_id) as total_categories,
    SUM(cpa.total_revenue) as total_category_revenue,
    AVG(cpa.category_conversion_rate) as avg_category_conversion_rate,
    -- Time-based metrics
    DATE_FORMAT(NOW(), '%Y-%m') as current_month
FROM v_customer_value_analysis_l2 cva
CROSS JOIN v_product_performance_l2 ppa
CROSS JOIN v_category_performance_l2 cpa;
```

**2. Customer Segmentation Dashboard (Level 3)**
```sql
-- Advanced customer segmentation combining all levels
CREATE VIEW v_customer_segmentation_l3 AS
SELECT 
    cva.customer_id,
    -- Value-based segmentation
    CASE 
        WHEN cva.customer_lifetime_value > 1000 THEN 'HIGH_VALUE'
        WHEN cva.customer_lifetime_value > 500 THEN 'MEDIUM_VALUE'
        WHEN cva.customer_lifetime_value > 100 THEN 'LOW_VALUE'
        ELSE 'MINIMAL_VALUE'
    END as value_segment,
    -- Engagement-based segmentation
    CASE 
        WHEN cva.total_sessions > 50 AND cva.avg_session_duration > 300 THEN 'HIGH_ENGAGEMENT'
        WHEN cva.total_sessions > 20 THEN 'MEDIUM_ENGAGEMENT'
        ELSE 'LOW_ENGAGEMENT'
    END as engagement_segment,
    -- Purchase behavior segmentation
    CASE 
        WHEN cva.total_orders > 10 THEN 'FREQUENT_BUYER'
        WHEN cva.total_orders > 5 THEN 'REGULAR_BUYER'
        WHEN cva.total_orders > 1 THEN 'OCCASIONAL_BUYER'
        ELSE 'ONE_TIME_BUYER'
    END as purchase_segment,
    -- Combined segment
    CONCAT(
        CASE 
            WHEN cva.customer_lifetime_value > 1000 THEN 'HIGH_VALUE'
            WHEN cva.customer_lifetime_value > 500 THEN 'MEDIUM_VALUE'
            ELSE 'LOW_VALUE'
        END,
        '_',
        CASE 
            WHEN cva.total_sessions > 50 THEN 'HIGH_ENGAGEMENT'
            WHEN cva.total_sessions > 20 THEN 'MEDIUM_ENGAGEMENT'
            ELSE 'LOW_ENGAGEMENT'
        END
    ) as combined_segment,
    -- Metrics
    cva.total_spent,
    cva.customer_lifetime_value,
    cva.conversion_rate,
    cva.total_sessions,
    cva.avg_session_duration
FROM v_customer_value_analysis_l2 cva;
```

**3. Product Portfolio Analysis (Level 3)**
```sql
-- Comprehensive product portfolio analysis
CREATE VIEW v_product_portfolio_l3 AS
SELECT 
    ppa.product_id,
    ppa.sku,
    ppa.name,
    ppa.brand,
    -- Performance tiers
    CASE 
        WHEN ppa.total_revenue > 10000 THEN 'STAR_PRODUCT'
        WHEN ppa.total_revenue > 5000 THEN 'GROWING_PRODUCT'
        WHEN ppa.total_revenue > 1000 THEN 'STABLE_PRODUCT'
        ELSE 'DEVELOPING_PRODUCT'
    END as performance_tier,
    -- Conversion performance
    CASE 
        WHEN ppa.view_to_cart_rate > 0.1 THEN 'HIGH_CONVERSION'
        WHEN ppa.view_to_cart_rate > 0.05 THEN 'MEDIUM_CONVERSION'
        ELSE 'LOW_CONVERSION'
    END as conversion_tier,
    -- Revenue efficiency
    CASE 
        WHEN ppa.revenue_per_view > 10 THEN 'HIGH_EFFICIENCY'
        WHEN ppa.revenue_per_view > 5 THEN 'MEDIUM_EFFICIENCY'
        ELSE 'LOW_EFFICIENCY'
    END as efficiency_tier,
    -- Metrics
    ppa.total_revenue,
    ppa.total_views,
    ppa.view_to_cart_rate,
    ppa.cart_to_purchase_rate,
    ppa.revenue_per_view,
    ppa.avg_view_time
FROM v_product_performance_l2 ppa;
```

**4. Inventory Optimization Dashboard (Level 3)**
```sql
-- Real-time inventory optimization combining all data sources
CREATE VIEW v_inventory_optimization_l3 AS
SELECT 
    p.product_id,
    p.sku,
    p.name,
    p.reorder_level,
    i.quantity_available,
    i.quantity_reserved,
    -- Demand prediction from Level 2
    ppa.total_sales_count as recent_sales,
    ppa.total_views as recent_views,
    ppa.view_to_cart_rate,
    -- Stock status
    CASE 
        WHEN i.quantity_available <= p.reorder_level THEN 'LOW_STOCK'
        WHEN i.quantity_available <= (p.reorder_level * 2) THEN 'MEDIUM_STOCK'
        ELSE 'SUFFICIENT_STOCK'
    END as stock_status,
    -- Reorder recommendation
    CASE 
        WHEN i.quantity_available <= p.reorder_level 
        THEN (ppa.total_sales_count * 2) - i.quantity_available
        ELSE 0
    END as recommended_reorder_quantity,
    -- Performance indicators
    (i.quantity_available / NULLIF(ppa.total_sales_count, 0)) as stock_turnover_ratio,
    (ppa.total_views / NULLIF(ppa.total_sales_count, 0)) as views_per_sale
FROM products p
JOIN inventory i ON p.product_id = i.product_id
LEFT JOIN v_product_performance_l2 ppa ON p.product_id = ppa.product_id
WHERE p.is_active = TRUE;
```

## Data Flow Architecture

### Ingestion Layer
1. **MySQL CDC (Change Data Capture)** → Kafka
2. **MongoDB Change Streams** → Kafka
3. **Real-time User Events** → Kafka

### Processing Layer (Flink)
1. **Level 1 Aggregations** (Lightweight, frequent updates)
   - Customer order summaries
   - Product sales summaries
   - Basic behavior metrics

2. **Level 2 Aggregations** (Medium complexity, periodic updates)
   - Customer value analysis
   - Product performance analysis
   - Category performance analysis

3. **Level 3 Aggregations** (Complex, batch updates)
   - Executive dashboards
   - Customer segmentation
   - Portfolio analysis

### Storage Layer
1. **MySQL**: Transactional data, structured business data
2. **MongoDB**: User behavior, analytics, flexible schema data
3. **ClickHouse**: Aggregated analytics (Level 1, 2, 3 views)
4. **Redis**: Real-time caching, session data

### Query Layer (Trino)
- Unified SQL interface across all data sources
- Real-time analytics queries
- Complex business intelligence queries

## Implementation Notes

1. **Data Consistency**: Use event sourcing and CQRS patterns
2. **Scalability**: Partition data by customer_id, product_id, and date
3. **Performance**: Implement proper indexing strategies
4. **Monitoring**: Track data quality, latency, and throughput metrics
5. **Security**: Implement row-level security and data encryption
6. **Aggregation Strategy**: 
   - Level 1: Real-time updates (every 5-15 minutes)
   - Level 2: Periodic updates (every 1-4 hours)
   - Level 3: Batch updates (daily/weekly)

This hierarchical design provides optimized computation by breaking complex queries into manageable levels, reducing workload and improving performance. 