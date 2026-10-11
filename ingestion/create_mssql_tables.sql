-- Olist source schema for MSSQL
-- All columns as NVARCHAR/DECIMAL/DATETIME per CSV contents.
-- No PK/FK constraints — this is a replica of an OLTP source.

USE olist;
GO

-- 1. customers
IF OBJECT_ID('dbo.customers', 'U') IS NOT NULL DROP TABLE dbo.customers;
CREATE TABLE dbo.customers (
    customer_id                 NVARCHAR(50)  NOT NULL,
    customer_unique_id          NVARCHAR(50)  NOT NULL,
    customer_zip_code_prefix    NVARCHAR(10)  NULL,
    customer_city               NVARCHAR(100) NULL,
    customer_state              NVARCHAR(5)   NULL
);
GO

-- 2. geolocation
IF OBJECT_ID('dbo.geolocation', 'U') IS NOT NULL DROP TABLE dbo.geolocation;
CREATE TABLE dbo.geolocation (
    geolocation_zip_code_prefix NVARCHAR(10)  NULL,
    geolocation_lat             DECIMAL(18, 8) NULL,
    geolocation_lng             DECIMAL(18, 8) NULL,
    geolocation_city            NVARCHAR(100) NULL,
    geolocation_state           NVARCHAR(5)   NULL
);
GO

-- 3. orders
IF OBJECT_ID('dbo.orders', 'U') IS NOT NULL DROP TABLE dbo.orders;
CREATE TABLE dbo.orders (
    order_id                        NVARCHAR(50) NOT NULL,
    customer_id                     NVARCHAR(50) NOT NULL,
    order_status                    NVARCHAR(20) NULL,
    order_purchase_timestamp        DATETIME2    NULL,
    order_approved_at               DATETIME2    NULL,
    order_delivered_carrier_date    DATETIME2    NULL,
    order_delivered_customer_date   DATETIME2    NULL,
    order_estimated_delivery_date   DATETIME2    NULL
);
GO

-- 4. order_items
IF OBJECT_ID('dbo.order_items', 'U') IS NOT NULL DROP TABLE dbo.order_items;
CREATE TABLE dbo.order_items (
    order_id            NVARCHAR(50) NOT NULL,
    order_item_id       INT          NOT NULL,
    product_id          NVARCHAR(50) NOT NULL,
    seller_id           NVARCHAR(50) NOT NULL,
    shipping_limit_date DATETIME2    NULL,
    price               DECIMAL(18, 2) NULL,
    freight_value       DECIMAL(18, 2) NULL
);
GO

-- 5. order_payments
IF OBJECT_ID('dbo.order_payments', 'U') IS NOT NULL DROP TABLE dbo.order_payments;
CREATE TABLE dbo.order_payments (
    order_id                NVARCHAR(50) NOT NULL,
    payment_sequential      INT          NULL,
    payment_type            NVARCHAR(30) NULL,
    payment_installments    INT          NULL,
    payment_value           DECIMAL(18, 2) NULL
);
GO

-- 6. order_reviews
IF OBJECT_ID('dbo.order_reviews', 'U') IS NOT NULL DROP TABLE dbo.order_reviews;
CREATE TABLE dbo.order_reviews (
    review_id               NVARCHAR(50)   NOT NULL,
    order_id                NVARCHAR(50)   NOT NULL,
    review_score            INT            NULL,
    review_comment_title    NVARCHAR(200)  NULL,
    review_comment_message  NVARCHAR(MAX)  NULL,
    review_creation_date    DATETIME2      NULL,
    review_answer_timestamp DATETIME2      NULL
);
GO

-- 7. products
IF OBJECT_ID('dbo.products', 'U') IS NOT NULL DROP TABLE dbo.products;
CREATE TABLE dbo.products (
    product_id                  NVARCHAR(50)   NOT NULL,
    product_category_name       NVARCHAR(100)  NULL,
    product_name_lenght         INT            NULL,
    product_description_lenght  INT            NULL,
    product_photos_qty          INT            NULL,
    product_weight_g            INT            NULL,
    product_length_cm           INT            NULL,
    product_height_cm           INT            NULL,
    product_width_cm            INT            NULL
);
GO

-- 8. sellers
IF OBJECT_ID('dbo.sellers', 'U') IS NOT NULL DROP TABLE dbo.sellers;
CREATE TABLE dbo.sellers (
    seller_id               NVARCHAR(50)  NOT NULL,
    seller_zip_code_prefix  NVARCHAR(10)  NULL,
    seller_city             NVARCHAR(100) NULL,
    seller_state            NVARCHAR(5)   NULL
);
GO

-- 9. product_category_name_translation
IF OBJECT_ID('dbo.product_category_name_translation', 'U') IS NOT NULL DROP TABLE dbo.product_category_name_translation;
CREATE TABLE dbo.product_category_name_translation (
    product_category_name           NVARCHAR(100) NOT NULL,
    product_category_name_english   NVARCHAR(100) NOT NULL
);
GO