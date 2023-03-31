DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.products;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.products
(
    product_id         varchar(100) NOT NULL UNIQUE ENABLED,
    category_name      varchar(100),
    weight_g           float,
    length_cm          float,
    height_cm          float,
    width_cm           float
)
    ORDER BY product_id
    SEGMENTED BY hash(product_id) ALL NODES;

DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.customers;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.customers
(
    customer_id        varchar(100) NOT NULL UNIQUE ENABLED,
    customer_unique_id varchar(100),
    zip_code           int,
    city               varchar(100),
    state              varchar(50)
)
    ORDER BY customer_id
    SEGMENTED BY hash(customer_id) ALL NODES;

DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.geolocation;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.geolocation
(
    zip_code  int NOT NULL UNIQUE ENABLED,
    latitude  float,
    longitude float,
    city      varchar(100),
    state     varchar(50)
)
    ORDER BY zip_code
    SEGMENTED BY hash(zip_code) ALL NODES;

DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.sellers;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.sellers
(
    seller_id varchar(100) NOT NULL UNIQUE ENABLED,
    zip_code  int,
    city      varchar(100),
    state     varchar(50)
)
    ORDER BY seller_id
    SEGMENTED BY hash(seller_id) ALL NODES;

DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.reviews;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.reviews
(
    review_id        varchar(100) NOT NULL UNIQUE ENABLED,
    order_id         varchar(100),
    review_score     int,
    comment_title    varchar(100),
    comment_message  varchar(300),
    creation_date    timestamp,
    answer_timestamp timestamp
)
    ORDER BY review_id
    SEGMENTED BY hash(review_id) ALL NODES;


DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.payments;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.payments
(
    order_id             varchar(100) NOT NULL,
    payment_sequential   int,
    payment_type         varchar(100),
    payment_installments int,
    payment_value        float
)
    ORDER BY order_id
    SEGMENTED BY hash(order_id) ALL NODES;



DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.order_items;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.order_items
(
    order_id            varchar(100) NOT NULL,
    order_item_id       int,
    product_id          varchar(100),
    seller_id           varchar(100),
    shipping_limit_date timestamp,
    price               float,
    freight_value       float
)
    ORDER BY order_id
    SEGMENTED BY hash(order_id) ALL NODES;


DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.orders;
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.orders
(
    order_id                      varchar(100) NOT NULL UNIQUE ENABLED,
    customer_id                   varchar(100),
    order_status                  varchar(50),
    order_purchase_timestamp      timestamp,
    order_approved_at             timestamp,
    order_delivered_carrier_date  timestamp,
    order_delivered_customer_date timestamp,
    order_estimated_delivery_date timestamp
)
    ORDER BY order_id
    SEGMENTED BY hash(order_id) ALL NODES;