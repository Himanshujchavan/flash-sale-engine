-- Run this once against your native Windows PostgreSQL install, e.g.:
--   psql -U postgres -f setup_databases.sql
--
-- Creates one database PER SERVICE. This is deliberate: each service owns
-- its own data and must never query another service's tables directly.

CREATE DATABASE order_db;
CREATE DATABASE inventory_db;
CREATE DATABASE payment_db;
CREATE DATABASE saga_db;
CREATE DATABASE notification_db;
