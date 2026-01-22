-- 1. Create the Catalog (The top-level container)
-- Note: You must have 'CREATE CATALOG' permissions on the metastore.
CREATE CATALOG IF NOT EXISTS workspace
COMMENT 'Main catalog for the Medallion Lakehouse Framework';

-- 2. Create the Schema (The database)
-- We explicitly point to the catalog we just created.
CREATE SCHEMA IF NOT EXISTS workspace.lakehouse_db
COMMENT 'Schema for Finance and Taxi medallion layers';

-- 3. Create the Volume (The landing zone for raw files)
-- Volumes are the modern way to handle non-tabular data (JSON, CSV, Images) in Unity Catalog.
CREATE VOLUME IF NOT EXISTS workspace.lakehouse_db.landing_zone
COMMENT 'External location for raw data ingestion (Landing Zone)';


DESCRIBE VOLUME workspace.lakehouse_db.landing_zone;