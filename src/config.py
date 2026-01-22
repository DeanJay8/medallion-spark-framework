# 1. Unity Catalog Hierarchy
CATALOG = "workspace"          # Usually your name or 'main'
SCHEMA  = "lakehouse_db"       # The database where your Delta tables live
VOLUME  = "landing_zone"       # The Unity Catalog Volume for raw files

# 2. Base Paths
# This path works with both Spark and Python's os/open commands
BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/"

# 3. Table Naming Convention
# Centralizing these prevents "Table Not Found" errors across different notebooks
TABLE_BRONZE = f"{CATALOG}.{SCHEMA}.finance_bronze"
TABLE_SILVER = f"{CATALOG}.{SCHEMA}.finance_silver"
TABLE_GOLD   = f"{CATALOG}.{SCHEMA}.finance_gold"

# 4. Checkpoint Locations
# Streaming needs these to stay consistent to avoid re-processing data
CHECKPOINT_BASE   = f"{BASE_PATH}_checkpoints/"
CHECKPOINT_BRONZE = f"{CHECKPOINT_BASE}bronze"
CHECKPOINT_SILVER = f"{CHECKPOINT_BASE}silver"