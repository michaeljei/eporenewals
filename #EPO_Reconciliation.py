# --- Lake Database Table Automation Script ---
# Supports recursive directory traversal, schema management, and partition detection

from notebookutils import mssparkutils
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import json

# --- Configuration ---
workspace = mssparkutils.env.getWorkspaceName()
ls_name = "mibisilver_datalake"

# Resolve storage account dynamically
props = json.loads(mssparkutils.credentials.getPropertiesAll(ls_name))
endpoint = props["Endpoint"]
account = endpoint.replace("https://", "").replace("/", "")

# Define storage container and paths
container = "ipo-dw"
schema_folder = "dbo"
ldb_name = "IPO_DW"
base_path = f"abfss://{container}@{account}/{schema_folder}/"

# --- Spark DateTime Configuration ---
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")

# --- Function: Detect Data Formats ---
def detect_data_formats_synapse(loc: str, recursive=True, ignore_hidden=True) -> dict:
    """
    Detect if a location contains Parquet, CSV, or Delta files.
    """
    found = {"parquet": False, "csv": False, "delta": False, "any": False}
    if not mssparkutils.fs.exists(loc):
        return found

    stack = [loc]
    csv_suffixes = (".csv", ".tsv", ".txt", ".csv.gz", ".csv.bz2", ".csv.zip", ".tsv.gz", ".txt.gz")

    while stack and not found["any"]:
        cur = stack.pop()
        entries = mssparkutils.fs.ls(cur)

        for e in entries:
            name = e.name
            path = e.path
            is_dir = e.isDir

            if is_dir and name == "_delta_log":
                found["delta"] = True
                found["any"] = True
                break

            if ignore_hidden and (name.startswith("_") or name.startswith(".")):
                continue

            if is_dir:
                if recursive:
                    stack.append(path)
            else:
                lower = name.lower()
                if lower.endswith(".parquet"):
                    found["parquet"] = True
                    found["any"] = True
                    break
                if lower.endswith(csv_suffixes):
                    found["csv"] = True
                    found["any"] = True
                    break

    return found

# --- Function: Check for Partitions in Storage ---
def has_partitions_in_storage(loc):
    """
    Check if the storage location contains partitioned data.
    """
    if not mssparkutils.fs.exists(loc):
        return False
    
    try:
        entries = mssparkutils.fs.ls(loc)
        for entry in entries:
            if entry.isDir and '=' in entry.name:
                return True
        return False
    except Exception as e:
        print(f"[WARNING] Error checking partitions in storage: {str(e)}")
        return False

# --- Function: Find Table Folders Recursively ---
def find_table_folders(base_path, max_depth=10):
    """
    Recursively find all folders that contain data files (actual tables).
    """
    table_folders = []
    
    def recurse(current_path, current_depth, relative_path=""):
        if current_depth > max_depth:
            print(f"[WARNING] Max depth reached at {current_path}")
            return
        
        if not mssparkutils.fs.exists(current_path):
            return
        
        try:
            entries = mssparkutils.fs.ls(current_path)
            
            # Check if current folder has data files
            has_data = detect_data_formats_synapse(current_path, recursive=False)['any']
            
            if has_data:
                # This folder contains data files - it's a table!
                folder_name = current_path.rstrip('/').split('/')[-1]
                
                # Determine schema name from relative path
                path_parts = relative_path.strip('/').split('/') if relative_path else []
                
                if path_parts:
                    schema_name = path_parts[0]  # First folder level is the schema
                else:
                    schema_name = "default"  # Tables directly under base_path go to 'default' schema
                
                table_folders.append({
                    'path': current_path,
                    'table_name': folder_name,
                    'schema_name': schema_name,
                    'relative_path': relative_path.lstrip('/')
                })
                print(f"[DISCOVERED] Table: {schema_name}.{folder_name} (path: {relative_path}/{folder_name})")
            else:
                # No data files here, check subdirectories
                has_subdirs = False
                for entry in entries:
                    if entry.isDir:
                        # Skip hidden/system folders
                        if entry.name.startswith('_') or entry.name.startswith('.'):
                            continue
                        # Skip partition folders
                        if '=' in entry.name:
                            continue
                        
                        has_subdirs = True
                        new_relative = f"{relative_path}/{entry.name}" if relative_path else entry.name
                        recurse(entry.path, current_depth + 1, new_relative)
                
                # If no subdirectories and no data, it's an empty folder
                if not has_subdirs:
                    print(f"[SKIPPED] Empty folder: {relative_path or current_path}")
        
        except Exception as e:
            print(f"[ERROR] Failed to process {current_path}: {str(e)}")
    
    print(f"Scanning directory tree from: {base_path}")
    recurse(base_path, 0)
    print(f"Found {len(table_folders)} table(s) with data files\n")
    
    return table_folders

# --- Function: Check if Table is Partitioned ---
def check_if_partitioned(ldb_name, schema_name, table):
    """
    Check if a table is partitioned by attempting to show partitions.
    """
    full_table_name = f"{ldb_name}.{schema_name}.{table}"
    try:
        partitions = spark.sql(f"SHOW PARTITIONS {full_table_name}").collect()
        return len(partitions) > 0 or True
    except Exception as e:
        if "not a partitioned table" in str(e).lower() or "not allowed" in str(e).lower():
            return False
        else:
            print(f"[WARNING] Could not determine partition status for {table}: {str(e)}")
            return False

# --- Function: Process Tables ---
def process_tables(table_info_list, ldb_name):
    """
    Process a list of table folders with schema support.
    """
    print("Processing tables...")
    print("="*100)
    
    # Group tables by schema to create schemas first
    schemas = set([t['schema_name'] for t in table_info_list])
    
    print(f"\n[INFO] Creating schemas: {', '.join(schemas)}")
    for schema in schemas:
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ldb_name}.{schema}")
            print(f"[SUCCESS] Schema {schema} ready")
        except Exception as e:
            print(f"[ERROR] Failed to create schema {schema}: {str(e)}")
    
    print("\n" + "="*100)

    for table_info in table_info_list:
        loc = table_info['path']
        table = table_info['table_name']
        schema = table_info['schema_name']
        relative = table_info['relative_path']
        
        full_table_name = f"{ldb_name}.{schema}.{table}"
        
        print(f"\n[PROCESSING] Table: {schema}.{table}")
        if relative:
            print(f"[LOCATION] Storage path: {relative}/")
        
        # Check if table exists in the specific schema
        table_exists = bool(spark.sql(f"SHOW TABLES IN {ldb_name}.{schema} LIKE '{table}'").collect())

        if detect_data_formats_synapse(loc)['any']:
            print(f"[INFO] Files found for table: {schema}.{table}")
            if table_exists:
                print(f"[INFO] Checking schema for existing table: {schema}.{table}")
                try:
                    container_schema = spark.read.format("parquet").load(loc).schema
                    ldb_schema = spark.table(full_table_name).schema
                    if container_schema.simpleString() == ldb_schema.simpleString():
                        print(f"[SUCCESS] {schema}.{table} table already exists, and schemas match. No updates required.\n")
                    else:
                        print(f"[WARNING] {schema}.{table} table exists, but schemas don't match.")
                        print(f"[ACTION] Recreating {schema}.{table} in {ldb_name} with updated schema.\n")
                        spark.sql(f"DROP TABLE {full_table_name}")
                        spark.sql(f"CREATE TABLE {full_table_name} USING PARQUET LOCATION '{loc}'")
                        
                        # Check if storage location has partitions
                        if has_partitions_in_storage(loc):
                            print(f"[INFO] Partitioned table detected. Running MSCK REPAIR...")
                            spark.sql(f"MSCK REPAIR TABLE {full_table_name}")
                            print(f"[SUCCESS] {schema}.{table} table schema refreshed and partitions repaired.\n")
                        else:
                            print(f"[SUCCESS] {schema}.{table} table schema refreshed (non-partitioned table).\n")
                except Exception as e:
                    print(f"[ERROR] Failed to process existing table {schema}.{table}: {str(e)}\n")
            else:
                print(f"[INFO] {schema}.{table} table does not exist. Creating in {ldb_name}.\n")
                try:
                    spark.sql(f"CREATE TABLE {full_table_name} USING PARQUET LOCATION '{loc}'")
                    
                    # Check if storage location has partitions
                    if has_partitions_in_storage(loc):
                        print(f"[INFO] Partitioned table detected. Running MSCK REPAIR...")
                        spark.sql(f"MSCK REPAIR TABLE {full_table_name}")
                        print(f"[SUCCESS] {schema}.{table} table created successfully and partitions repaired.\n")
                    else:
                        print(f"[SUCCESS] {schema}.{table} table created successfully (non-partitioned table).\n")
                except Exception as e:
                    print(f"[ERROR] Failed to create table {schema}.{table}: {str(e)}\n")
        else:
            print(f"[WARNING] {schema}.{table} location {loc} appears to have no valid lake data files. Skipping.\n")

    print("="*100)
    print("[COMPLETED] Lake Database table creation and schema validation complete.")


# --- Main Execution ---
table_folders = find_table_folders(base_path, max_depth=5)

# Ensure database exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS {ldb_name}")

# Process tables
if table_folders:
    process_tables(table_folders, ldb_name)
else:
    print("[WARNING] No table folders found with data files.")
