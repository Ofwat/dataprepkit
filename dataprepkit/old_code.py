import uuid
target_table_name = "tbl_map_measure"
batch_id = uuid.uuid4().hex
env = "prod"

import com.microsoft.spark.fabric # needed to write to warehouse

import pandas as pd
from sqlalchemy import create_engine, text, inspect
from datetime import datetime


# ===================================================================
# Detect dataframe type (pandas vs spark)
# ===================================================================
def detect_df_type(df):
    try:
        import pyspark.sql.dataframe as pyspark_df
        if isinstance(df, pyspark_df.DataFrame):
            return "spark"
    except ImportError:
        pass
    if isinstance(df, pd.DataFrame):
        return "pandas"
    raise TypeError(f"Unsupported DataFrame type: {type(df)}")


# ===================================================================
# Map SQLite → Spark types
# ===================================================================
def map_sqlite_to_spark(sqlite_type):
    t = sqlite_type.upper()
    from pyspark.sql.types import IntegerType, StringType, FloatType, BooleanType, TimestampType
    if "INT" in t:
        return IntegerType()
    elif "CHAR" in t or "CLOB" in t or "TEXT" in t:
        return StringType()
    elif "REAL" in t or "FLOA" in t or "DOUB" in t:
        return FloatType()
    elif "DATE" in t or "TIME" in t:
        return TimestampType()
    elif "BOOL" in t or "BIT" in t:
        return BooleanType()
    else:
        return StringType()


# ===================================================================
# Cast Spark DF to SQLite schema
# ===================================================================
def cast_spark_df_to_sqlite_schema(spark_df, sqlite_schema):
    from pyspark.sql import functions as F

    for col_name, col_info in sqlite_schema.items():
        if col_name in spark_df.columns:
            spark_type = map_sqlite_to_spark(col_info["type"])
            spark_df = spark_df.withColumn(col_name, F.col(col_name).cast(spark_type))

    return spark_df


# ===================================================================
# Spark write (Synapse)
# ===================================================================
def write_spark_table(
    spark_df,
    sqlite_schema,
    workspace_ID=None,
    warehouse=None,
    target_schema=None,
    target_table=None
):
    if not (workspace_ID and warehouse and target_schema and target_table):
        print("⚠️ Spark write skipped — missing workspace/table info.")
        return

    Constants = type("Constants", (), {"WorkspaceId": "WorkspaceId"})  # stub

    print(f"🚀 Writing via Synapse Spark: {warehouse}.{target_schema}.{target_table}")
    spark_df.write.option(Constants.WorkspaceId, workspace_ID)\
        .mode("overwrite")\
        .synapsesql(f"{warehouse}.{target_schema}.{target_table}")


# ===================================================================
# Read SQLite schema
# ===================================================================
def get_sqlite_schema(sqlite_conn, table_name):
    schema_df = pd.read_sql(f"PRAGMA table_info({table_name});", sqlite_conn)
    return {
        row["name"]: {"type": row["type"], "notnull": bool(row["notnull"])}
        for _, row in schema_df.iterrows()
    }


# ===================================================================
# SQLite → SQL Server type mapping
# ===================================================================
def map_sqlite_to_mssql(sqlite_type):
    t = sqlite_type.upper()
    if "INT" in t:
        return "INT"
    elif "CHAR" in t or "CLOB" in t or "TEXT" in t:
        return "VARCHAR(8000)"
    elif "REAL" in t or "FLOA" in t or "DOUB" in t:
        return "FLOAT"
    elif "DATE" in t or "TIME" in t:
        return "DATETIME2(6)"
    elif "BOOL" in t or "BIT" in t:
        return "BIT"
    else:
        return "VARCHAR(8000)"


# ===================================================================
# Align Pandas DF to schema
# ===================================================================
def align_df_to_schema(df, schema):
    aligned = df.copy()
    for col in schema:
        if col not in aligned.columns:
            aligned[col] = pd.NA
    aligned = aligned[list(schema.keys())]
    return aligned


# ===================================================================
# Row growth validation
# ===================================================================
def validate_row_growth(conn, df, target_table, schema_name=""):
    schema_str = f"[{schema_name}]." if schema_name else ""
    try:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {schema_str}[{target_table}]"))
        old_count = result.scalar() or 0
    except Exception:
        old_count = 0

    new_count = len(df)

    print(f"📊 Row count check: existing={old_count}, new={new_count}")

    if old_count > 0 and new_count < old_count:
        raise ValueError(
            f"❌ Row count validation failed: new {new_count} < existing {old_count}"
        )

    return old_count, new_count


# ===================================================================
# Create table in SQL Server
# ===================================================================
def create_table_mssql(conn, table_name, schema_name, schema):
    cols_sql = []
    for col_name, col_info in schema.items():
        sql_type = map_sqlite_to_mssql(col_info["type"])
        not_null = "NOT NULL" if col_info["notnull"] else "NULL"
        cols_sql.append(f"[{col_name}] {sql_type} {not_null}")

    schema_str = f"[{schema_name}]." if schema_name else ""
    create_sql = f"CREATE TABLE {schema_str}[{table_name}] (\n    " + ",\n    ".join(cols_sql) + "\n)"
    print(f"🛠 Creating table:\n{create_sql}")
    conn.execute(text(create_sql))


# ===================================================================
# Schema drift detection
# ===================================================================
def detect_schema_drift(
    target_conn,
    target_table,
    sqlite_schema,
    schema_name="dbo",
    allow_drift_checks=None
):
    if allow_drift_checks is None:
        allow_drift_checks = {
            "missing_columns": False,
            "extra_columns": True,
            "nullability": False,
            "type_mismatch": False,
        }

    q = f"""
    SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{schema_name}'
      AND TABLE_NAME = '{target_table}'
    """
    target_df = pd.read_sql(q, target_conn)

    target_schema = {
        row["COLUMN_NAME"]: {
            "notnull": row["IS_NULLABLE"] == "NO",
            "type": row["DATA_TYPE"].upper()
        }
        for _, row in target_df.iterrows()
    }

    sqlite_cols = set(sqlite_schema.keys())
    target_cols = set(target_schema.keys())

    missing_cols = sqlite_cols - target_cols
    extra_cols = target_cols - sqlite_cols

    errors = []

    if missing_cols and not allow_drift_checks.get("missing_columns"):
        errors.append(f"Missing columns: {sorted(missing_cols)}")

    if extra_cols and not allow_drift_checks.get("extra_columns"):
        errors.append(f"Extra columns: {sorted(extra_cols)}")

    # Nullability
    for c in sqlite_cols & target_cols:
        if sqlite_schema[c]["notnull"] != target_schema[c]["notnull"]:
            if not allow_drift_checks.get("nullability"):
                errors.append(f"Nullability mismatch: {c}")

    # Type mismatch
    for c in sqlite_cols & target_cols:
        mapped = map_sqlite_to_mssql(sqlite_schema[c]["type"]).split("(")[0]
        target = target_schema[c]["type"]
        if mapped != target and not allow_drift_checks.get("type_mismatch"):
            errors.append(f"Type mismatch: {c} SQLite={mapped}, Target={target}")

    if errors:
        raise ValueError("Schema drift detected:\n" + "\n".join(errors))


# ===================================================================
# Master replace function
# ===================================================================
def replace_table_using_sqlite_schema(
    target_engine,
    df,
    target_table,
    target_schema,
    sqlite_schema_conn,
    sqlite_table,
    enforce_row_growth=True,
    allow_drift_checks=None,
    drop_backup=True,
    workspace_ID=None,
    warehouse=None
):

    # Load schema once
    sqlite_schema = get_sqlite_schema(sqlite_schema_conn, sqlite_table)

    df_type = detect_df_type(df)

    # =======================================================
    # CASTING BRANCH
    # =======================================================
    if df_type == "pandas":
        print("📦 Casting pandas DataFrame...")
        df_casted = align_df_to_schema(df, sqlite_schema)
        df_for_validation = df_casted

    else:
        print("⚡ Casting Spark DataFrame...")
        df_casted = cast_spark_df_to_sqlite_schema(df, sqlite_schema)

        print("📤 Converting Spark → pandas for validation...")
        df_for_validation = df_casted.toPandas()

    # =======================================================
    # VALIDATION (always pandas)
    # =======================================================
    inspector = inspect(target_engine)
    backup_table = None

    with target_engine.begin() as conn:

        # Table exists?
        if target_table in inspector.get_table_names(schema=target_schema):

            detect_schema_drift(
                conn,
                target_table,
                sqlite_schema,
                schema_name=target_schema,
                allow_drift_checks=allow_drift_checks
            )

            if enforce_row_growth:
                validate_row_growth(
                    conn, df_for_validation, target_table, schema_name=target_schema
                )

            # Backup old table
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            backup_table = f"{target_table}_{timestamp}"

            print(f"🔄 Backup existing: {target_schema}.{target_table} → {backup_table}")
            conn.execute(text(f"EXEC sp_rename '{target_schema}.{target_table}', '{backup_table}'"))

        # Recreate table
        create_table_mssql(conn, target_table, target_schema, sqlite_schema)

        # =======================================================
        # WRITING (branch by input type)
        # =======================================================
        if df_type == "pandas":
            print("📝 Writing using pandas to_sql()...")
            df_casted.to_sql(
                name=target_table,
                con=conn,
                schema=target_schema,
                if_exists="append",
                index=False,
            )
        else:
            print("🚀 Writing using Spark Synapse...")
            write_spark_table(
                spark_df=df_casted,
                sqlite_schema=sqlite_schema,
                workspace_ID=workspace_ID,
                warehouse=warehouse,
                target_schema=target_schema,
                target_table=target_table
            )

        # Cleanup backup
        if backup_table and drop_backup:
            print(f"🗑 Dropping backup: {backup_table}")
            conn.execute(text(f"DROP TABLE [{target_schema}].[{backup_table}]"))

    print("✅ Replace complete.")


import pandas as pd
from sqlalchemy.engine import Engine


def load_dim_tables(
    engine: Engine,
    table_names: list[str],
    schema_name: str = "Dimensions"
) -> dict[str, pd.DataFrame]:
    """
    Load a list of dimension tables from a database schema into pandas DataFrames.

    :param engine: SQLAlchemy Engine connected to the database.
    :param table_names: List of table names to load.
    :param schema_name: Name of the schema containing the tables. Defaults to 'Dimensions'.
    :return: Dictionary of {table_name: DataFrame}.
    """
    dim_table_dfs = {}

    for table in table_names:
        try:
            df = pd.read_sql_table(table_name=table, con=engine, schema=schema_name)
            dim_table_dfs[table] = df
        except Exception as e:
            print(f"⚠️ Failed to load table '{table}' from schema '{schema_name}': {e}")

    return dim_table_dfs


import sqlite3
import pandas as pd
from datetime import datetime
import hashlib

# -----------------------
# Utility: Compute a consistent MD5 hash for a DataFrame row
# -----------------------
def _compute_row_hash(row, exclude_cols=None):
    """Compute MD5 hash for a given row excluding certain columns."""
    exclude_cols = exclude_cols or []
    items = [f"{col}={row[col]}" for col in sorted(row.index) if col not in exclude_cols]
    raw_str = "|".join(map(str, items))
    return hashlib.md5(raw_str.encode("utf-8")).hexdigest()

# -----------------------
# Function to create table and index dynamically
# -----------------------
def initialize_db(conn, table_name, columns, key_column="id",
                  effective_date_column='effective_datetime',
                  end_date_column='end_datetime',
                  latest_ind_column='latest_ind',
                  surrogate_key_column='surrogate_key',
                  row_hash_column='row_hash',
                  numeric_key_column='key_column_numeric',
                  start_sequence_at=100):
    """Create a SQLite table dynamically with system columns."""
    cursor = conn.cursor()
    column_clauses = []
    for col_name, col_def in columns.items():
        if isinstance(col_def, str):
            clause = f"{col_name} {col_def}"
        else:
            col_type = col_def.get("type", "TEXT")
            parts = [f"{col_name} {col_type}"]
            if col_def.get("pk", False):
                parts.append("PRIMARY KEY")
            if not col_def.get("nullable", True):
                parts.append("NOT NULL")
            if col_def.get("unique", False):
                parts.append("UNIQUE")
            if "default" in col_def:
                default_val = col_def["default"]
                if isinstance(default_val, str):
                    default_val = f"'{default_val}'"
                parts.append(f"DEFAULT {default_val}")
            if "check" in col_def:
                parts.append(f"CHECK ({col_def['check']})")
            clause = " ".join(parts)
        column_clauses.append(clause)

    # Add system columns
    column_clauses.extend([
        f"{row_hash_column} TEXT NOT NULL",
        f"{effective_date_column} DATETIME NOT NULL",
        f"{end_date_column} DATETIME",
        f"{latest_ind_column} BOOLEAN NOT NULL",
    ])

    query = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            {surrogate_key_column} INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            {numeric_key_column} INTEGER NOT NULL,
            {", ".join(column_clauses)}
        )
    '''
    cursor.execute(query)
    cursor.execute(f'''
        CREATE INDEX IF NOT EXISTS idx_{table_name}_{key_column} ON {table_name}({key_column})
    ''')
    conn.commit()

    # Ensure sequence starts at start_sequence_at
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'")
    if cursor.fetchone():
        cursor.execute(f"SELECT seq FROM sqlite_sequence WHERE name = ?", (table_name,))
        seq_row = cursor.fetchone()
        if seq_row is None:
            cursor.execute(f"INSERT INTO sqlite_sequence (name, seq) VALUES (?, ?)",
                           (table_name, start_sequence_at - 1))
        elif seq_row[0] < start_sequence_at - 1:
            cursor.execute(f"UPDATE sqlite_sequence SET seq = ? WHERE name = ?",
                           (start_sequence_at - 1, table_name))
        conn.commit()

# -----------------------
# Insert new records
# -----------------------
def _batch_insert_new_records(conn, table_name, new_rows, current_dt,
                              effective_date_column='effective_datetime',
                              end_date_column='end_datetime',
                              latest_ind_column='latest_ind',
                              numeric_key_column='key_column_numeric'):
    if new_rows.empty:
        return
    cursor = conn.cursor()
    columns = list(new_rows.columns)
    cursor.executemany(f'''
        INSERT INTO {table_name} ({", ".join(columns)}, {effective_date_column}, {end_date_column}, {latest_ind_column}, {numeric_key_column})
        VALUES ({", ".join("?" for _ in columns)}, ?, ?, ?, ?)
    ''', [
        tuple(row[col] for col in columns) + (current_dt, None, True, row[numeric_key_column])
        for _, row in new_rows.iterrows()
    ])
    conn.commit()


import sqlite3
import pandas as pd
from datetime import datetime
import hashlib

# -----------------------
# Utility: Compute a consistent MD5 hash for a DataFrame row
# -----------------------
def _compute_row_hash(row, exclude_cols=None):
    """Compute MD5 hash for a given row excluding certain columns."""
    exclude_cols = exclude_cols or []
    items = [f"{col}={row[col]}" for col in sorted(row.index) if col not in exclude_cols]
    raw_str = "|".join(map(str, items))
    return hashlib.md5(raw_str.encode("utf-8")).hexdigest()


# -----------------------
# Expire existing records
# -----------------------
def _expire_existing_records(conn, table_name, key_values, current_dt,
                             key_column="id",
                             end_date_column='end_datetime',
                             latest_ind_column='latest_ind'):
    """Marks existing active records as expired."""
    if not key_values:
        return
    cursor = conn.cursor()
    cursor.executemany(f'''
        UPDATE {table_name}
        SET {end_date_column} = ?, {latest_ind_column} = 0
        WHERE {key_column} = ? AND {end_date_column} IS NULL
    ''', [(current_dt, k) for k in key_values])
    conn.commit()


# -----------------------
# Expire deleted records
# -----------------------
def _expire_deleted_records(conn, table_name, incoming_keys, current_dt,
                            key_column="id",
                            end_date_column='end_datetime',
                            latest_ind_column='latest_ind'):
    """Expire rows that exist in DB but are missing from incoming batch."""
    cursor = conn.cursor()
    existing_df = pd.read_sql_query(
        f'''
        SELECT {key_column}
        FROM {table_name}
        WHERE {latest_ind_column} = 1 AND {end_date_column} IS NULL
        ''',
        conn
    )
    existing_keys = set(existing_df[key_column])
    keys_to_delete = existing_keys - set(incoming_keys)
    if keys_to_delete:
        cursor.executemany(f'''
            UPDATE {table_name}
            SET {end_date_column} = ?, {latest_ind_column} = 0
            WHERE {key_column} = ? AND {end_date_column} IS NULL
        ''', [(current_dt, k) for k in keys_to_delete])
        conn.commit()
        print(f"✅ Expired {len(keys_to_delete)} deleted records")


# -----------------------
# Batch insert SCD2 rows
# -----------------------
def _batch_insert_new_records(
    conn,
    table_name,
    df,
    current_dt,
    effective_date_column,
    end_date_column,
    latest_ind_column,
    numeric_key_column
):
    cols = df.columns.tolist()
    placeholders = ",".join("?" for _ in cols)
    cursor = conn.cursor()
    cursor.executemany(
        f"INSERT INTO {table_name} ({','.join(cols)}) VALUES ({placeholders})",
        df.where(pd.notnull(df), None).values.tolist(),
    )
    conn.commit()


# -----------------------
# Insert SCD2 with proper non-reusable numeric keys
# -----------------------
def insert_scd2(
    conn,
    table_name,
    new_data,
    key_column='id',
    row_hash_column='row_hash',
    effective_date_column='effective_datetime',
    end_date_column='end_datetime',
    latest_ind_column='latest_ind',
    numeric_key_column='key_column_numeric',
    start_key_sequence_at=100,
    exclude_hash_columns=None,
    mode='full',
    deleted_flag_column='deleted',
    partial_threshold=50
):
    """
    SCD2 upsert with NON-REUSABLE numeric surrogate keys.
    New numeric keys always increase, even if older rows were deleted.
    Numeric keys for updates are reused.
    """

    current_dt = datetime.now().isoformat(sep=' ', timespec='seconds')
    new_data = new_data.copy()

    # Columns excluded from row_hash
    exclude_cols = {
        effective_date_column, end_date_column, latest_ind_column,
        row_hash_column, numeric_key_column, 'is_deleted'
    }
    if exclude_hash_columns:
        exclude_cols.update(exclude_hash_columns)

    # Compute row hashes
    new_data[row_hash_column] = new_data.apply(
        lambda r: _compute_row_hash(r, exclude_cols), axis=1
    )
    new_data[effective_date_column] = current_dt
    new_data[end_date_column] = None
    new_data[latest_ind_column] = True

    cursor = conn.cursor()

    try:
        cursor.execute("BEGIN TRANSACTION")

        incoming_keys = new_data[key_column].tolist()
        if not incoming_keys:
            print("No data provided.")
            conn.rollback()
            return

        print(f"ℹ️ Running in {mode.upper()} mode with {len(new_data)} rows")

        # =====================================================
        # 1️⃣ HANDLE DELETES
        # =====================================================
        expired_count = 0

        if mode == 'full':
            existing_df = pd.read_sql_query(
                f'''
                SELECT {key_column}
                FROM {table_name}
                WHERE {latest_ind_column} = 1 AND {end_date_column} IS NULL
                ''',
                conn
            )
            existing_keys = set(existing_df[key_column])
            keys_to_delete = existing_keys - set(incoming_keys)
            if keys_to_delete:
                _expire_existing_records(
                    conn, table_name, list(keys_to_delete), current_dt,
                    key_column, end_date_column, latest_ind_column
                )
                expired_count = len(keys_to_delete)

        elif mode == 'delta':
            if deleted_flag_column not in new_data.columns:
                new_data[deleted_flag_column] = False

            expired_count = new_data[deleted_flag_column].sum()
            if expired_count > 0:
                ids_to_expire = new_data.loc[new_data[deleted_flag_column], key_column].tolist()
                _expire_existing_records(
                    conn, table_name, ids_to_expire, current_dt,
                    key_column, end_date_column, latest_ind_column
                )

        # =====================================================
        # 2️⃣ FETCH EXISTING ACTIVE ROWS
        # =====================================================
        placeholders = ",".join("?" for _ in incoming_keys)
        existing_df = pd.read_sql_query(
            f'''
            SELECT {key_column}, {row_hash_column}, {numeric_key_column}
            FROM {table_name}
            WHERE {key_column} IN ({placeholders})
            AND {end_date_column} IS NULL
            ''',
            conn,
            params=incoming_keys
        )

        # =====================================================
        # 3️⃣ DETECT INSERTS/UPDATES
        # =====================================================
        merged = new_data.merge(
            existing_df,
            on=key_column,
            how='left',
            suffixes=('_new', '_old')
        )

        changed_rows = merged[
            (merged[f"{row_hash_column}_old"].isna()) |
            (merged[f"{row_hash_column}_new"] != merged[f"{row_hash_column}_old"])
        ]

        if changed_rows.empty:
            print(f"✅ No inserts/updates detected. Soft-deleted: {expired_count}")
            conn.rollback()
            return

        # =====================================================
        # 4️⃣ EXPIRE CHANGED ROWS
        # =====================================================
        ids_to_expire = [
            i for i in changed_rows[key_column].tolist()
            if i in set(existing_df[key_column])
        ]

        _expire_existing_records(
            conn, table_name, ids_to_expire, current_dt,
            key_column, end_date_column, latest_ind_column
        )

        updated_count = len(ids_to_expire)

        # =====================================================
        # 5️⃣ ASSIGN NUMERIC KEYS
        # =====================================================
        # Pull ALL numeric keys ever used (not only latest)
        numeric_history_df = pd.read_sql_query(
            f'''
            SELECT {numeric_key_column}
            FROM {table_name}
            ''',
            conn
        )

        next_key = numeric_history_df[numeric_key_column].max() + 1 if not numeric_history_df.empty else start_key_sequence_at

        # New or changed rows to insert
        new_rows = new_data[new_data[key_column].isin(changed_rows[key_column])].copy()

        # Map of existing numeric keys (active records only)
        existing_numeric = dict(zip(existing_df[key_column], existing_df[numeric_key_column]))

        assigned_keys = []
        for row_key in new_rows[key_column]:
            if row_key in existing_numeric:
                # UPDATE → reuse numeric key
                assigned_keys.append(existing_numeric[row_key])
            else:
                # INSERT → assign new numeric key
                assigned_keys.append(next_key)
                next_key += 1

        new_rows[numeric_key_column] = assigned_keys

        # =====================================================
        # 6️⃣ INSERT NEW & UPDATED ROWS
        # =====================================================
        inserted_count = len(new_rows) - updated_count

        _batch_insert_new_records(
            conn, table_name, new_rows, current_dt,
            effective_date_column, end_date_column,
            latest_ind_column, numeric_key_column
        )

        conn.commit()
        print(f"✅ Inserted: {inserted_count}, Updated: {updated_count}, Soft-deleted: {expired_count}")

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Error during insert_scd2: {e}")

import os
import pandas as pd
from datetime import datetime

def archive_dataframe(df: pd.DataFrame, table_name: str, batch_id: str, base_dir: str) -> str:
    """
    Converts all columns to strings and saves the DataFrame as a parquet file
    under a dedicated folder for the table, using a sortable timestamped filename.

    Directory structure:
        {base_dir}/{table_name}/
            {table_name}__{timestamp}_BATCH{batch_id}.parquet

    Example:
        archive/tbl_d_region/tbl_d_region__20251030T142315_BATCH42.parquet
    """
    # ---- Validation ----
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")
    if not table_name:
        raise ValueError("table_name must be provided")
    if not batch_id:
        raise ValueError("batch_id must be provided")
    if not base_dir:
        raise ValueError("base_dir must be provided")

    # ---- Prepare directories ----
    table_dir = os.path.join(base_dir, table_name)
    os.makedirs(table_dir, exist_ok=True)

    # ---- Convert all columns to string ----
    df_str = df.astype(str)

    # ---- Timestamp for lexicographic sorting ----
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S%f")[:-3]  # e.g. 20251030T142315123

    # ---- Construct file path ----
    filename = f"{table_name}__{timestamp}__BATCH{batch_id}.parquet"
    file_path = os.path.join(table_dir, filename)

    # ---- Save file ----
    df_str.to_parquet(file_path, index=False, compression="snappy")

    print(f"✅ Archived DataFrame to: {file_path}")
    return {
        "table": table_name,
        "batch_id": batch_id,
        "timestamp": timestamp,
        "file_path": file_path
    }


!pip install ofwat-dataprepkit

import pandas as pd
import dataprepkit


from notebookutils import credentials as _default_credentials
import sqlalchemy as _sa
import struct as _struct
import pyodbc as _pyodbc
import re as _re

def _get_latest_sql_driver() -> str:
    drivers = _pyodbc.drivers() # pylint: disable=I1101
    sql_drivers = [d for d in drivers if "SQL Server" in d or "ODBC Driver" in d]
    if not sql_drivers:
        raise RuntimeError("No suitable ODBC driver for SQL Server found.")

    def extract_version(name: str) -> int:
        match = _re.search(r"(\d+)", name)
        return int(match.group(1)) if match else 0

    latest_driver = max(sql_drivers, key=extract_version)
    # _logger.info("Using ODBC driver: %s", latest_driver)
    return latest_driver

def get_fabric_warehouse_engine(
        sql_endpoint: str,
        port: int = 1433,
        credentials=_default_credentials
    ) -> _sa.engine.Engine:
    # pylint: disable=C0301
    """
    Create and return a SQLAlchemy engine connected to an Azure Fabric data warehouse.

    Args:
        sql_endpoint (str): The Fabric SQL endpoint to connect to.
        port (int, optional): The TCP port for the SQL server. Defaults to 1433.
        credentials (optional): An object with a getToken(resource) method. Defaults to Fabric's credentials.

    Returns:
        _sa.engine.Engine: A SQLAlchemy Engine instance connected to the Fabric warehouse.

    Raises:
        ValueError: If `sql_endpoint` is empty or None.
        RuntimeError: If no suitable ODBC driver is found.
        Exception: If token retrieval or engine creation fails.
    """
    if not sql_endpoint:
        raise ValueError("sql_endpoint is required and cannot be empty.")

    try:
        driver = _get_latest_sql_driver()
        server = f"{sql_endpoint},{port}"

        token = credentials.getToken("https://database.windows.net/").encode("UTF-16-LE")
        token_struct = _struct.pack(f"<I{len(token)}s", len(token), token)

        connection_string = f"DRIVER={{{driver}}};SERVER={server}"
        connection_url = _sa.engine.URL.create(
            "mssql+pyodbc",
            query={"odbc_connect": connection_string}
        )

        engine = _sa.create_engine(
            connection_url,
            connect_args={"attrs_before": {1256: token_struct}},
            pool_pre_ping=True,
            pool_recycle=3600,
            # fast_executemany=True,
        )

        # _logger.info("Successfully created Fabric SQL engine.")
        return engine

    except Exception as ex:
        # _logger.error("Failed to create Fabric engine: %s", ex, exc_info=True)
        raise


env = env.upper()

if env == "PROD":
    sql_connection_string = "byx2sqtktgzedbish3jdpk4dcm-sbeaaeq5h43utfrbvrcuxytome.datawarehouse.fabric.microsoft.com"
else:
    sql_connection_string = "byx2sqtktgzedbish3jdpk4dcm-vd6yg3wowq2enahjchvizod6qa.datawarehouse.fabric.microsoft.com"

# engine = create_engine(sql_connection_string, fast_executemany=True)

engine = get_fabric_warehouse_engine(sql_connection_string)
dataprepkit.helpers.connectors.warehouse.validate_fabric_warehouse_engine(engine)


import sempy.fabric as fabric
workspaces_df = fabric.list_workspaces()

WS__UUID = workspaces_df[workspaces_df["Name"] == "Ocean_Data_PROD"]["Id"].to_list()[0]
ws_items_df = fabric.list_items(workspace=WS__UUID)
LAKEHOUSE_UUID = ws_items_df[(ws_items_df["Type"] == "Lakehouse") & (ws_items_df["Display Name"] == "Dimension_Source_Data")]["Id"].to_list()[0]
LAKEHOUSE_BASE_PATH = f"abfss://{WS__UUID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_UUID}"

notebookutils.fs.unmount("/home/trusted-service-user/mounts/Source_Data")

counter = 0
while counter < 20:
    try:
        notebookutils.fs.mount(  
            LAKEHOUSE_BASE_PATH,  
            "/home/trusted-service-user/mounts/Source_Data"
        )
        SOURCE_DATA_PATH = notebookutils.fs.getMountPath("/home/trusted-service-user/mounts/Source_Data")
        break
    except:
        counter += 1
        time.sleep(1)

try:
    notebookutils.fs.mount(
        LAKEHOUSE_BASE_PATH,  
        "/home/trusted-service-user/mounts/Source_Data"
    )
    SOURCE_DATA_PATH = notebookutils.fs.getMountPath("/home/trusted-service-user/mounts/Source_Data")
except Exception as e: 
    RuntimeError(f"Failed to mount after 20 attempts: {e}")

import sempy.fabric as fabric
workspaces_df = fabric.list_workspaces()

WS__UUID__TARGET = workspaces_df[workspaces_df["Name"] == f"Ocean_Data_{env}"]["Id"].to_list()[0]
ws_items_df = fabric.list_items(workspace=WS__UUID__TARGET)
LAKEHOUSE_UUID__SOURCE = ws_items_df[(ws_items_df["Type"] == "Lakehouse") & (ws_items_df["Display Name"] == "Dimension_Source_Data")]["Id"].to_list()[0]
LAKEHOUSE_BASE_PATH = f"abfss://{WS__UUID__TARGET}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_UUID__SOURCE}"
WAREHOUSE_UUD = ws_items_df[(ws_items_df["Type"] == "Warehouse") & (ws_items_df["Display Name"] == "OCEAN_Data_Collection")]["Id"].to_list()[0]

notebookutils.fs.unmount("/home/trusted-service-user/mounts/Archive_Data")

counter = 0
while counter < 20:
    try:
        notebookutils.fs.mount(  
            LAKEHOUSE_BASE_PATH,  
            "/home/trusted-service-user/mounts/Archive_Data"
        )
        ARCHIVE_DATA_PATH = notebookutils.fs.getMountPath("/home/trusted-service-user/mounts/Archive_Data")
        break
    except:
        counter += 1
        time.sleep(1)

try:
    notebookutils.fs.mount(
        LAKEHOUSE_BASE_PATH,  
        "/home/trusted-service-user/mounts/Archive_Data"
    )
    ARCHIVE_DATA_PATH = notebookutils.fs.getMountPath("/home/trusted-service-user/mounts/Archive_Data")
except Exception as e: 
    RuntimeError(f"Failed to mount after 20 attempts: {e}")


import pandas as pd


class CommonDimensionTransform:
    def __init__(self, df_dict: dict[str, pd.DataFrame]):
        """
        :param df_dict: Dictionary of DataFrames.
        """
        self.df_dict = df_dict

    def run_etl(self, df_key: str) -> pd.DataFrame:
        """
        Simply returns the input DataFrame from df_dict without modification.

        :param df_key: The key of the DataFrame in `df_dict` to return
        :return: The same DataFrame as provided in df_dict[df_key]
        """
        if df_key not in self.df_dict:
            raise KeyError(f"'{df_key}' not found in df_dict.")

        # Return the same DataFrame unchanged
        return self.df_dict[df_key].copy()


import pandas as pd
from datetime import datetime


class IntervalDimensionTransform:
    def __init__(self, df_dict: dict[str, pd.DataFrame]):
        """
        :param df_dict: Dictionary of DataFrames.
        """
        self.df_dict = df_dict

    def run_etl(self, df_key: str) -> pd.DataFrame:
        """
        Retrieves a DataFrame from df_dict, filters to Current_Ind = True,
        cleans date strings, converts Interval_Start_Date and Interval_End_Date
        into ISO-8601 format strings, and returns the transformed DataFrame.

        :param df_key: The key of the DataFrame in `df_dict` to process
        :return: Transformed DataFrame with ISO-8601 formatted date strings
        """
        if df_key not in self.df_dict:
            raise KeyError(f"'{df_key}' not found in df_dict.")

        df = self.df_dict[df_key].copy()

        input_format = "%d/%m/%Y %H:%M"

        for col in ["Interval_Start_Date", "Interval_End_Date"]:
            if col in df.columns:
                # Clean up invisible or stray characters
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(r"[^\x00-\x7F]+", " ", regex=True)
                    .str.strip()
                )

                def to_iso8601_safe(x: str) -> str | None:
                    try:
                        dt = datetime.strptime(x, input_format)
                        return dt.isoformat(timespec="seconds")
                    except Exception:
                        return None

                df[col] = df[col].apply(to_iso8601_safe)

        return df


import pandas as pd

class CompanyDimensionTransform:
    def __init__(self, df_dict: dict[str, pd.DataFrame]):
        """
        :param df_dict: Dictionary of DataFrames. Expected keys:
                        - 'tbl_d_company_type': company type dimension
                        - 'tbl_d_company_service': company service dimension
        """
        self.df_dict = df_dict

        # Validate required dimension tables
        required_keys = ['tbl_d_company_type', 'tbl_d_company_service']
        missing_keys = [k for k in required_keys if k not in df_dict]
        if missing_keys:
            raise ValueError(f"Missing required DataFrames: {missing_keys}")

        # ---- Filter both dimension tables by Current_Ind == True ----
        self.company_type_df = df_dict['tbl_d_company_type'].copy()
        self.company_service_df = df_dict['tbl_d_company_service'].copy()

        for name, df in [('tbl_d_company_type', self.company_type_df),
                         ('tbl_d_company_service', self.company_service_df)]:
            if 'Current_Ind' not in df.columns:
                raise KeyError(f"'Current_Ind' column not found in {name}")

        self.company_type_df = self.company_type_df[self.company_type_df['Current_Ind'].isin([True, 'Y', 'y', 1])].copy()
        self.company_service_df = self.company_service_df[self.company_service_df['Current_Ind'].isin([True, 'Y', 'y', 1])].copy()

    @staticmethod
    def _perform_lookup(df: pd.DataFrame, dim_df: pd.DataFrame, df_key: str, dim_key: str, new_col: str) -> pd.DataFrame:
        """
        Perform left join lookup without hardcoding the ID column.
        Assumes dim_df has already been filtered to Current_Ind == True.
        """
        if df_key not in df.columns:
            raise KeyError(f"'{df_key}' not found in input DataFrame")

        if dim_key not in dim_df.columns:
            raise KeyError(f"'{dim_key}' not found in dimension DataFrame")

        if dim_df.shape[1] < 2:
            raise ValueError("Dimension table must have at least two columns (code and ID).")

        # Determine the ID column (anything except the dim_key)
        id_col_candidates = [c for c in dim_df.columns if c != dim_key]
        if not id_col_candidates:
            raise ValueError("Could not determine ID column in dimension table")
        id_col = id_col_candidates[0]

        # Rename dimension lookup column to avoid merge collision
        temp_dim_key = f"__dim_{dim_key}"
        dim_temp = dim_df.rename(columns={dim_key: temp_dim_key})

        # Merge lookup
        merged = df.merge(
            dim_temp[[temp_dim_key, id_col]],
            left_on=df_key,
            right_on=temp_dim_key,
            how='left'
        )

        # Assign new ID column name
        merged.rename(columns={id_col: new_col}, inplace=True)

        # Remove temporary column
        merged.drop(columns=[temp_dim_key], inplace=True)

        return merged

    def run_etl(self, df_key: str) -> pd.DataFrame:
        """Run ETL: perform lookups and return enriched DataFrame."""
        if df_key not in self.df_dict:
            raise KeyError(f"Input DataFrame '{df_key}' not found in df_dict.")

        company_df = self.df_dict[df_key].copy()

        # 1. Company Type lookup
        company_df = self._perform_lookup(
            company_df, self.company_type_df,
            df_key='Company_Type_Cd', dim_key='Company_Type_Cd', new_col='Company_Type_Id'
        )

        # 2. Company Service lookup
        company_df = self._perform_lookup(
            company_df, self.company_service_df,
            df_key='Service_Type_Cd', dim_key='Company_Service_Cd', new_col='Service_Type_Id'
        )

        # ---- Remove input code columns after lookups ----
        columns_to_remove = ['Company_Type_Cd', 'Service_Type_Cd']
        existing = [c for c in columns_to_remove if c in company_df.columns]
        company_df.drop(columns=existing, inplace=True)

        return company_df.reset_index(drop=True)


import pandas as pd

class RegionDimensionTransform:
    def __init__(self, df_dict: dict[str, pd.DataFrame]):
        """
        :param df_dict: Dictionary of DataFrames. Expected keys:
                        - 'tbl_d_company': Company dimension table
        """
        self.df_dict = df_dict

        # Validate required dimension tables
        required_keys = ['tbl_d_company']
        missing_keys = [k for k in required_keys if k not in df_dict]
        if missing_keys:
            raise ValueError(f"Missing required DataFrames: {missing_keys}")

        self.company_dim_df = df_dict['tbl_d_company']

    @staticmethod
    def _perform_lookup(
        df: pd.DataFrame,
        dim_df: pd.DataFrame,
        df_key: str,
        dim_key: str,
        new_col: str
    ) -> pd.DataFrame:
        """
        Perform left join lookup between an input DataFrame and a dimension DataFrame.

        - Filters dimension records where Current_Ind == True or 'Y'
        - Infers and adds ID columns from the dimension
        - Avoids column name collisions via temporary renaming
        """
        if df_key not in df.columns:
            raise KeyError(f"'{df_key}' not found in input DataFrame")

        if dim_key not in dim_df.columns:
            raise KeyError(f"'{dim_key}' not found in dimension DataFrame")

        if dim_df.shape[1] < 2:
            raise ValueError("Dimension table must have at least two columns (code and ID).")

        # Filter to current records
        if 'Current_Ind' in dim_df.columns:
            dim_df = dim_df[dim_df['Current_Ind'].isin([True, 'Y', 'y', 1])].copy()

        # Determine the ID column (any column not equal to dim_key)
        id_col_candidates = [c for c in dim_df.columns if c != dim_key]
        if not id_col_candidates:
            raise ValueError("Could not determine ID column in dimension table")

        id_col = id_col_candidates[0]

        # Rename to avoid column name conflicts
        temp_dim_key = f"__dim_{dim_key}"
        dim_temp = dim_df.rename(columns={dim_key: temp_dim_key})

        # Merge input with dimension
        merged = df.merge(
            dim_temp[[temp_dim_key, id_col]],
            left_on=df_key,
            right_on=temp_dim_key,
            how='left'
        )

        # Rename joined ID column
        merged.rename(columns={id_col: new_col}, inplace=True)
        merged.drop(columns=[temp_dim_key], inplace=True)

        return merged

    def run_etl(self, df_key: str) -> pd.DataFrame:
        """
        Run ETL for the specified input DataFrame key.

        :param df_key: Key of the input DataFrame (e.g., 'region_input')
        :return: DataFrame enriched with Company_Id
        """
        if df_key not in self.df_dict:
            raise KeyError(f"Input DataFrame '{df_key}' not found in df_dict")

        region_df = self.df_dict[df_key].copy()

        # Perform company lookup
        # Here, Organisation_Cd in the input matches Organisation_Cd in the dimension
        region_df = self._perform_lookup(
            region_df, self.company_dim_df,
            df_key='Organisation_Cd', dim_key='Organisation_Cd', new_col='Company_Id'
        )

        return region_df.reset_index(drop=True)


import pandas as pd

class DMeXMetricMappingDimensionTransform:
    def __init__(self, df_dict: dict[str, pd.DataFrame]):
        """
        :param df_dict: Dictionary of DataFrames. Expected keys:
                        - 'tbl_d_dmex_metric': DMeX Metric dimension table
        """
        self.df_dict = df_dict

        # Validate required dimension tables
        required_keys = ['tbl_d_dmex_metric']
        missing_keys = [k for k in required_keys if k not in df_dict]
        if missing_keys:
            raise ValueError(f"Missing required DataFrames: {missing_keys}")

        self.company_dim_df = df_dict['tbl_d_dmex_metric']

    @staticmethod
    def _perform_lookup(
        df: pd.DataFrame,
        dim_df: pd.DataFrame,
        df_key: str,
        dim_key: str,
        new_col: str
    ) -> pd.DataFrame:

        if df_key not in df.columns:
            raise KeyError(f"'{df_key}' not found in input DataFrame")

        if dim_key not in dim_df.columns:
            raise KeyError(f"'{dim_key}' not found in dimension DataFrame")

        # Filter current rows
        if 'Current_Ind' in dim_df.columns:
            dim_df = dim_df[dim_df['Current_Ind'].isin([True, 'Y', 'y', 1])].copy()

        # Determine ID column
        id_col_candidates = [c for c in dim_df.columns if c not in [dim_key, 'Current_Ind']]
        id_col = id_col_candidates[0]

        # Temporary names to avoid collisions
        temp_dim_key = f"__dim_{dim_key}"
        temp_id_col = f"__dim_{id_col}"

        dim_temp = dim_df.rename(columns={
            dim_key: temp_dim_key,
            id_col: temp_id_col
        })

        # Merge
        merged = df.merge(
            dim_temp[[temp_dim_key, temp_id_col]],
            left_on=df_key,
            right_on=temp_dim_key,
            how='left'
        )

        # Rename ID back to output column name
        merged.rename(columns={temp_id_col: new_col}, inplace=True)

        # Drop temp join key
        merged.drop(columns=[temp_dim_key], inplace=True)

        return merged

    def run_etl(self, df_key: str) -> pd.DataFrame:
        """
        Run ETL for the specified input DataFrame key.

        :param df_key: Key of the input DataFrame (e.g., 'region_input')
        :return: DataFrame enriched with DMeX_Metric_Id
        """
        if df_key not in self.df_dict:
            raise KeyError(f"Input DataFrame '{df_key}' not found in df_dict")

        region_df = self.df_dict[df_key].copy()

        # Perform company lookup
        # Here, Organisation_Cd in the input matches Organisation_Cd in the dimension
        region_df = self._perform_lookup(
            region_df, self.company_dim_df,
            df_key='DMeX_Metric_Cd', dim_key='DMeX_Metric_Cd', new_col='DMeX_Metric_Id'
        )

        # DMeX_Metric_Cd

        return region_df.reset_index(drop=True)


warehouse_name = "OCEAN_Data_Collection"
target_schema_name = "Dimensions"

metadata_map = {
    "tbl_d_assurance" : {
        "insert_update" : {
            "join_keys": ["Assurance_Cd"],
            "join_numeric_key": "Assurance_Id",
            "surrogate_key": "Assurance_Instance_Id",
            "data_columns":  {
                "Assurance_Cd": {"type": "TEXT", "unique": False, "nullable": False},
                "Assurance_Level": {"type": "TEXT"},
                "Assurance_Definition": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {'Assurance_Cd', 'Assurance_Level', 'Assurance_Definition'},
        "renames": {},
        "filepath": "assurance_dim.csv",
    },
    "tbl_d_business_type": {
        "insert_update": {
            "join_keys": ["Business_Type_Cd"],
            "join_numeric_key": "Business_Type_Id",
            "surrogate_key": "Business_Type_Instance_Id",
            "data_columns":  {
                "Business_Type_Cd": {"type": "TEXT", "unique": False, "nullable": False},
                "Business_Type": {"type": "TEXT"},
                "Business_Type_Desc": {"type": "TEXT"},
                "Business_Type_Adjustments_Ind": {"type": "BOOLEAN", "nullable": False},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {'Business_Type_Cd', 'Business_Type', 'Business_Type_Desc', 'Business_Type_Adjustments_Flg',},
        "renames": {
            "Business_Type_Adjustments_Flg": "Business_Type_Adjustments_Ind"
        },
        "filepath": "business_type_dim.csv",
    },
    "tbl_d_business_unit" : {
        "insert_update": {
            "join_keys": ["Business_Unit_Cd"],
            "join_numeric_key": "Business_Unit_Id",
            "surrogate_key": "Business_Unit_Instance_Id",
            "data_columns":  {
                "Business_Unit_Cd": {"type": "TEXT", "unique": False, "nullable": False},
                "Business_Unit": {"type": "TEXT"},
                "Business_Unit_Desc": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {'Business_Unit_Cd', 'Business_Unit', 'Business_Unit_Desc',},
        "renames": {},
        "filepath": "business_unit_dim.csv",
    },
        "tbl_d_company" : {
            "insert_update": {
                "join_keys": ["Organisation_Cd"],
                "join_numeric_key": "Company_Id",
                "surrogate_key": "Company_Instance_Id",
                "data_columns":  {
                    "Organisation_Cd": {"type": "TEXT", "unique": False, "nullable": False},
                    "Company_Type_Id": {"type": "INTEGER", "nullable": False},
                    "Service_Type_Id": {"type": "INTEGER", "nullable": False},
                    "Legacy_Company_Name": {"type": "TEXT"},
                    "Communication_Name": {"type": "TEXT"},
                    "Legal_Name": {"type": "TEXT"},
                    "Licence_Number": {"type": "TEXT"},
                    "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                    "Effective_Start_Date": {"type": "DATETIME"},
                    "Effective_End_Date": {"type": "DATETIME"},
                },
                "processing_class": CompanyDimensionTransform,
                "dependency_tables": [
                    "tbl_d_company_type",
                    "tbl_d_company_service",
                ],
            },
            "expected_columns": {'Organisation_Cd', 'Legacy_Company_Name', 'Communication_Name', 'Legal_Name', 'Licence_Number', 'Service_Type_Cd', 'Company_Type_Cd'},
            "renames": {},
            "filepath": "company_dim.csv",
        },
    "tbl_d_company_service" : {
        "insert_update": {
            "join_keys": ["Company_Service_Cd"],
            "join_numeric_key": "Company_Service_Id",
            "surrogate_key": "Company_Service_Instance_Id",
            "data_columns":  {
                "Company_Service_Cd": {"type": "TEXT", "unique": False, "nullable": False},
                "Company_Service_Desc": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {'Company_Service_Cd', 'Company_Service_Desc',},
        "renames": {},
        "filepath": "company_service_dim.csv",
    },
    "tbl_d_company_type" : {
        "insert_update": {
            "join_keys": ["Company_Type_Cd"],
            "join_numeric_key": "Company_Type_Id",
            "surrogate_key": "Company_Type_Instance_Id",
            "data_columns":  {
                "Company_Type_Cd": {"type": "TEXT", "unique": False, "nullable": False},
                "Company_Type_Desc": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {'Company_Type_Cd', 'Company_Type_Desc',},
        "renames": {},
        "filepath": "company_type_dim.csv",
    },
    "tbl_d_credit_protection" : {
        "insert_update": {
            "join_keys": ["Credit_Protection_Cd"],
            "join_numeric_key": "Credit_Protection_Id",
            "surrogate_key": "Credit_Protection_Instance_Id",
            "data_columns":  {},
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {},
        "renames": {},
        "filepath": "credit_protection_dim.csv", # missing csv?
    },
    "tbl_d_currency_pair" : {
        "insert_update": {
            "join_keys": ["Currency_Pair_Cd"],
            "join_numeric_key": "Currency_Pair_Id",
            "surrogate_key": "Currency_Pair_Instance_Id",
            "data_columns":  {},
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {},
        "renames": {},
        "filepath": "currency_pair_dim.csv", # missing csv?
    },
    "tbl_d_customer_type" : {
        "insert_update": {
            "join_keys": ["Customer_Type_Cd"],
            "join_numeric_key": "Customer_Type_Id",
            "surrogate_key": "Customer_Type_Instance_Id",
            "data_columns":  {},
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {},
        "renames": {},
        "filepath": "customer_type_dim.csv", # missing csv?
    },
    "tbl_d_data_source" : {
        "insert_update": {
            "join_keys": ["Data_Source_Cd"],
            "join_numeric_key": "Data_Source_Id",
            "surrogate_key": "Data_Source_Instance_Id",
            "data_columns":  {
                "Data_Source_Cd": {"type": "TEXT", "unique": False, "nullable": False},
                "Data_Source_Desc": {"type": "TEXT"},
                "Data_Source_Definition": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {'Data_Source_Cd', 'Data_Source_Desc', 'Data_Source_Definition'},
        "renames": {},
        "filepath": "data_source_dim.csv",
    },
    "tbl_d_dmex_metric": {
        "insert_update": {
            "join_keys": ["DMeX_Metric_Cd"],
            "join_numeric_key": "DMeX_Metric_Id",
            "surrogate_key": "DMeX_Metric_Instance_Id",
            "data_columns":  {
                "DMeX_Metric_Cd": {"type": "TEXT", "unique": False, "nullable": False},
                "DMeX_Metric_Desc": {"type": "TEXT"},
                "Target": {"type": "TEXT"},
                "Service_Type": {"type": "TEXT"},
                "Applies_to": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {'DMeX_Metric_Cd', 'DMeX_Metric_Desc', 'Target', 'Service_Type', 'Applies_to'},
        "renames": {},
        "filepath": "dmex_metric_dim.csv",
    },
    "tbl_d_dmex_metric_mapping": {
        "insert_update": {
            "join_keys": ["Measure_Cd"],
            "join_numeric_key": "Measure_Id",
            "surrogate_key": "Measure_Instance_Id",
            "data_columns":  {
                "Measure_Cd": {"type": "TEXT", "unique": False, "nullable": False},
                "DMeX_Metric_Cd": {"type": "TEXT"},
                "DMeX_Measure_Type_Cd": {"type": "TEXT"},
                "DMeX_Metric_Id": {"type": "INTEGER"},
                "DMeX_Measure_Type": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": DMeXMetricMappingDimensionTransform,
            "dependency_tables": ["tbl_d_dmex_metric"],
        },
        "expected_columns": {'Measure_Cd', 'DMeX_Metric_Cd', 'DMeX_Measure_Type', 'DMeX_Metric_Type_Cd'},
        "renames": {"DMeX_Metric_Type_Cd": "DMeX_Measure_Type_Cd"},
        "filepath": "dmex_metric_mapping_dim.csv",
    },
    "tbl_d_grants_contributions": {
        "insert_update": {
            "join_keys": ["Grants_Contributions_Treatment_Cd"],
            "join_numeric_key": "Grants_Contributions_Treatment_Id",
            "surrogate_key": "Grants_Contributions_Treatment_Instance_Id",
            "data_columns":  {},
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {},
        "renames": {},
        "filepath": "grants_contributions_dim.csv", # missing csv?
    },
    "tbl_d_interval": {
        "insert_update": {
            "join_keys": ["Interval_Cd"],
            "join_numeric_key": "Interval_Id",
            "surrogate_key": "Interval_Instance_Id",
            "data_columns":  {
                "Interval_Cd": {"type": "TEXT", "unique": False, "nullable": False},
                "Interval_Start_Date": {"type": "DATETIME", "nullable": True},
                "Interval_End_Date": {"type": "DATETIME", "nullable": True},
                "Interval_Type": {"type": "TEXT"},
                "Interval_Duration": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": IntervalDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {'Interval_Cd', 'Start_Date', 'End_Date', 'Interval_Type', 'Interval_Duration'},
        "renames": {"Start_Date": "Interval_Start_Date", "End_Date": "Interval_End_Date"},
        "filepath": "interval_dim.csv",
    },
    "tbl_d_measure": {
        "insert_update": {
            "join_keys": ["Measure_Cd"],
            "join_numeric_key": "Measure_Id",
            "surrogate_key": "Measure_Instance_Id",
            "data_columns":  {
                "Measure_Cd": {"type": "TEXT", "unique": False, "nullable": False},
                "Measure_Name": {"type": "TEXT"},
                "Measure_Description": {"type": "TEXT"},
                "Measure_Definition": {"type": "TEXT"},
                "Unit": {"type": "TEXT"},
                "Decimal_Point": {"type": "INTEGER"},
                "Measure_Area": {"type": "TEXT"},
                "Guidance_Document": {"type": "TEXT"},
                "Guidance_Reference": {"type": "TEXT"},
                "Collection_Table": {"type": "TEXT"},
                "Collection_SubTable": {"type": "TEXT"},
                "Collection_Section": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns":{
            'Measure_Cd',
            'Measure_Name',
            'Measure_Description',
            'Measure_Definition',
            'Unit',
            'Decimal_Point',
            'Measure_Area',
            'Guidance_Document',
            'Guidance_Reference',
            'Collection_Table',
            'Collection_SubTable',
            'Collection_Section'},
        "renames": {},
        "filepath": "measure_dim.csv",
    },
    "tbl_d_mtm_analysis_assumption": {
        "insert_update": {
            "join_keys": ["MTM_Analysis_Assumption_Cd"],
            "join_numeric_key": "MTM_Analysis_Assumption_Id",
            "surrogate_key": "MTM_Analysis_Assumption_Instance_Id",
            "data_columns":  {},
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {},
        "renames": {},
        "filepath": "mtm_analysis_assumption_dim.csv", # missing csv?
    },
    "tbl_d_observation": {
        "insert_update": {
            "join_keys": ["Observation_Cd"],
            "join_numeric_key": "Observation_Id",
            "surrogate_key": "Observation_Instance_Id",
            "data_columns":  {
                "Observation_Cd": {"type": "TEXT", "unique": False, "nullable": False},
                "Observation_Desc": {"type": "TEXT"},
                "Observation_Definition": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {'Observation_Cd', 'Observation_Desc', 'Observation_Definition'},
        "renames": {},
        "filepath": "observation_dim.csv",
    },
    "tbl_d_observation_coverage" : {
        "insert_update": {
            "join_keys": ["Observation_Coverage_Cd"],
            "join_numeric_key": "Observation_Coverage_Id",
            "surrogate_key": "Observation_Coverage_Instance_Id",
            "data_columns":  {
                "Observation_Coverage_Cd": {"type": "TEXT", "unique": False, "nullable": False},
                "Observation_Coverage_Desc": {"type": "TEXT"},
                "Observation_Coverage_Definition": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {'Observation_Coverage_Cd', 'Observation_Coverage_Desc', 'Observation_Coverage_Definition'},
        "renames": {},
        "filepath": "observation_coverage_dim.csv",
    },
    "tbl_d_rate_type" : {
        "insert_update": {
            "join_keys": ["Rate_Type_Cd"],
            "join_numeric_key": "Rate_Type_Id",
            "surrogate_key": "Rate_Type_Instance_Id",
            "data_columns":  {
                "Rate_Type_Cd": {"type": "TEXT", "unique": False, "nullable": False},
                "Rate_Type_Desc": {"type": "TEXT"},
                "Index_Linked_Ind": {"type": "BOOLEAN"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {'Rate_Type_Cd', 'Rate_Type_Desc', 'Index_Linked_Flag'},
        "renames": {"Index_Linked_Flag": "Index_Linked_Ind"},
        "filepath": "rate_type_dim.csv",
    },
    "tbl_d_region" : {
        "insert_update": {
            "join_keys": ["Region_Cd"],
            "join_numeric_key": "Region_Id",
            "surrogate_key": "Region_Instance_Id",
            "data_columns":  {
                "Region_Cd": {"type": "TEXT", "unique": False, "nullable": False},
                "Company_Id": {"type": "INTEGER", "nullable": False},
                "Organisation_Cd": {"type": "TEXT", "nullable": False},
                "Region_Name": {"type": "TEXT", "nullable": False},
                "Region_Name_old": {"type": "TEXT"},
                "Country": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": RegionDimensionTransform,
            "dependency_tables": ["tbl_d_company"],
        },
        "expected_columns": {'Organisation_Cd', 'Region_Cd', 'Region_Name_old', 'Region_Name', 'Country'},
        "renames": {},
        "filepath": "region_dim.csv",
    },
    "tbl_d_returns_equity" : {
        "insert_update": {
            "join_keys": ["Returns_Equity_Cd"],
            "join_numeric_key": "Returns_Equity_Id",
            "surrogate_key": "Returns_Equity_Instance_Id",
            "data_columns":  {
                "Returns_Equity_Cd": {"type": "TEXT", "unique": False, "nullable": False},
                "Returns_Equity": {"type": "TEXT"},
                "Returns_Equity_Desc": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {'Returns_Equity_Cd', 'Returns_Equity', 'Returns_Equity_Desc'},
        "renames": {},
        "filepath": "return_equity_dim.csv", # return / return(s) ?
    },
    "tbl_d_sensitivity" : {
        "insert_update": {
            "join_keys": ["Sensitivity_Cd"],
            "join_numeric_key": "Sensitivity_Id",
            "surrogate_key": "Sensitivity_Instance_Id",
            "data_columns":  {
                "Sensitivity_Cd": {"type": "TEXT", "unique": False, "nullable": False},
                "Sensitivity_Classification": {"type": "TEXT"},
                "Security_Mark": {"type": "TEXT"},
                "Sensitivity_Definition": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {'Sensitivity_Cd', 'Sensitivity_Classification', 'Security_Mark', 'Sensitivity_Definition'},
        "renames": {},
        "filepath": "sensitivity_dim.csv",
    },
    "tbl_map_assurance": {
        "insert_update": {
            "join_keys": ["Collection_Process"],
            "join_numeric_key": "Collection_Process_Id",
            "surrogate_key": "Collection_Process_Instance_Id",
            "data_columns":  {
                "Collection_Process": {"type": "TEXT", "unique": False, "nullable": False},
                "Legacy_BonCode": {"type": "TEXT"},
                "Assurance_Level": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {'Collection_Process', 'Legacy_BonCode', 'Assurance_Level'},
        "renames": {},
        "filepath": "map_assurance_dim.csv",
    },
    "tbl_map_auxiliary": {
        "insert_update": {
            "join_keys": ["Boncode"],
            "join_numeric_key": "Boncode_Id",
            "surrogate_key": "Boncode_Instance_Id",
            "data_columns":  {
                "Boncode": {"type": "TEXT", "unique": False, "nullable": False},
                "Business_Type_Cd": {"type": "TEXT"},
                "Rate_Type_Cd": {"type": "TEXT"},
                "Returns_Equity_Cd": {"type": "TEXT"},
                "Business_Unit_Cd": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {'Boncode', 'Business_Type_Cd', 'Rate_Type_Cd', 'Returns_Equity_Cd', 'Business_Unit_Cd'},
        "renames": {},
        "filepath": "map_auxiliary_dim.csv",
    },
    "tbl_map_measure": {
        "insert_update": {
            "join_keys": ["Legacy_BonCode"],
            "join_numeric_key": "Legacy_BonCode_Id",
            "surrogate_key": "Legacy_BonCode_Instance_Id",
            "data_columns":  {
                "Ocean_Measure_Code": {"type": "TEXT", "unique": False, "nullable": False},
                "Legacy_BonCode": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {'Ocean_Measure_Code', 'Legacy_BonCode'},
        "renames": {},
        "filepath": "map_measure_dim.csv",
    },
    "tbl_map_observation": {
        "insert_update": {
            "join_keys": ["Collection_Process"],
            "join_numeric_key": "Collection_Process_Id",
            "surrogate_key": "Collection_Process_Instance_Id",
            "data_columns":  {
                "Collection_Process": {"type": "TEXT", "unique": False, "nullable": False},
                "Legacy_BonCode": {"type": "TEXT"},
                "Observation": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {'Collection_Process', 'Legacy_BonCode', 'Observation'},
        "renames": {},
        "filepath": "map_observation_dim.csv",
    },
    "tbl_map_sensitivity": {
        "insert_update": {
            "join_keys": ["Collection_Process"],
            "join_numeric_key": "Collection_Process_Id",
            "surrogate_key": "Collection_Process_Instance_Id",
            "data_columns":  {
                "Collection_Process": {"type": "TEXT", "unique": False, "nullable": False},
                "Legacy_BonCode": {"type": "TEXT"},
                "Security_Mark": {"type": "TEXT"},
                "Batch_Id": {"type": "TEXT", "nullable": False, "unique": False,},
                "Effective_Start_Date": {"type": "DATETIME"},
                "Effective_End_Date": {"type": "DATETIME"},
            },
            "processing_class": CommonDimensionTransform,
            "dependency_tables": [],
        },
        "expected_columns": {'Collection_Process', 'Legacy_BonCode', 'Security_Mark'},
        "renames": {},
        "filepath": "map_sensitivity_dim.csv",
    },
}

surrogate_key = metadata_map[target_table_name]["insert_update"]["surrogate_key"]
join_keys = metadata_map[target_table_name]["insert_update"]["join_keys"]
processing_class = metadata_map[target_table_name]["insert_update"]["processing_class"]
dependency_tables = metadata_map[target_table_name]["insert_update"]["dependency_tables"]
data_columns = metadata_map[target_table_name]["insert_update"]["data_columns"]
join_numeric_key = metadata_map[target_table_name]["insert_update"]["join_numeric_key"]


surrogate_key, join_keys, processing_class, dependency_tables, data_columns, join_numeric_key


DIM_FILE_PATH = f"{SOURCE_DATA_PATH}/Files/{metadata_map[target_table_name]['filepath']}"

dim_update_df = pd.read_csv(
    DIM_FILE_PATH,
    header=0,
    encoding='utf-8',
    dtype=str,
    keep_default_na=False,  # Don't treat "NA", "NaN", etc. as NaN
    na_values=["NULL"],     #Make NULL string a NaN
)

# dim_update_df = dim_update_df[~dim_update_df['Measure_Cd'].str.startswith('MFR_TEST_')]

df_preprocessed = dataprepkit.processors.dimensions.dim_common.process_dim_dataframe(
    dim_update_df,
    metadata_map[target_table_name]["expected_columns"],
    metadata_map[target_table_name]["renames"],
    batch_id
)

# Convert all values in the DataFrame to string, including NaNs → ''
df_preprocessed = df_preprocessed.fillna('').astype(str)

required_dim_tables = load_dim_tables(engine, dependency_tables)
required_dim_tables["incoming_values"] = df_preprocessed

etl_processor = processing_class(
    required_dim_tables
)
df_to_write = etl_processor.run_etl("incoming_values")


df_to_write = df_to_write.drop(['Insert_Date', 'Update_Date'], axis=1)
df_to_write[["Effective_Start_Date", "Effective_End_Date"]] = None

if __name__ == "__main__":
    conn = sqlite3.connect('scd_type2_example.db')


    conn.execute(f"drop table if exists {target_table_name}")
    

    initialize_db(
        conn,
        target_table_name,
        data_columns,
        key_column=join_keys[0],
        numeric_key_column = join_numeric_key,
        surrogate_key_column=surrogate_key,
        effective_date_column="Insert_Date",
        end_date_column="Update_Date",
        latest_ind_column ="Current_Ind",
        row_hash_column="Row_Hash",)

    from sqlalchemy import inspect

    inspector = inspect(engine)
    if target_table_name in inspector.get_table_names(schema=target_schema_name):
        df_current = pd.read_sql(f"SELECT * FROM {target_schema_name}.{target_table_name}", engine)

        df_current.to_sql(
            name=target_table_name,      # Table name
            con=conn,           # SQLite connection
            if_exists='append', # Options: 'fail', 'replace', 'append'
            index=False         # Don't write DataFrame index as a column
        )
    else:
        print("Table does not exist")
        df_current = pd.DataFrame()  # empty fallback

    insert_scd2(
        conn,
        target_table_name,
        df_to_write,
        key_column=join_keys[0],
        numeric_key_column = join_numeric_key,
        effective_date_column="Insert_Date",
        end_date_column="Update_Date",
        latest_ind_column ="Current_Ind",
        row_hash_column="Row_Hash",
        exclude_hash_columns=["Batch_Id"],
        mode='full',
    )



if __name__ == "__main__":
    sqlite_conn = conn
    sqlite_table_name = target_table_name
    target_engine = engine

    df_new = pd.read_sql(f"SELECT * FROM {target_table_name}", sqlite_conn)

    allow_drift = {
        'missing_columns': False,
        'extra_columns': False,
        'nullability': False,
        'type_mismatch': False
    }

    # df_new = spark.createDataFrame(df_new)

    replace_table_using_sqlite_schema(
        target_engine,
        df_new,
        target_table=f"{target_table_name}",
        target_schema=target_schema_name,
        sqlite_schema_conn=sqlite_conn,
        sqlite_table=sqlite_table_name,
        enforce_row_growth=True,
        allow_drift_checks=allow_drift,
        drop_backup=True,
        workspace_ID=WS__UUID__TARGET,
        warehouse = warehouse_name,
    )


df_current_after = pd.read_sql(f"select * from {target_schema_name}.{target_table_name}",engine)
df_current_after


if not df_current_after.equals(df_current):
    # Archive to structured folder
    archive_dataframe(
        dim_update_df,
        table_name=target_table_name,
        batch_id=batch_id,
        base_dir=f"{ARCHIVE_DATA_PATH}/Files/Archive/"
    )