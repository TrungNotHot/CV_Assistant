from dagster import IOManager, InputContext, OutputContext
import polars as pl
import psycopg2
from psycopg2.extras import execute_values


def connect_psql(config, table):
    conn = {
        "url": f"jdbc:postgresql://{config['host']}:{config['port']}/{config['database']}",
        "dbtable": table,
        "user": config["user"],
        "password": config["password"],
        "host": config["host"],
        "port": config["port"],
        "database": config["database"],
    }
    return conn


class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        # Use asset metadata if available, otherwise fallback to default path-based approach
        if context.metadata and "schema" in context.metadata and "table" in context.metadata:
            schema_ = context.metadata["schema"]
            table = context.metadata["table"]
        else:
            # Fallback to default behavior
            table = context.asset_key.path[-1]
            schema_ = context.asset_key.path[-2]
            
        full_table = f"{schema_}.{table}"
        conn_info = connect_psql(self._config, full_table)
        
        context.log.info(f"Writing Polars DataFrame to PostgreSQL table {full_table}")

        # Convert Polars DataFrame to pandas for compatibility with psycopg2
        pandas_df = obj.to_pandas()
        
        # Get column names and data types
        columns = pandas_df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(columns))
        column_str = ', '.join([f'"{col}"' for col in columns])
        
        # Create connection and cursor
        conn = psycopg2.connect(
            host=conn_info['host'],
            port=conn_info['port'],
            database=conn_info['database'],
            user=conn_info['user'],
            password=conn_info['password']
        )
        
        try:
            with conn.cursor() as cur:
                # Create schema if it doesn't exist
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_}")
                
                # Drop table if exists (for overwrite mode)
                cur.execute(f"DROP TABLE IF EXISTS {full_table}")
                
                # Extract metadata for table creation
                primary_keys = context.metadata.get("primary_keys", []) if context.metadata else []
                specific_columns = context.metadata.get("columns", []) if context.metadata else []
                
                # Create table based on DataFrame schema and metadata
                create_table_query = self._create_table_sql(full_table, pandas_df, primary_keys, specific_columns)
                cur.execute(create_table_query)
                
                # Insert data using execute_values for better performance
                data_tuples = [tuple(row) for row in pandas_df.values]
                insert_query = f'INSERT INTO {full_table} ({column_str}) VALUES %s'
                execute_values(cur, insert_query, data_tuples)
                
                conn.commit()
                context.log.info(f"Successfully wrote {len(obj)} rows to {full_table}")
                
                # Log metadata information for reference
                if primary_keys:
                    context.log.info(f"Table created with primary key(s): {', '.join(primary_keys)}")
                    
        except Exception as e:
            conn.rollback()
            context.log.error(f"Error writing to PostgreSQL: {str(e)}")
            raise
        finally:
            conn.close()
    
    def _create_table_sql(self, table_name, df, primary_key_cols=None, specific_columns=None):
        """Generate SQL to create table based on pandas DataFrame schema and asset metadata"""
        columns = []
        
        # If specific columns are defined in metadata, ensure they are in the correct order
        column_order = specific_columns if specific_columns else df.columns
        
        for col_name in column_order:
            if col_name in df.columns:  # Make sure the column exists in the DataFrame
                pg_type = self._map_pandas_type_to_postgres(df.dtypes[col_name])
                columns.append(f'"{col_name}" {pg_type}')
            else:
                # Log warning but continue - this should not happen if metadata is consistent with data
                # Could add logging here in the future
                pass
                
        # Add primary key constraint if primary keys are defined
        if primary_key_cols and len(primary_key_cols) > 0:
            primary_key_clause = f', PRIMARY KEY ({", ".join([f"\"{pk}\"" for pk in primary_key_cols])})'
            return f"CREATE TABLE {table_name} ({', '.join(columns)}{primary_key_clause})"
        
        return f"CREATE TABLE {table_name} ({', '.join(columns)})"
    
    def _map_pandas_type_to_postgres(self, dtype):
        """Map pandas dtype to PostgreSQL type"""
        dtype_str = str(dtype)
        
        if 'int' in dtype_str:
            return 'INTEGER'
        elif 'float' in dtype_str:
            return 'DOUBLE PRECISION'
        elif 'bool' in dtype_str:
            return 'BOOLEAN'
        elif 'datetime' in dtype_str:
            return 'TIMESTAMP'
        else:
            return 'TEXT'

    def load_input(self, context: InputContext) -> pl.DataFrame:
        pass