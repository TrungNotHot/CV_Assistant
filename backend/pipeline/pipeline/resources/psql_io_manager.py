import polars as pl
import pandas as pd
from sqlalchemy import create_engine, text
from dagster import IOManager, OutputContext, InputContext


class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        self.connection_string = (
            f"postgresql+psycopg2://{config['user']}:{config['password']}"
            + f"@{config['host']}:{config['port']}"
            + f"/{config['database']}"
        )
        self.engine = create_engine(self.connection_string)

    def _ensure_schema_exists(self, schema, context):
        """Create schema if it doesn't exist."""
        try:
            with self.engine.connect() as conn:
                # Check if schema exists
                check_schema_sql = text(
                    f"SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = '{schema}')"
                )
                schema_exists = conn.execute(check_schema_sql).scalar()
                
                if not schema_exists:
                    context.log.info(f"Schema '{schema}' does not exist. Creating it.")
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                    conn.commit()
                    context.log.info(f"Successfully created schema '{schema}'")
                    return True
                
                return True
        except Exception as e:
            context.log.error(f"Failed to create schema '{schema}': {str(e)}")
            raise
            
    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        """Store a Polars DataFrame in a PostgreSQL table."""
        if obj is None or obj.shape[0] == 0:
            context.log.warning("Received empty DataFrame, skipping storage operation")
            return
            
        layer, schema, table = context.asset_key.path
        table_name = table.replace(f'{layer}_', '')
        context.log.info(f"Writing {obj.shape[0]} rows to {schema}.{table_name}")
        
        # Ensure schema exists before attempting to write
        self._ensure_schema_exists(schema, context)
        
        try:
            if context.has_partition_key:
                try:
                    # Convert Polars DataFrame to Pandas temporarily for using to_sql
                    obj.to_pandas().to_sql(
                        name=table_name,
                        con=self.engine,
                        schema=schema,
                        if_exists='append',
                        index=False
                    )
                    context.log.info(f"Successfully appended data to {schema}.{table_name}")
                except Exception as e:
                    context.log.info(f"Table {schema}.{table_name} needs to be recreated: {str(e)}")
                    # Create empty dataframe with same schema
                    empty_df = pl.DataFrame(schema=obj.schema).to_pandas()
                    empty_df.to_sql(
                        name=table_name,
                        con=self.engine,
                        schema=schema,
                        if_exists='replace',
                        index=False
                    )
                    # Then append the data
                    obj.to_pandas().to_sql(
                        name=table_name,
                        con=self.engine,
                        schema=schema,
                        if_exists='append',
                        index=False
                    )
                    context.log.info(f"Successfully recreated and appended data to {schema}.{table_name}")
            else:
                obj.to_pandas().to_sql(
                    name=table_name,
                    con=self.engine,
                    schema=schema,
                    if_exists='replace',
                    index=False
                )
                context.log.info(f"Successfully replaced data in {schema}.{table_name}")
        except Exception as e:
            context.log.error(f"Failed to write data to {schema}.{table_name}: {str(e)}")
            raise

    def load_input(self, context: InputContext) -> pl.DataFrame:
        """Load data from a PostgreSQL table as a Polars DataFrame."""
        layer, schema, table = context.asset_key.path
        table_name = table.replace(f'{layer}_', '')
        
        # Ensure schema exists before attempting to read
        schema_exists = self._ensure_schema_exists(schema, context)
        if not schema_exists:
            context.log.warning(f"Schema '{schema}' could not be created. Returning empty DataFrame.")
            return pl.DataFrame()
            
        try:
            context.log.info(f"Loading data from {schema}.{table_name}")
            
            # Check if the table exists
            with self.engine.connect() as conn:
                check_table_sql = text(
                    "SELECT EXISTS (SELECT FROM information_schema.tables "
                    f"WHERE table_schema = '{schema}' AND table_name = '{table_name}')"
                )
                result = conn.execute(check_table_sql).scalar()
                
                if not result:
                    context.log.warning(f"Table {schema}.{table_name} does not exist")
                    return pl.DataFrame()
                
                # If we have a partition key, filter by partition
                if context.has_partition_key:
                    # Assuming partition key is stored in metadata
                    partition_key = context.asset_partition_key
                    partition_column = context.resource_config.get('partition_column', 'date')
                    
                    query = f"SELECT * FROM {schema}.{table_name} WHERE {partition_column} = '{partition_key}'"
                    context.log.info(f"Loading partition {partition_key} from {schema}.{table_name}")
                else:
                    query = f"SELECT * FROM {schema}.{table_name}"
                
                # Read data into pandas DataFrame first
                pandas_df = pd.read_sql(query, conn)
                
                # Convert to Polars DataFrame
                polars_df = pl.from_pandas(pandas_df)
                context.log.info(f"Loaded {polars_df.shape[0]} rows from {schema}.{table_name}")
                
                return polars_df
                
        except Exception as e:
            context.log.error(f"Failed to load data from {schema}.{table_name}: {str(e)}")
            raise
