from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings, TableDescriptor, Schema
from pyflink.datastream import StreamExecutionEnvironment

def table_api_demo():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    env.enable_checkpointing(60000) 
    env.get_checkpoint_config().set_min_pause_between_checkpoints(500)
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, environment_settings=env_settings)
    
    table_env.get_config().set("execution.checkpointing.interval", "3000")

    table_env.execute_sql("""
        CREATE TABLE source_table (
            id STRING,
            name STRING,
            description STRING
        ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '5',
            'fields.id.kind' = 'sequence',
            'fields.id.start' = '1',
            'fields.id.end' = '1000',
            'fields.name.length' = '10',
            'fields.description.length' = '20'
        )
    """)

    table_env.execute_sql("""
        CREATE TABLE sink_table (
            id STRING,
            name STRING,
            description STRING
        ) WITH (
            'connector' = 'print'
        )
    """)

    statement_set = table_env.create_statement_set()
    statement_set.add_insert_sql("""
        INSERT INTO sink_table
        SELECT id, name, description
        FROM source_table
    """)
    
    statement_set.execute()


if __name__ == '__main__':
    table_api_demo()