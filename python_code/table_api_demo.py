from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings, TableDescriptor, Schema


def table_api_demo():
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    table_env = StreamTableEnvironment.create(environment_settings=env_settings)

    table_env.execute_sql("""
        CREATE TABLE source_table (
            id STRING,
            name STRING,
            description STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'source_table',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """)

    table_env.execute_sql("""
        CREATE TABLE sink_table (
            id STRING,
            name STRING,
            description STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sink_table',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """)

    table_env.execute_sql("""
        INSERT INTO sink_table
        SELECT id, name, description
        FROM source_table
    """)

    table_env.execute("table_api_demo")