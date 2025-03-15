from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_events_aggregated_sink(t_env):
    table_name = 'longest_streaks'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            pu_location_id INTEGER,
            do_location_id INTEGER,
            streak_start TIMESTAMP(3),
            streak_end TIMESTAMP(3),
            streak_count DOUBLE,
            PRIMARY KEY (pu_location_id, do_location_id, streak_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_events_source_kafka(t_env):
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime TIMESTAMP(3),
            lpep_dropoff_datetime TIMESTAMP(3),
            PULocationID INT,
            DOLocationID INT,
            passenger_count INT,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            event_watermark AS lpep_dropoff_datetime,
            pickup_event_time AS lpep_pickup_datetime,
            WATERMARK FOR pickup_event_time AS lpep_pickup_datetime - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-data',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name

def log_aggregation():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    try:
        source_table = create_events_source_kafka(t_env)
        aggregated_table = create_events_aggregated_sink(t_env)

        t_env.execute_sql(f"""
        CREATE TEMPORARY VIEW cleaned_events AS 
        SELECT 
            PULocationID, 
            DOLocationID, 
            lpep_pickup_datetime, 
            lpep_dropoff_datetime, 
            COALESCE(NULLIF(passenger_count, ''), 0) AS passenger_count, 
            trip_distance, 
            tip_amount, 
            event_watermark
        FROM {source_table};
        """)

        t_env.execute_sql(f"""
        INSERT INTO {aggregated_table}
        SELECT
            PULocationID AS pu_location_id,
            DOLocationID AS do_location_id,
            SESSION_START(pickup_event_time, INTERVAL '5' MINUTE) AS streak_start,
            SESSION_END(pickup_event_time, INTERVAL '5' MINUTE) AS streak_end,
            count(*) AS total_distance
        FROM {source_table}
        --TABLE(
        --SESSION(TABLE {source_table}, DESCRIPTOR(event_watermark), INTERVAL --'5' MINUTE))
        GROUP BY
            PULocationID,
            DOLocationID,
           SESSION(pickup_event_time, INTERVAL '5' MINUTE)
            --window_start,
            --window_end;
        """).wait()
    
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))

if __name__ == '__main__':
    log_aggregation()
