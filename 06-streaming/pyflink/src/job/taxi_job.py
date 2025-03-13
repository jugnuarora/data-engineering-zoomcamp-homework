from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.common.time import Duration

def create_kafka_source(t_env):
    """
    Creates a Kafka source table to read trip data.
    """
    table_name = "green_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            PULocationID INT,
            DOLocationID INT,
            passenger_count INT,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            event_time AS TO_TIMESTAMP_LTZ(lpep_dropoff_datetime, 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-data',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def create_postgres_sink(t_env):
    """
    Creates a PostgreSQL sink table to store results.
    """
    table_name = "longest_streaks"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            pickup_location_id INT,
            dropoff_location_id INT,
            trip_start TIMESTAMP(3),
            trip_end TIMESTAMP(3),
            trip_count BIGINT,
            PRIMARY KEY (pickup_location_id, dropoff_location_id, trip_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://localhost:5432/postgres',
            'table-name' = 'longest_streaks',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def process_stream():
    """
    Sets up PyFlink streaming job to compute the longest unbroken streak of taxi trips.
    """
    # Create Execution Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)

    # Create Table Environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Register Kafka Source & PostgreSQL Sink
    source_table = create_kafka_source(t_env)
    sink_table = create_postgres_sink(t_env)

    # SQL Query to compute the longest unbroken streak
    query = f"""
        INSERT INTO {sink_table}
        SELECT 
            PULocationID AS pickup_location_id,
            DOLocationID AS dropoff_location_id,
            window_start AS trip_start,
            window_end AS trip_end,
            COUNT(*) AS trip_count
        FROM TABLE(
            SESSION(TABLE {source_table}, DESCRIPTOR(event_time), INTERVAL '5' MINUTE)
        )
        GROUP BY window_start, window_end, PULocationID, DOLocationID;
    """

    # Execute Query
    t_env.execute_sql(query).wait()


if __name__ == "__main__":
    process_stream()
