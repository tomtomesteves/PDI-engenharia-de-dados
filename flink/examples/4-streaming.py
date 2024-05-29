from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.descriptors import Schema, Kafka, Json
from pyflink.table.expressions import col

# Configurações do ambiente
s_env = StreamExecutionEnvironment.get_execution_environment()
s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
s_env.set_parallelism(1)

st_env = StreamTableEnvironment.create(s_env, environment_settings=EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build())

# Registra a fonte Kafka
st_env.connect(
    Kafka()
        .version("universal")
        .topic("Fares")
        .start_from_earliest()
        .property("zookeeper.connect", "zookeeper:2181")
        .property("bootstrap.servers", "kafka:9092")
).with_format(
    Json()
        .fail_on_missing_field(False)
        .schema(DataTypes.ROW([
            DataTypes.FIELD("rideId", DataTypes.BIGINT()),
            DataTypes.FIELD("eventTime", DataTypes.STRING()),
            DataTypes.FIELD("payMethod", DataTypes.STRING()),
            DataTypes.FIELD("fare", DataTypes.FLOAT()),
            DataTypes.FIELD("toll", DataTypes.FLOAT()),
            DataTypes.FIELD("tip", DataTypes.FLOAT())
        ]))
).with_schema(
    Schema()
        .field("rideId", DataTypes.BIGINT())
        .field("eventTime", DataTypes.STRING())
        .field("payMethod", DataTypes.STRING())
        .field("fare", DataTypes.FLOAT())
        .field("toll", DataTypes.FLOAT())
        .field("tip", DataTypes.FLOAT())
).in_append_mode().create_temporary_table("FaresSource")

# Registra o sink Kafka
st_env.connect(
    Kafka()
        .version("universal")
        .topic("FaresCalculated")
        .property("zookeeper.connect", "zookeeper:2181")
        .property("bootstrap.servers", "kafka:9092")
).with_format(
    Json()
        .fail_on_missing_field(False)
        .schema(DataTypes.ROW([
            DataTypes.FIELD("rideId", DataTypes.BIGINT()),
            DataTypes.FIELD("tax", DataTypes.FLOAT()),
            DataTypes.FIELD("eventTime", DataTypes.STRING())
        ]))
).with_schema(
    Schema()
        .field("rideId", DataTypes.BIGINT())
        .field("tax", DataTypes.FLOAT())
        .field("eventTime", DataTypes.STRING())
).in_append_mode().create_temporary_table("FaresCalculatedSink")

# Processa os dados e aplica a fórmula matemática
st_env.from_path("FaresSource") \
    .filter((col("payMethod") == "CSH") | (col("payMethod") == "CRD")) \
    .select(
        col("rideId"),
        ((col("fare") - col("toll")) * (1.1 if col("payMethod") == "CSH" else 1.15)).alias("tax"),
        col("eventTime")
    ) \
    .insert_into("FaresCalculatedSink")

# Executa a pipeline
st_env.execute("Fares Streaming Pipeline")
