from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.descriptors import Schema, Kafka, Json

# Configuração do ambiente de batch
env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
t_env = TableEnvironment.create(environment_settings=env_settings)

# Definindo a fonte Kafka
t_env.connect(
    Kafka()
        .version("universal")
        .topic("FaresCalculated")
        .start_from_earliest()
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
        .field("rowtime", DataTypes.TIMESTAMP(3)).proctime()
).create_temporary_table("FaresCalculatedSource")

# Definindo o sink do filesystem
t_env.execute_sql("""
    CREATE TABLE TaxAggregated (
        rideId BIGINT,
        tax FLOAT
    ) WITH (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '/opt/examples/table/output/tax_aggregated'
    )
""")

# Processamento dos dados
t_env.execute_sql("""
    INSERT INTO TaxAggregated
    SELECT
        rideId,
        SUM(tax) as tax
    FROM FaresCalculatedSource
    GROUP BY rideId
""")

# Executa a pipeline
t_env.execute("Fares Batch Pipeline")
