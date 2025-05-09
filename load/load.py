from pyspark.sql import SparkSession
import psycopg2

    
def load(rate1_path,provider1_path):
    spark=SparkSession.builder.appName('provider').getOrCreate()

    df = spark.read.parquet(rate1_path)
    df1=spark.read.parquet(provider1_path)

    conn = psycopg2.connect(
        dbname="my_pgdb",
        user="postgres",
        password="admin",
        host="localhost",
        port=5432
    )

    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    connection_properties = {
        "user": "postgres",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }

    cur = conn.cursor()

    create_table_query = """
    DROP TABLE IF EXISTS provider_data1;
    CREATE TABLE IF NOT EXISTS provider_data1 (
        provider_group_id INT,
        npi BIGINT,
        tin_type SMALLINT,
        tin TEXT
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    df.write.jdbc(url=jdbc_url,table="provider_data1",mode="append", properties=connection_properties)



    create_table_query = """
    DROP TABLE IF EXISTS innetwork_data1;
    CREATE TABLE IF NOT EXISTS innetwork_data1 (
        billing_code TEXT,
        billing_code_type TEXT,
        negotiation_arrangement TEXT,
        provider_group_id INT,
        billing_class TEXT,
        billing_code_modifier TEXT[],
        negotiated_rate DOUBLE PRECISION,
        negotiated_type TEXT,
        service_code INTEGER[]
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    df1.write.jdbc(url=jdbc_url,table="innetwork_data1",mode="append", properties=connection_properties)

    cur.close()
    conn.close()

    