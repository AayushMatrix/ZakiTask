from pyspark.sql import SparkSession
import psycopg2

    
def load(rate1_path,provider1_path,etl):
    spark=etl.spark

    pg_host = etl.pg_host
    pg_port = etl.pg_port
    pg_database = etl.pg_database
    pg_user = etl.pg_user
    pg_password = etl.pg_password

    df = spark.read.parquet(rate1_path)
    df1=spark.read.parquet(provider1_path)

    conn = psycopg2.connect(
        dbname=pg_database,
        user=pg_user,
        password=pg_password,
        host=pg_host,
        port=pg_port
    )

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}"
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
        tin VARCHAR(15),
        prv_city VARCHAR(255),
        prv_phone VARCHAR(15),
        prv_state CHAR(2),
        prv_street_1 VARCHAR(255),
        prv_type_code SMALLINT,
        prv_zip VARCHAR(10),
        full_name VARCHAR(255),
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        taxonomy TEXT[],
        prv_specialty TEXT[]
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    df1.write.jdbc(url=jdbc_url,table="provider_data1",mode="append", properties=connection_properties)



    create_table_query = """
    DROP TABLE IF EXISTS innetwork_data1;
    CREATE TABLE IF NOT EXISTS innetwork_data1 (
        billing_code VARCHAR(10),
        billing_code_type VARCHAR(10),
        negotiation_arrangement VARCHAR(5),
        provider_group_id INT,
        billing_class VARCHAR(15),
        billing_code_modifier TEXT[],
        negotiated_rate DOUBLE PRECISION,
        negotiated_type VARCHAR(12),
        service_code INTEGER[]
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    df.write.jdbc(url=jdbc_url,table="innetwork_data1",mode="append", properties=connection_properties)
    

    cur.close()
    conn.close()

    