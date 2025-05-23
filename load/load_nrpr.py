from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lpad
import psycopg2
def load_nrpr(rate_path,provider_path,df2,etl):
    spark=etl.spark
    pg_host = etl.pg_host
    pg_port = etl.pg_port
    pg_database = etl.pg_database
    pg_user = etl.pg_user
    pg_password = etl.pg_password
    df = spark.read.parquet(rate_path)
    df1=spark.read.parquet(provider_path)
    # df2 = spark.read.option("header", "true").csv(r"C:\Users\aayus\Desktop\coding\task\ZakiTask\ignore\billing_taxonomy_list.csv")
    # df2 = df2.withColumn("billing_code", lpad(col("billing_code"), 5, "0"))
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
    DROP TABLE IF EXISTS provider;
    CREATE TABLE IF NOT EXISTS provider (
        provider_group_id INT,
        npi BIGINT,
        tin_type SMALLINT,
        tin VARCHAR(15),
        prv_city VARCHAR(255),
        prv_phone VARCHAR(255),
        prv_state CHAR(2),
        prv_street_1 VARCHAR(255),
        prv_type_code SMALLINT,
        prv_zip VARCHAR(10),
        full_name VARCHAR(255),
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        taxonomy TEXT[],
        prv_specialty TEXT[],
        geom GEOGRAPHY GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography)STORED
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    df1.write.jdbc(url=jdbc_url,table="provider",mode="append", properties=connection_properties)
    create_table_query = """
    DROP TABLE IF EXISTS rate;
    CREATE TABLE IF NOT EXISTS rate (
        billing_code VARCHAR(10),
        billing_code_type VARCHAR(10),
        negotiation_arrangement VARCHAR(5),
        provider_group_id INT,
        billing_class VARCHAR(15),
        billing_code_modifier TEXT[],
        negotiated_rate DOUBLE PRECISION,
        negotiated_type VARCHAR(12),
        service_code INTEGER[],
        taxonomy_list TEXT[]
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    df.write.jdbc(url=jdbc_url,table="rate",mode="append", properties=connection_properties)

    cur.execute("CREATE SCHEMA IF NOT EXISTS taxonomy;")
    conn.commit()
    create_table_query = """
    DROP TABLE IF EXISTS taxonomy.billing_taxonomy;
    CREATE TABLE IF NOT EXISTS taxonomy.billing_taxonomy (
        billing_code VARCHAR(5),
        billing_code_type VARCHAR(10),
        billing_description TEXT,
        taxonomy_list TEXT[]
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    df2.write.jdbc(url=jdbc_url,table="taxonomy.billing_taxonomy",mode="append", properties=connection_properties)
    cur.close()
    conn.close()
