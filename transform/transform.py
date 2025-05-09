from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col,array,regexp_replace,when
from pyspark.sql.types import ArrayType,IntegerType,ShortType

def trasform_loc(provider_path,innetwork_path):
    spark=SparkSession.builder.appName('provider').getOrCreate()

    df = spark.read.json(innetwork_path)
    df1 = spark.read.json(provider_path)



    exploded_rates_df = df.withColumn("negotiated_rate", explode("negotiated_rates"))
    rate_df = exploded_rates_df.withColumn("provider_group_id", explode("negotiated_rate.provider_references")) \
                            .withColumn("negotiated_price", explode("negotiated_rate.negotiated_prices")) \
                            .select(
                                col("billing_code"),
                                col("billing_code_type"),
                                col("negotiation_arrangement"),
                                col("negotiated_price.billing_class").alias("billing_class"),
                                col("negotiated_price.negotiated_rate").alias("negotiated_rate"),
                                col("negotiated_price.negotiated_type").alias("negotiated_type"),
                                col("provider_group_id"),
                                col("negotiated_price.billing_code_modifier").alias("billing_code_modifier"),
                                col("negotiated_price.service_code").alias("service_code"),
                                )


    rate = rate_df.withColumn("service_code",col("service_code").cast(ArrayType(IntegerType())))
    rate1 = rate.withColumn("provider_group_id", col("provider_group_id").cast(ShortType()))


    provider_df = df1.withColumn("row", explode("provider_groups"))
    npi_df = provider_df.withColumn("row1",explode("row.npi"))
    provider_flat = npi_df.select(
        col("provider_group_id"),
        col("row1").alias("npi"),
        col("row.tin.type").alias("tin_type"),
        col("row.tin.value").alias("tin")
    )

    hypenrm_df=provider_flat.withColumn("tin",regexp_replace(col("tin"),"-",''))
    change_df = hypenrm_df.withColumn('tin_type',when((col('tin_type')== 'ein'), 1).when((col('tin_type')== 'npi'), 2))
    change_df.show()

    change2_df = change_df.withColumn("tin_type", col("tin_type").cast(ShortType()))

    # rate1.write.parquet("rate")
    # change2_df.write.parquet("provider")
    
    
    rate1_path = "file/rate_data.parquet"
    provider1_path = "file/provider_data.parquet"

    rate1.write.parquet(rate1_path)
    provider1_path.write.parquet(provider1_path)

    return(rate1_path,provider1_path)


