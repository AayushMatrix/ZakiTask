from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col,array,regexp_replace,when,concat,lit,concat_ws
from pyspark.sql.types import ArrayType,IntegerType,ShortType
import yaml 


def trasform_loc(provider_path,innetwork_path,etl,provider):

    spark=etl.spark
    df1 = spark.read.json(innetwork_path)
    df = spark.read.json(provider_path)
    df2 = spark.read.json(provider)


    exploded_rates_df = df1.withColumn("negotiated_rate", explode("negotiated_rates"))
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


    provider_df = df.withColumn("row", explode("provider_groups"))
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
 
    
    # new provider 
    df2 = spark.read.json("/home/aayush-gyawali/Downloads/provider_detail.json")

    column_drop = df2.drop('prv_fax','provider_name_prefix_text','prv_type_desc')
    mapped1= column_drop.withColumn('prv_type_code',when(col('prv_type_code')=="P",1).when(col('prv_type_code')=="F",2))
    mapped = mapped1.withColumn('prv_type_code',col('prv_type_code').cast(IntegerType()))
    merge = mapped.withColumn("full_name", concat_ws(" ","provider_first_name", "provider_middle_name","provider_last_name")).drop("provider_first_name","provider_last_name","provider_middle_name")
    
    merge = merge.select("*",col("loc.lat").alias("latitude"),col("loc.lon").alias("longitude")).drop('loc')

    merge = merge.withColumn("taxonomy",array(col("prv_taxonomy_1_code"),col("prv_taxonomy_2_code"),col("prv_taxonomy_3_code"))).drop("prv_taxonomy_1_code","prv_taxonomy_2_code","prv_taxonomy_3_code")

    merge1 = merge.withColumn("prv_specialty",array(col("prv_specialty_1_desc"),col("prv_specialty_2_desc"),col("prv_specialty_2_desc"))).drop("prv_specialty_1_desc","prv_specialty_2_desc","prv_specialty_3_desc")
    df_joined = change2_df.join(merge1, on="npi", how="inner")

    unjoined_df= change2_df.join(merge1,on="npi",how="left_anti")

    # rate1.write.parquet("rate")
    # change2_df.write.parquet("provider")
    
    
    rate1_path = "file/rate_data.parquet"
    provider1_path = "file/provider_data.parquet"
    unjoin_path= "file/unjoin_data.parquet"

    rate1.write.parquet(rate1_path,"overwrite")
    df_joined.write.parquet(provider1_path,"overwrite")
    unjoined_df.write.parquet(unjoin_path,"overwrite")
  

    return(rate1_path,provider1_path)


