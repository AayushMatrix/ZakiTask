from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col,array,regexp_replace,when,concat,lpad,lit,concat_ws,array_except,hash
from pyspark.sql.types import ArrayType,IntegerType,ShortType
import yaml
def trasform_nrpr(new_path,etl,prov):
    spark=etl.spark
    df = spark.read.option("multiline","True").json(new_path)
    df1 = spark.read.parquet(prov)
    df2 = spark.read.option("header", "true").csv("/home/aayush-gyawali/Desktop/task/ZakiTask/ignore/billing_taxonomy_list.csv")
    df2 = df2.filter(col("billing_code").isNotNull() & (col("billing_code") != "")).drop('_c4', '_c5', '_c6').withColumn("billing_code", lpad(col("billing_code"), 5, "0")).withColumn("taxonomy_list",array(regexp_replace(col("taxonomy_list"), r"^\{|\}$", "")))
    
    nrpr = (df.selectExpr("*", "explode(in_network) as network").select("*", "network.*").drop("in_network", "network")
        .selectExpr("*","explode(negotiated_rates) as negs_rate").drop("negotiated_rates")
            .selectExpr("*","explode(negs_rate.negotiated_prices) as negs_price").drop("negotiated_prices")
            .selectExpr("*","explode(negs_rate.provider_groups) as provider").drop("negs_rate")
            .selectExpr("*","explode(provider.npi) as npi").selectExpr("*","provider.tin.type as tin_type","provider.tin.value as tin").drop("provider_groups","provider")
            .select("*","negs_price.*").drop("negs_price"))
    
    nrpr=nrpr.withColumn("tin",regexp_replace(col("tin"),"-",""))
    nrpr = nrpr.withColumn("provider_group_id", hash(concat("npi", "tin")))
    nrpr = nrpr.filter(col("billing_code").isNotNull() & (col("billing_code") != "")).withColumn("service_code", col("service_code").cast(ArrayType(IntegerType())))

# rate table
    rate=nrpr.select("provider_group_id","billing_code","billing_code_type","negotiation_arrangement","billing_class","billing_code_modifier","negotiated_rate","negotiated_type","service_code")

# provider table
    provdier = nrpr.select("provider_group_id", "tin", "tin_type","npi")
    provdier =provdier.withColumn("tin_type",when(col("tin_type")=="ein",1).when(col("tin_type")=="npi",2))
    provdier = provdier.withColumn("tin_type",col("tin_type").cast(ShortType()))

# new provider scrub garako (taxonomy cha ayesma to join mrf ko provider sanga using tin & npi)
    column_drop = df1.drop('prv_fax','provider_name_prefix_text','prv_type_desc')
    mapped1= column_drop.withColumn('prv_type_code',when(col('prv_type_code')=="P",1).when(col('prv_type_code')=="F",2))
    mapped = mapped1.withColumn('prv_type_code',col('prv_type_code').cast(IntegerType()))
    merge = mapped.withColumn("full_name", concat_ws(" ","provider_first_name", "provider_middle_name","provider_last_name")).drop("provider_first_name","provider_last_name","provider_middle_name")
    merge = merge.select("*",col("loc.lat").alias("latitude").cast("double"),  col("loc.lon").alias("longitude").cast("double")).drop('loc')
    merge = merge.withColumn("taxonomy",array(col("prv_taxonomy_1_code"),col("prv_taxonomy_2_code"),col("prv_taxonomy_3_code"))).drop("prv_taxonomy_1_code","prv_taxonomy_2_code","prv_taxonomy_3_code")
    merge1 = merge.withColumn("prv_specialty",array(col("prv_specialty_1_desc"),col("prv_specialty_2_desc"),col("prv_specialty_2_desc"))).drop("prv_specialty_1_desc","prv_specialty_2_desc","prv_specialty_3_desc")
    merge1 = merge1.withColumn("taxonomy",array_except(col("taxonomy"),array(lit(None), lit("")))) \
            .withColumn("prv_specialty",array_except(col("prv_specialty"),array(lit(None), lit(""))))

# csv file lai rate sanga join garna ko lagi
    newcsv = df2.select('billing_code','taxonomy_list')
    return merge1,rate,df2,newcsv,provdier