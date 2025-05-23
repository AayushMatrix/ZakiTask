from pyspark.sql.functions import size,array_intersect,col

def process_nrpr(merge1,rate,newcsv,provdier):


    new_rate = newcsv.join(rate,on="billing_code",how = "inner")
    new_pr = provdier.join(merge1, on=["npi","tin"], how="inner")
    taxonomy = new_pr.join(new_rate,on="provider_group_id",how="inner")

    specialized = taxonomy.filter(size(array_intersect(col("taxonomy_list"), col("taxonomy_list"))) > 0)

    specialized.show(5)

    
    


    

    rate_path = "file/rate_datanrpr.parquet"
    provider_path = "file/providernrpr_data.parquet"

    
    
    new_rate.write.parquet(rate_path,"overwrite")
    new_pr.write.parquet(provider_path,"overwrite")

    return rate_path,provider_path