from ecommerce_etl import bronze
from pyspark.sql.functions import col, round as spark_round, when
from ecommerce_etl.spark_session import get_spark_session


def get_silver_data(
    input_path: str = "data/bronze/orders",
    output_path: str = "data/silver/orders",
) -> None:
    spark = get_spark_session("SilverCleaning")
    bronze_df = spark.read.format("delta").load(input_path)
    
    silver_df = (bronze_df.withColumn("is_valid_quantity", when(col("quantity") > 0, True).otherwise(False)) \
                            .withColumn("has_email", when(col("customer_email").isNotNull(), True).otherwise(False)) \
                            .withColumn("total_price", spark_round(col("unit_price") * col("quantity"), 2)).dropDuplicates(["order_id"])
                            
    )
    
    #SEPARATING CLEAN AND REJECTED FILTER LOGIC
   
    cleaned_df = silver_df.filter(
        (col("is_valid_quantity") == True) & (col("has_email") == True),
    )
    
    rejected_df = silver_df.filter(
        (col("is_valid_quantity") == False) | (col("has_email") == False)
    )
    
    print(f"Silver: {cleaned_df.count()} clean rows, {rejected_df.count()} rejected rows")
    
    cleaned_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path)
    rejected_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("data/silver/orders_rejected")
    
    print(f"Silver: clean data saved to {output_path}")
    print(f"Silver: rejected rows saved to data/silver/orders_rejected")
    
    spark.stop()
    
    
if __name__ == "__main__":
    get_silver_data()