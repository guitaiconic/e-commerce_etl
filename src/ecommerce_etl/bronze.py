from pyspark.sql.functions import current_timestamp, input_file_name
from ecommerce_etl.spark_session import get_spark_session


def get_bronze_data(
    input_path = "data/raw/orders.csv",
    output_path = "data/bronze/orders",
) -> None:
    spark = get_spark_session("BronzeIngestion")
    
    
    
    df = spark.read.csv(input_path, header =True, inferSchema = True)
    bronze_df = df.withColumn("ingested_at", current_timestamp()) \
                    .withColumn("source_file", input_file_name())
                          
    print(f"Bronze: Ingesting {bronze_df.count()} rows from {input_path}")
    
    
    
    bronze_df.write.format("delta").mode("overwrite").save(output_path)
    print(f"Bronze: sent to {output_path}")
    
    spark.stop()
    
if __name__ == "__main__":
    get_bronze_data()