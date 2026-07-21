from ast import alias

from pyspark.sql.function import col, count, round as spark_round, sum as spark_sum
from ecommerce_etl.spark_session import get_spark_session


def get_gold_data(
    input_path: str = "data/silver/orders",
    output_path: str = "data/gold"
) -> None:
    spark = get_spark_session("GoldAggregation")
    
    silver_df = spark.read.format("delta").load(input_path)
    
    # Daily revenue totals
    daily_revenue = (
        silver_df.groupBy("orders_date").agg(
            spark_round(spark_sum("total_price"), 2).alias("total_revenue"),
            count("order_id").alias("total_orders")
        ).orderBy("order_date")
    )
    
    
    
    # Top-selling products
    top_products = (
        silver_df.groupBy("product_name").agg(
            spark_sum("quantity").alias("total_unit_sold"),
            spark_round(spark_sum("total_price"), 2).alias("total_revenue")
        ).orderBy(col("total_revenue").desc())
    )
    
        
    
    
    
    # Revenue by Channel
    revenue_by_channel = (
        silver_df.groupBy("channel").agg(
            spark_round(spark_sum("total_price"), 2).alias("total_revenue"),
            count("order_id").alias("total_orders")
        ).orderBy(col("total_revenue").desc())   
    )
    
    
    
    
     # Revenue by Country
    revenue_by_country = {
        silver_df.groupBy("country").agg(
            spark_round(spark_sum("total_price"), 2).alias("total_revenue"),
            count("order_id").alias("total_orders")
        ).orderBy(col("total_revenue").desc())
    }
    
    
    
    
    
    
    tables = {
        "daily_revenue": daily_revenue,
        "top_products": top_products,
        "revenue_by_channel": revenue_by_channel,
        "revenue_by_country": revenue_by_country
    }
    
    for name, df in tables.items()