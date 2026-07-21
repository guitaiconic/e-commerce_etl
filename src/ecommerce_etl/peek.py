from pathlib import Path
import sys
from ecommerce_etl.spark_session import get_spark_session


def peek(table_path: str, num_rows: int =10) -> None:
    spark = get_spark_session("Peek")
    
    df = spark.read.format("delta").load(table_path)
    
    print(f"\nSchema for {table_path}:")
    df.printSchema()
    
    print(f"\nFirst {num_rows} rows:")
    df.show(num_rows, truncate=False)
    
    print(f"Total rows: {df.count()}")
    
    spark.stop()
    
    
if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "data/bronze/orders"
    peek(path)