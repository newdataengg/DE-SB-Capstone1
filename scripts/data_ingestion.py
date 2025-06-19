import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import col, when, split, regexp_extract, size, trim
import logging

# Configuration
container_name = "input"
storage_name = "springboardcapstone1st"
sas_token = "sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-08-31T06:20:25Z&st=2025-06-13T22:20:25Z&spr=https&sig=hqXfl1w9w%2B7KvoHpUQx8qp7e2tDOpjJQ8RKvaW7yAZw%3D"

# Directory paths
csv_dir_1 = "/data/csv/2020-08-05/NYSE/"
csv_dir_2 = "/data/csv/2020-08-06/NYSE/"
json_dir_1 = "/data/json/2020-08-05/NASDAQ/"
json_dir_2 = "/data/json/2020-08-06/NASDAQ/"

# Define the common schema
commonEventSchema = StructType([
    StructField("trade_dt", StringType(), True),
    StructField("file_tm", StringType(), True),
    StructField("record_type", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("event_tm", StringType(), True),
    StructField("event_seq_nb", IntegerType(), True),
    StructField("exchange", StringType(), True),
    StructField("bid_pr", DecimalType(18, 2), True),
    StructField("bid_size", IntegerType(), True),
    StructField("ask_pr", DecimalType(18, 2), True),
    StructField("ask_size", IntegerType(), True),
    StructField("partition", StringType(), True)
])

# Get Spark session
spark = SparkSession.getActiveSession()
if spark is None:
    spark = SparkSession.builder.appName("ADLS Gen2 Data Ingestion").getOrCreate()

print("Starting ADLS Gen2 data ingestion process...")

try:
    # Method 1: Mount storage using wasbs://
    mount_point = "/mnt/adlsgen2"
    source_url = f"wasbs://{container_name}@{storage_name}.blob.core.windows.net/"
    
    print(f"Attempting to mount {source_url} to {mount_point}")
    
    # Check if already mounted
    existing_mounts = [mount.mountPoint for mount in dbutils.fs.mounts()]
    if mount_point not in existing_mounts:
        dbutils.fs.mount(
            source=source_url,
            mount_point=mount_point,
            extra_configs={
                f"fs.azure.sas.{container_name}.{storage_name}.blob.core.windows.net": sas_token
            }
        )
        print("✓ Mount successful!")
    else:
        print("✓ Storage already mounted")
    
    # Test mount by listing root directory
    root_items = dbutils.fs.ls(mount_point)
    print(f"✓ Mount verification successful - found {len(root_items)} items in root")
    
    # Function to process CSV files using pure DataFrame operations
    def process_csv_files(file_path, file_description):
        print(f"\n--- Processing {file_description} ---")
        try:
            # Read as text file
            df_raw = spark.read.text(file_path)
            row_count = df_raw.count()
            print(f"Found {row_count} lines in {file_description}")
            
            if row_count == 0:
                print(f"No data found in {file_description}")
                return None
            
            # Show sample of raw data
            print("Sample raw lines:")
            df_raw.show(3, truncate=False)
            
            # Split CSV lines and create structured data
            df_split = df_raw.select(
                split(col("value"), ",").alias("fields")
            ).filter(
                size(col("fields")) >= 9  # Ensure minimum required fields
            )
            
            # Extract fields for CSV format
            df_structured = df_split.select(
                col("fields").getItem(0).alias("trade_dt"),
                col("fields").getItem(1).alias("file_tm"),
                col("fields").getItem(2).alias("record_type"),
                col("fields").getItem(3).alias("symbol"),
                col("fields").getItem(4).alias("event_tm"),
                col("fields").getItem(5).cast("int").alias("event_seq_nb"),
                col("fields").getItem(6).alias("exchange"),
                col("fields").getItem(7).cast("decimal(18,2)").alias("bid_pr"),
                col("fields").getItem(8).cast("int").alias("bid_size"),
                when(col("fields").getItem(2) == "Q", 
                     col("fields").getItem(9).cast("decimal(18,2)")).alias("ask_pr"),
                when(col("fields").getItem(2) == "Q", 
                     col("fields").getItem(10).cast("int")).alias("ask_size"),
                col("fields").getItem(2).alias("partition")
            ).filter(
                col("record_type").isin("T", "Q")  # Only process Trade and Quote records
            )
            
            processed_count = df_structured.count()
            print(f"✓ Successfully processed {processed_count} records from {file_description}")
            
            # Show sample processed data
            print("Sample processed data:")
            df_structured.show(5, truncate=False)
            
            return df_structured
            
        except Exception as e:
            print(f"✗ Error processing {file_description}: {e}")
            return None
    
    # Function to process JSON files using DataFrame operations
    def process_json_files(file_path, file_description):
        print(f"\n--- Processing {file_description} ---")
        try:
            # Read as text file first
            df_raw = spark.read.text(file_path)
            row_count = df_raw.count()
            print(f"Found {row_count} lines in {file_description}")
            
            if row_count == 0:
                print(f"No data found in {file_description}")
                return None
            
            # Show sample of raw data
            print("Sample raw lines:")
            df_raw.show(3, truncate=False)
            
            # Try to parse as JSON
            try:
                df_json = spark.read.json(file_path)
                json_count = df_json.count()
                print(f"Successfully parsed {json_count} JSON records")
                
                # Show schema
                print("JSON schema:")
                df_json.printSchema()
                
                # Transform to common schema
                df_structured = df_json.select(
                    col("trade_dt"),
                    col("file_tm"), 
                    col("event_type").alias("record_type"),
                    col("symbol"),
                    col("event_tm"),
                    col("event_seq_nb").cast("int"),
                    col("exchange"),
                    col("bid_pr").cast("decimal(18,2)"),
                    col("bid_size").cast("int"),
                    when(col("event_type") == "Q", col("ask_pr").cast("decimal(18,2)")).alias("ask_pr"),
                    when(col("event_type") == "Q", col("ask_size").cast("int")).alias("ask_size"),
                    col("event_type").alias("partition")
                ).filter(
                    col("record_type").isin("T", "Q")  # Only process Trade and Quote records
                )
                
                processed_count = df_structured.count()
                print(f"✓ Successfully processed {processed_count} records from {file_description}")
                
                # Show sample processed data
                print("Sample processed data:")
                df_structured.show(5, truncate=False)
                
                return df_structured
                
            except Exception as json_error:
                print(f"JSON parsing failed: {json_error}")
                print("Trying line-by-line JSON parsing...")
                
                # Fallback: parse JSON line by line using raw text
                # This would require RDD operations, so we'll skip for now
                print("Line-by-line JSON parsing not implemented in this version")
                return None
                
        except Exception as e:
            print(f"✗ Error processing {file_description}: {e}")
            return None
    
    # Process all file types
    dataframes = []
    
    # Process CSV files
    csv_paths = [
        (f"{mount_point}{csv_dir_1}*.txt", "CSV Directory 1 (NYSE 2020-08-05)"),
        (f"{mount_point}{csv_dir_2}*.txt", "CSV Directory 2 (NYSE 2020-08-06)")
    ]
    
    for path, description in csv_paths:
        try:
            # Check if files exist
            dir_path = path.replace("*.txt", "")
            files = dbutils.fs.ls(dir_path)
            txt_files = [f for f in files if f.name.endswith('.txt')]
            
            if txt_files:
                print(f"Found {len(txt_files)} .txt files in {description}")
                df = process_csv_files(path, description)
                if df is not None:
                    dataframes.append(df)
            else:
                print(f"No .txt files found in {description}")
                
        except Exception as e:
            print(f"Error accessing {description}: {e}")
    
    # Process JSON files
    json_paths = [
        (f"{mount_point}{json_dir_1}*.txt", "JSON Directory 1 (NASDAQ 2020-08-05)"),
        (f"{mount_point}{json_dir_2}*.txt", "JSON Directory 2 (NASDAQ 2020-08-06)")
    ]
    
    for path, description in json_paths:
        try:
            # Check if files exist
            dir_path = path.replace("*.txt", "")
            files = dbutils.fs.ls(dir_path)
            txt_files = [f for f in files if f.name.endswith('.txt')]
            
            if txt_files:
                print(f"Found {len(txt_files)} .txt files in {description}")
                df = process_json_files(path, description)
                if df is not None:
                    dataframes.append(df)
            else:
                print(f"No .txt files found in {description}")
                
        except Exception as e:
            print(f"Error accessing {description}: {e}")
    
    # Union all DataFrames
    if dataframes:
        print(f"\n--- Combining {len(dataframes)} DataFrames ---")
        
        # Union all dataframes
        combined_df = dataframes[0]
        for df in dataframes[1:]:
            combined_df = combined_df.union(df)
        
        # Get statistics
        total_records = combined_df.count()
        print(f"✓ Total combined records: {total_records}")
        
        # Show record type distribution
        print("\nRecord type distribution:")
        combined_df.groupBy("partition").count().orderBy("count", ascending=False).show()
        
        # Show exchange distribution
        print("\nExchange distribution:")
        combined_df.filter(col("exchange").isNotNull()) \
                  .groupBy("exchange").count() \
                  .orderBy("count", ascending=False).show()
        
        # Show sample of final data
        print("\nSample of combined data:")
        combined_df.show(10, truncate=False)
        
        # Write to output
        output_path = f"{mount_point}/output/processed_data"
        print(f"\nWriting processed data to: {output_path}")
        
        combined_df.write \
                  .partitionBy("partition") \
                  .mode("overwrite") \
                  .option("compression", "snappy") \
                  .parquet(output_path)
        
        print("✓ Data successfully written to output location")
        
        # Create temporary view for SQL analysis
        combined_df.createOrReplaceTempView("market_data")
        
        # Sample analytics
        print("\n--- Sample Analytics ---")
        
        spark.sql("""
            SELECT 
                partition as record_type,
                COUNT(*) as record_count,
                COUNT(DISTINCT symbol) as unique_symbols,
                COUNT(DISTINCT exchange) as unique_exchanges
            FROM market_data 
            GROUP BY partition
            ORDER BY record_count DESC
        """).show()
        
        print("🎉 Data processing completed successfully!")
        
    else:
        print("❌ No DataFrames were created - no data to process")
        
except Exception as e:
    print(f"❌ Error during processing: {e}")
    import traceback
    traceback.print_exc()

finally:
    print("\n--- Script execution completed ---")