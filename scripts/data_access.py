# Configuration
storage_account = "springboardcapstone1st"
container = "input"
sas_token = "xx"

# Set Spark configuration for blob storage
spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", 
    "SAS"
)
spark.conf.set(
    f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", 
    "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
)
spark.conf.set(
    f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", 
    sas_token
)

# Clean the SAS token
sas_token = sas_token.strip().lstrip('?')

display(dbutils.fs.ls(f"abfss://{container}@{storage_account}.dfs.core.windows.net/"))

# Access blob folders and files
dbutils.fs.ls(f"abfss://{container}@{storage_account}.dfs.core.windows.net/data/csv/2020-08-05/NYSE")
df = spark.read.text(f"abfss://{container}@{storage_account}.dfs.core.windows.net/data/csv/2020-08-05/NYSE/part-00000-5e4ced0a-66e2-442a-b020-347d0df4df8f-c000.txt")
df.show()

