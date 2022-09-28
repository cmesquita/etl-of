# Databricks notebook source
# MAGIC %md
# MAGIC ### Extract, Transform, Load Order Fulfillment - John Deere

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importing libraries

# COMMAND ----------

from monitoring.listener import MyListener
from utils.transformation import checkCondition, substringCondition

# MAGIC %md
# MAGIC #### Monitoring

# COMMAND ----------

my_listener = MyListener()
spark.streams.addListener(my_listener)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extract 

# COMMAND ----------

sales_stream = spark.readStream.format("delta") \
                 .option("maxFilesPerTrigger", 5) \
                 .table("sales")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transform 
                 
# COMMAND ----------

sales_stream_filtered = sales_stream.select(['doc_id','doc_type','purchase_order_no']) \
                        			.withColumn("comar_code",        checkCondition("doc_type", "purchase_order_no")) \
                                    .withColumn("doc_id",            substringCondition("doc_type","doc_id"))

# COMMAND ----------

sales_hist_df = spark.read.format('delta').table('sales_hist') 
sales_stream_antijoin = sales_stream.join(sales_hist_df,  sales_stream.doc_id == sales_hist_df.doc_id ,'leftanti')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load

# COMMAND ----------

sales_stream_filtered.writeStream \
    .option("checkpointLocation", "/tmp/sales_stream_filtered") \
    .queryName("sales_stream_filtered") \
    .trigger(processingTime='10 seconds') \
    .toTable("sales_stream_filtered") 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Quality

# COMMAND ----------
        
sales_stream_antijoin.writeStream \
    .option("checkpointLocation", "/tmp/sales_stream_antijoin") \
    .queryName("sales_stream_antijoin") \
    .trigger(processingTime='30 seconds') \
    .toTable("sales_stream_antijoin") 
