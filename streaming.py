from pyspark.sql import SparkSession
from kafka.settings import TOPIC_NAME
from pyspark.sql.functions import from_json, col, to_date, unix_timestamp, date_format, when, concat, lit, regexp_replace, round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from helper import get_exchange_rate
from datetime import datetime

DAILY_FOLDER = datetime.now().strftime("transactions_%d%m%Y")

BASE_HDFS_PATH = "hdfs://laptop-qhs1r0bj.mshome.net:9000/user/odap"
CHECK_POINT_DIR = f"{BASE_HDFS_PATH}/{DAILY_FOLDER}/checkpoints"
OUTPUT_PATH = f"{BASE_HDFS_PATH}/{DAILY_FOLDER}/transactions_csv" # Đường dẫn lưu dữ liệu trên HDFS

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("TransactionStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .master("local[2]") \
    .config("spark.hadoop.dfs.blocksize", "16m") \
    .getOrCreate()

# Lấy tỷ giá USD/VND
usd_to_vnd = get_exchange_rate()

# Đọc dữ liệu từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()
# Dùng để không bị terminated khi streaming dữ liệu

# Chuyển value từ Kafka thành chuỗi
messages = df.selectExpr("CAST(value AS STRING) as json_value") 

# Định nghĩa schema cho dữ liệu JSON
schema = StructType([  
    StructField("User", IntegerType(), True),
    StructField("Card", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Month", IntegerType(), True),
    StructField("Day", IntegerType(), True),
    StructField("Time", StringType(), True),
    StructField("Amount", StringType(), True),  # Tiền tệ dưới dạng chuỗi
    StructField("Use Chip", StringType(), True),
    StructField("Merchant Name", StringType(), True),
    StructField("Merchant City", StringType(), True),
    StructField("Merchant State", StringType(), True),
    StructField("Zip", FloatType(), True),
    StructField("MCC", IntegerType(), True),
    StructField("Errors?", StringType(), True),
    StructField("Is Fraud?", StringType(), True)
])

# Parse chuỗi JSON thành DataFrame
data_df = messages.select(from_json(col("json_value"), schema).alias("data")).select("data.*")

# Xử lý chuyển đổi dữ liệu:
# Lọc các giao dịch hợp lệ (Is Fraud? == 'No')
valid_transactions = data_df.filter(col("Is Fraud?") == "No")

# Chuyển đổi tiền tệ từ USD sang VND
valid_transactions = valid_transactions.withColumn(
    "Amount_VND", 
    round(when(col("Amount").rlike(r"^\$"), 
           (regexp_replace(col("Amount"), r"[$]", "").cast("float") * usd_to_vnd)
          ).otherwise(0.0), 2)
)

# Định dạng lại tháng và ngày theo định dạng MM, dd
valid_transactions = valid_transactions.withColumn(
    "Month", when(col("Month") < 10, concat(lit("0"), col("Month").cast("string"))).otherwise(col("Month").cast("string"))
)
valid_transactions = valid_transactions.withColumn(
    "Day", when(col("Day") < 10, concat(lit("0"), col("Day").cast("string"))).otherwise(col("Day").cast("string"))
)
# Định dạng ngày theo kiểu dd/MM/yyyy, định dạng thời gian theo kiểu HH:mm:ss
formatted_transactions = valid_transactions \
    .withColumn("Date", date_format(
        to_date(concat(col("Day"), lit("/"), col("Month"), lit("/"), col("Year")), "dd/MM/yyyy"), "dd/MM/yyyy")) \
    .withColumn("Time", date_format(unix_timestamp(col("Time"), 'HH:mm').cast("timestamp"), 'HH:mm:ss'))

# Kiểm tra giá trị NULL và thay thế
formatted_transactions = formatted_transactions.fillna({
    "Zip": 0,           
    "Merchant State": "", 
    "Errors?": "No",         
})
# Thay thế giá trị "\NaN\"" thành "No" trong cột "Errors?" 
formatted_transactions = formatted_transactions.withColumn( "Errors?", regexp_replace(col("Errors?"), r"\\NaN\"\"", "No") )

# Ghi từng batch ra HDFS dưới dạng CSV
query = formatted_transactions \
    .coalesce(1) \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("header", "true") \
    .option("path", OUTPUT_PATH) \
    .option("checkpointLocation", CHECK_POINT_DIR) \
    .option("dfs.blocksize", "16m") \
    .trigger(processingTime="3 minutes") \
    .start()

# Chờ quá trình streaming
query.awaitTermination()