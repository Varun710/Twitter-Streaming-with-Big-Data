import pyspark
import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

def send_data(tags: dict) -> None:

    url = 'http://localhost:3000/updateData'
    response = requests.post(url, json=tags)

def data_process(row: pyspark.sql.types.Row) -> None:

    tags = row.asDict()
    print(tags) 
    send_data(tags)

def spark():

    spark = SparkSession.builder.appName("TwitterStreaming").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    input = spark.readStream.format("socket").option("host", "127.0.0.1").option("port", 9009).load()
    words = input.select(explode(split(input.value, " ")).alias("hashtag"))
    word_count = words.groupBy("hashtag").count()
    query = word_count.writeStream.foreach(data_process).outputMode('Update').start()
    query.awaitTermination()

if __name__ == '__main__':

    try:
        spark()
    except KeyboardInterrupt:
        exit("Keyboard Interrupt")
    except BrokenPipeError:
        exit("Broken pipe")
    except Exception as e:
        # traceback.print_exc()
        exit("Error in Spark App")
