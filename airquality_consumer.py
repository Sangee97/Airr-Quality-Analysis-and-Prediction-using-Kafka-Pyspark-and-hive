from pyspark.sql import SparkSession
import os
from time import sleep
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

def consume_locations(input_df,checkpoint_path):

    input_df = input_df.withColumnRenamed("Predominant_Parameter", "pp_value")
    json_df = input_df.selectExpr("split(value,',')[0] as Country" \
                     ,"split(value,',')[1] as State" \
                     ,"split(value,',')[2] as City" \
                     ,"split(value,',')[3] as Station" \
                     ,"split(value,',')[4] as Place"\
                     ,"split(value,',')[5] as Latitude" \
                     ,"split(value,',')[6] as Longitude" \
                     ,"split(value,',')[7] as Last_Update" \
                     ,"split(value,',')[8] as Pollutant" \
                     ,"split(value,',')[9] as Min" \
                    ,"split(value,',')[10] as Max" \
                    ,"split(value,',')[11] as Avg" \
                    ,"split(value,',')[12] as AQI" \
                    ,"split(value,',')[13] as Predominant_Parameter" \
                    )

    # Stream the data, from a Kafka topic to a Spark in-memory table
    query = json_df\
        .writeStream \
        .format("memory") \
        .queryName("base_table") \
        .outputMode("append") \
        .option("checkpoint",checkpoint_path)\
        .start()

    query.awaitTermination(5)
    sleep(10)


def pyspark_consumer(spark,checkpoint_loc_path):

    trans_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "kafka_topic1") \
        .option("startingOffsets", "earliest") \
        .load()

    matchdata_locations = trans_df.selectExpr("CAST(value AS STRING) as value").filter("topic = 'kafka_topic1'")

    consume_locations(matchdata_locations, checkpoint_loc_path)


    while trans_df.isStreaming:
        trans_df = spark.sql("select * from base_table where state is not null and state != 'State'")
        trans_df.write.saveAsTable("air_quality",mode="overwrite")
        # trans_df.show()

        #dimension tables were created

        #dim_country table created
        # test_df = spark.sql(
        #     """select row_number() over(order by country,State,city,station,place,latitude,longitude) as country_id,
        # country,State,city,station,place,latitude,longitude from air_quality
        # group by country,state,city,station,place,latitude,longitude""")
        # test_df.write.saveAsTable("dimension_country1", mode="overwrite")
        # test_df.show()

        # dim_station table created
        test_df1=spark.sql("""select row_number() over(order by station,place,latitude,longitude) as station_id,
        station,place,latitude,longitude from air_quality
        group by station,place,latitude,longitude""")
        test_df1.write.saveAsTable('dimension_station',mode='overwrite')
        # test_df1.show()
        # #dim_pollutant table created
        # test_df2 = spark.sql("""select row_number() over(order by station,place,latitude,longitude) as date_id,station,
        # place,latitude,longitude,last_update from air_quality
        # group by station,place,latitude,longitude,last_update""")
        # test_df2.write.saveAsTable('dimension_date', mode='overwrite')

        # test_df2.show()

        # query1=spark.sql("select distinct aqi from air_quality")
        # query1.show()

        sleep(5)

def main():
    checkpoint_loc_path = "C:\\checkpoint"
    spark = SparkSession \
        .builder \
        .appName("Pyspark_consumer") \
        .enableHiveSupport()\
        .getOrCreate()

    pyspark_consumer(spark,checkpoint_loc_path)


if __name__ == "__main__":
    main()