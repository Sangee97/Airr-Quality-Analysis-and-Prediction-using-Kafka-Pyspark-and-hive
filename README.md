# Airr-Quality-Analysis-and-Prediction-using-Kafka-Pyspark-and-hive
Overview:
This project involves analyzing air quality data from Indian cities to predict air quality trends. By creating a scalable data pipeline, we will process using data engineering tools like Python,Kafka,Pyspark,Hive and analyse to get some insights from those data.
Data Extraction:
Downloaded XML file from India air quality index set and converted XML file to CSV file using Python Script.
Data Ingestion:
Using Kafka producers to send data to topics and stored raw data in MySQL.
Data Processing:
Using Pyspark for streaming data from kafka topic and made some spark jobs for data cleaning and transformation
Loaded in Hive Table:
After loading into the hive table and performing some queries to get some data insights via charts.
