
import pandas as pd
import xml.etree.ElementTree as ET
import csv
from kafka3 import KafkaProducer
from kafka3 import KafkaConsumer


# reading the xml file and converting into csv file
tree=ET.parse("/Users/Sangeetha/Downloads/archive/data_aqi_cpcb.xml")
root = tree.getroot()
with open('AqIndex.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
# Writes the header
    header = [
        "Country", "State", "City", "Station", "Latitude", "Longitude", "Last Update",
        "Pollutant", "Min", "Max", "Avg", "AQI", "Predominant Parameter"
    ]
    writer.writerow(header)

    # Iterates through the XML structure and write rows
    for country in root.findall('Country'):
        country_id = country.get('id')
        for state in country.findall('State'):
            state_id = state.get('id')
            for city in state.findall('City'):
                city_id = city.get('id')
                for station in city.findall('Station'):
                    station_id = station.get('id')
                    latitude = station.get('latitude')
                    longitude = station.get('longitude')
                    last_update = station.get('lastupdate')

                    # Pollutant details
                    for pollutant in station.findall('Pollutant_Index'):
                        pollutant_id = pollutant.get('id')
                        min_val = pollutant.get('Min')
                        max_val = pollutant.get('Max')
                        avg_val = pollutant.get('Avg')

                        # Defaults AQI details to empty
                        aqi_value = ""
                        predominant_param = ""

                        # Checks for AQI details
                        aqi = station.find('Air_Quality_Index')
                        if aqi is not None:
                            aqi_value = aqi.get('Value')
                            predominant_param = aqi.get('Predominant_Parameter')

                        # Writes a row to the CSV
                        writer.writerow([
                            country_id, state_id, city_id, station_id, latitude, longitude,
                            last_update, pollutant_id, min_val, max_val, avg_val,
                            aqi_value, predominant_param
                        ])

# print("XML data has been successfully converted to CSV!")
output = pd.read_csv('AqIndex.csv')
# print(output)

# streaming the csv file in to kafka topic
producer = KafkaProducer(bootstrap_servers='localhost:9092',api_version=(0,11,5))
with open('/Users/Sangeetha/Desktop/milestoneproject_1/venv/AqIndex.csv', 'r') as csvfile:

    reader = csv.reader(csvfile)
    for row in reader:
        message = ','.join(row)
        producer.send('kafka_topic1', message.encode('utf-8'))
    producer.flush()
consumer = KafkaConsumer(
    'kafka_topic1',
    bootstrap_servers='localhost:9092',api_version=(0,11,5))
for message in consumer:
    print(message)