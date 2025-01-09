
from pyspark.sql import SparkSession
from matplotlib import pyplot

def max_originating_trans(spark):
    sql = '''
    select
     st.station as station,
     max(p.aqi) as aqi
     from dimension_station st
     join
     air_quality p
     on
     st.station = p.station
     group by
     st.station
     order by 2 desc 
     limit 20
    '''
    df = spark.sql(sql)
    df_data = df.collect()

    # create a numeric value for every label
    indexes = list(range(len(df_data)))

    # split the data into diff lists
    values = [row['station'] for row in df_data]
    labels = [row['aqi'] for row in df_data]

    # Plotting
    bar_width = 0.35

    # histogram
    pyplot.bar(indexes,values)

    # add labels
    labelidx = [i + bar_width for i in indexes]
    pyplot.xticks(labelidx,labels)
    pyplot.show()





def main():
    spark = SparkSession \
        .builder \
        .appName("test") \
        .enableHiveSupport()\
        .getOrCreate()

    max_originating_trans(spark)


if __name__ == "__main__":
    main()