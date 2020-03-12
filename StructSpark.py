from pyspark.sql import SparkSession
from pyspark.sql.functions import window
from pyspark.sql.functions import split
import json
from pyspark.sql.types import StringType
from pyspark.sql.functions import substring, length, expr



class StructSpark:
    def __init__(self, config):
        for i in config['SparkConfig']:
            self.address = "localhost"
            self.port = "9999"
            self.window_size = i['window_size']
            self.overlap_window_size = i['overlap_window_size']
            self.Outputmode = i['Outputmode']
            self.watermark = i['watermark']

        for i in config['Operators']:
            self.column = i['column']
            self.operation = i['operation']
            self.filter_value = str(i['filter_value'])
            self.trans_allowed = i['trans_allowed']
        self.spark = SparkSession.builder.appName("StructuredWordcount").getOrCreate()
        self.lines = self.spark.readStream.format('socket').option('host', self.address).option('port',self.port).option('includeTimestamp', 'true').load()

    def getonline(self):

        # split lines into words:
        words = self.lines.select(split(self.lines.value, ',').alias("value"), self.lines.timestamp)

        # creating structure for data-
        split_words = words.select((split(words.value[0], ',')).alias("name"), (split(words.value[1], ',')).alias("transaction_amount"), self.lines.timestamp)

        # casting the datatype to use it in query
        cast_string = split_words.select(split_words.name.cast(StringType()), split_words.transaction_amount.cast(StringType()), split_words.timestamp)

        # formatting the transaction_amt as it contains some special characters-
        split_words1 = cast_string.withColumn('transaction_amt', expr("substring(transaction_amount,2,length(transaction_amount)-2)"))

        # query generator-
        query = self.column + ' ' + self.operation + ' ' + self.filter_value

        # filter applied on the transaction amount
        filtered_1 = split_words1.select('name', 'timestamp', 'transaction_amt').where(query)

        # adding window size and watermark.
        windowedCount = filtered_1.withWatermark("timestamp", "10 minutes").groupBy(window(filtered_1.timestamp, "5 minutes", "5 minutes"), filtered_1.name,).count()

        # query2 generator
        query2 = 'count > ' + self.trans_allowed

        # Filter on count
        final_df = windowedCount.select('*').where(query2)

        # start the  stream and output on console.
        # stream = final_df.writeStream.outputMode(self.Outputmode).format("console").start()
        stream = final_df.writeStream.queryName('table1').outputMode('complete').format("memory").start()

        # call action
        self.trigger()
        # will be streaming till termination
        stream.awaitTermination()

    def trigger(self):
        # write query
        df = (self.spark.sql("select * from table1"))
        df.toPandas().to_csv('/home/vaibhav/Desktop/file.csv','a')


with open("/home/vaibhav/Desktop/SparkConfig.json") as f:
    config = json.load(f)
call1 = StructSpark(config)
call1.getonline()