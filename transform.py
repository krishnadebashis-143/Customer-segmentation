from  pyspark.sql.functions import col

class TransformFeatures():

    def __init__(self):
        pass

    def read_avro_transform(self ,df ,dict_columns ,data_types,data_columns):
        df = df.select(data_columns)
        df = df.select([col(c).alias(dict_columns.get(c ,c)) for c in
                        df.columns]). \
            selectExpr(data_types
                       )

        return df

    def find_start_end_date(self,days,spark,start_date,end_date):

        if days != None:
            end_date = spark.sql(""" select cast(max(tranx_date) as date)  as end_date from rfm_table """)
            # end_date.createOrReplaceTempView(end_date)
            # end_date = transactions_detail_csv.select(F.max(F.col('transaction_date')).cast(DateType()).alias('end_date'))
            start_end_date = end_date.withColumn('start_date', F.date_sub(F.col("end_date"), int(days)))

            start_end_date.createOrReplaceTempView('start_end_date')
        else:
            start_end_date = spark.createDataFrame([(start_date, end_date)], ['start_date', 'end_date'])
            start_end_date.createOrReplaceTempView('start_end_date')

        return start_end_date
