#from  pyspark.sql.functions import lit,concat

import pyspark.sql.functions as F
from pyspark.sql.window import Window
# import arrow

class Transform():
    
    def __init__(self, df, start_date_end_date, spark,columns):
        self.df = df
        self.spark = spark
        self.start_date_end_date = start_date_end_date
        self.columns = columns
        
    
    def latency(self):
        """ Latency calculation uses windowing function """
        lag_features = []
        for lg in self.columns:
            if lg == 'track_id':
                lag_features.append('track_id')
            # if feature == 'organization':
            #   group_features.append('organization')
            if lg == 'product':
                lag_features.append('product')

        window_feature = Window.partitionBy(lag_features).orderBy('tranx_date')
        df = self.df.withColumn('lag_diff', F.datediff('tranx_date', F.lag('tranx_date').over(window_feature)))

        return df

    def transformation(self):
        """ This function will transform the data into RFM  """
        
        df = Transform.latency(self)
        self.start_date_end_date.createOrReplaceTempView('start_end_date')

        df.createOrReplaceTempView('df_sql')
        group_features = []
        agg_features = []
        condition  = []

        for feature in self.columns:
            if feature == 'track_id':
                agg_features.append('track_id as track_id')
                agg_features.append('count(track_id) as frequency')
            #if feature == 'organization':
             #   agg_features.append('organization as organization')
            if feature == 'amount':
                agg_features.append('round(sum(amount),2) as monetary_value')
            if feature == 'tranx_date':
                agg_features.append('min(tranx_date)  as min_tranx_date')
                agg_features.append('max(tranx_date)  as max_tranx_date')
                agg_features.append('datediff((select end_date from start_end_date), max(tranx_date)) as recency')
            if feature == 'product':
                agg_features.append('product  as product')
            if feature == 'orders':
                agg_features.append('sum(orders)  as total_orders')
            if feature == 'country':
                feature.append('country as country')
            if feature == 'customer_type':
                feature.append('customer_type as country')


        for feature in self.columns:
            if feature == 'track_id':
                group_features.append('track_id')
            #if feature == 'organization':
             #   group_features.append('organization')
            if feature == 'product':
                group_features.append('product')
            if feature == 'country':
                group_features.append('country')



        for feature in self.columns:
            if feature == 'tranx_date':
                condition.append(
                    'tranx_date between (select start_date from start_end_date) and (select end_date from start_end_date)')

        data = self.spark.sql( """ select uuid() as uid,
        {0},
        (select start_date from start_end_date) as start_date,
        (select end_date from start_end_date) as end_date,
        sum(lag_diff) as total_latency
         
        from df_sql

        where  {1}

        group by {2}

        """.format( ', '.join(agg_features),', '.join(condition),
                                                      ', '.join(group_features)) )  # .orderBy(F.asc("customers_min_transaction_date"))

        percentiles = [0.2, 0.4, 0.6, 0.8]

        recency_quantile = data.approxQuantile('recency', percentiles, 0)
        frequency_quantile = data.approxQuantile('frequency', percentiles, 0)
        monetary_value_quantile = data.approxQuantile('monetary_value', percentiles, 0)
        # print(recency_quantile[0])

        data = data.withColumn("r_score",
                               F.when(F.col("recency") <= recency_quantile[0], 5).
                               otherwise(F.when(F.col("recency") <= recency_quantile[1], 4).
                                   otherwise(F.when(F.col("recency") <= recency_quantile[2], 3).
                                   otherwise(
                                   F.when(F.col("recency") <= recency_quantile[3], 2).otherwise(1)
                               )))
                               )

        data = data.withColumn("f_score",
                               F.when(F.col("frequency") <= frequency_quantile[0], 1).
                               otherwise(F.when(F.col("frequency") <= frequency_quantile[1], 2).
                                   otherwise(F.when(F.col("frequency") <= frequency_quantile[2], 3).
                                   otherwise(
                                   F.when(F.col("frequency") <= frequency_quantile[3], 4).otherwise(5)
                               )))
                               )

        data = data.withColumn("m_score",
                               F.when(F.col("monetary_value") <= monetary_value_quantile[0], 1).
                               otherwise(F.when(F.col("monetary_value") <= monetary_value_quantile[1], 2).
                                   otherwise(F.when(F.col("monetary_value") <= monetary_value_quantile[2], 3).
                                   otherwise(
                                   F.when(F.col("monetary_value") <= monetary_value_quantile[3], 4).
                                       otherwise(5)
                               )
                               )
                               )
                               )

        data = data.withColumn('rfm_score', F.concat(F.col('r_score'),
                                                     F.col('f_score'), F.col('m_score')))

        data = data.withColumn('rfm_score_avg', F.round((F.col('r_score') +
                                                         F.col('f_score') + F.col('m_score')) / 3, 2))

        data = data.withColumn('final_rfm_score', (F.col('r_score') +
                                                   F.col('f_score') + F.col('m_score')))

        data = data.withColumn("Segments",
                               F.when(F.col("final_rfm_score") <= 6, 'Low Value Segment').
                               otherwise(F.when((F.col("final_rfm_score") >= 7) & (F.col("final_rfm_score") <= 10),
                                                "Mid Value Segment").
                                         otherwise('High Value Segment')
                                         )
                               )

        segt_map = {

            r'555': 'Champions',
            r'43[4-5]': 'Loyal',
            r'53[3-5]': 'Loyal',
            r'4[4-5][3-5]': 'Loyal',
            r'5[4-5][3-4]': 'Loyal',
            r'5[1-2][4-5]': 'New',
            r'545': 'New',
            r'4[1-2][4-5]': 'Recent',
            r'433': 'Recent',
            r'[4-5][3-5]2': 'Potential Loyalist',
            r'3[3-5][2-5]': 'Potential Loyalist',
            r'3[1-2][3-4]': 'Faithful',
            r'[4-5][4-5]1': 'Faithful',
            r'[4-5][1-2]3': 'Faithful',
            r'[4-5][1-2][1-2]': 'Price Sensitive',
            r'[4-5]31': 'Price Sensitive',
            r'11[4-5]': 'Cant Lose Them',
            r'1[2-5][3-5]': 'Hibernating',
            r'113': 'Hibernating',
            r'1[1-5][1-2]': 'Inactive',
            r'2[1-5][1-5]': 'At Risk',
            r'3[1-3][1-2]': 'Promising',
            r'3[3-5]1': 'Promising',
            r'3[1-2]5': 'Promising'

        }
        data = data.withColumn('sub_segments', F.col('rfm_score'))

        # data = data.withColumn('segments', F.col('rfm_score'))

        for key, value in segt_map.items():
            data = data.withColumn('sub_segments',
                                   F.regexp_replace(F.col("sub_segments"),
                                                    key, value))


        return data




    """def rfm_aggregation(self, data):

        data.createOrReplaceTempView('df')

        group_columns = []
        orders = []
        product = []
        ##org = []
        for feature in self.columns:

            if feature == 'product':
                group_columns.append('segments,sub_segments,f_score,r_score,m_score,\
                                          rfm_score_avg,product')
            else:
                group_columns.append('segments,sub_segments,f_score,r_score,m_score,\
                                          rfm_score_avg')
        for feature in self.columns:
            if feature == 'orders':
                orders.append(',sum(total_orders) as total_orders')
            else:
                pass

        for feature in self.columns:
            if feature == 'product':
                product.append(',product as product')
            else:
                pass


        final_result = self.spark.sql(select uuid() as uid,
                                          segments as segments,
                                          sub_segments as sub_segments,
                                          max(start_date) as start_date,
                                          max(end_date) as end_date,
                                          count(track_id) as total_customers,
                                          round(sum(recency),2) as total_recency,
                                          sum(frequency) as total_frequency,
                                          round(cast(sum(monetary_value) as double),2) as total_monetary_value,
                                          f_score as f_score,
                                          r_score as r_score,
                                          m_score as m_score   ,                 
                                          rfm_score_avg as rfm_score
                                          {0} {1}
                                        from df
                                        group by {2} .format(', '.join(orders),', '.join(product),', '.join(group_columns)))

        return final_result"""
