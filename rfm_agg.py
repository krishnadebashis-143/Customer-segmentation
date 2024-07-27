# RFM Aggregation Class

class rfm_agg():



    def rfm_(self, data,columns,spark):
        data.createOrReplaceTempView('df')
        group_columns = []
        fields = []

        for feature in columns:

            if feature == 'product':
                group_columns.append('segments,sub_segments,product')
            else:
                group_columns.append('segments,sub_segments')

        for feature in columns:

            if feature == 'orders':
                fields.append('sum(total_orders) as total_orders')

            if feature == 'product':
                fields.append('product as product')

            if feature == 'country':
                fields.append('country as country')

            if feature == 'customer_type':
                fields.append('customer_type as customer_type')

            else:
                pass

        """for feature in self.columns:
            if feature == 'organization':
                org.append(',organization as organization')
            else:
                pass"""

        if fields != []:
            fields = ',' + ','.join(fields)
        else:
            fields = ','.join(fields)

        final_result = spark.sql(""" select
                                             segments as segments,
                                             sub_segments as sub_segments,
                                             count(track_id) as total_customers,
                                             concat(min(monetary_value),'-',max(monetary_value)) as monetary_value_range,
                                             concat(min(recency),'-',max(recency) ,' ','days') as range_of_recency,
                                             concat(min(frequency),'-',max(frequency) ) as range_of_frequency,
                                             min(monetary_value)  as min_monetary_value,
                                             max(monetary_value)  as max_monetary_value,
                                             min(recency) as min_recency,
                                             max(recency) as max_recency,
                                             min(frequency) min_frequency,
                                             max(frequency)  as max_frequency,
                                             round(avg(monetary_value),2) as avg_monetary_value,
                                             round(avg(r_score),2) as r_score,
                                             round(avg(f_score),2) as f_score,
                                             round(avg(m_score),2) as m_score  ,
                                             concat(min(final_rfm_score),'-',max(final_rfm_score) ) as range_of_rfm_score,
                                             round(avg(final_rfm_score),2) as final_rfm_score,
                                             round(avg(rfm_score_avg),2) as rfm_score_avg,
                                             concat(cast(max(start_date) as date) ,'-', cast(max(end_date) as date)  ) as date_range,
                                             round(sum(recency),2) as total_recency,
                                             round(sum(frequency),2) as total_frequency,
                                             round(sum(monetary_value),2) as total_monetary_value,
                                             round(sum(total_latency),2) as total_latency,
                                             round(avg(total_latency),2) as avg_latency
                                             {0} 
                                             from df
                                             group by {1} """.format(fields, ', '.join(group_columns)))

        # final_result.withcolumn('date_range',lit(start_date_end_date.selectExp(" concat(cast(max(start_date) as date) ,'-', cast(max(end_date) as date)  )")))

        return final_result
