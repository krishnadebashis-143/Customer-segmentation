#from  pyspark.sql.functions import col

class Read_module():

    def read_avro_file(self,file_path ,file_format,spark):
        df = spark.read.format(file_format).load(file_path)
        return df








