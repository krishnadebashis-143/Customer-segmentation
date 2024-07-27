class Write_module():

    def __init__(self):
        pass

    def write_csv(self,df,mode,path):
         df.repartition(1).write.mode(mode).csv(path, header=True)





