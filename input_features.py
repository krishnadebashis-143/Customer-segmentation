

class Input():

    def __init__(self):
        pass

    def input_features(self,dict_columns):

        col_names = []

        data_columns = []

        for names in dict_columns.values():
            col_names.append(names)

        for names in dict_columns.keys():
            data_columns.append(names)

        return col_names,data_columns

    def input_datatypes(self,col_names):


        data_types = []


        for feature in col_names:
            if feature == 'track_id':
                data_types.append('cast(track_id as string)')
            if feature == 'organization':
                data_types.append('cast(organization as string)')
            if feature == 'amount':
                data_types.append('cast(amount as float)')
            if feature == 'tranx_date':
                data_types.append('cast(tranx_date as timestamp)')
            if feature == 'product':
                data_types.append('cast(product as string)')
            if feature == 'orders':
                data_types.append('cast(orders as float)')
            if feature == 'country':
                data_types.append('cast(country as string)')
            if feature == 'customer_type':
                data_types.append('cast(customer_type as string)')

        return data_types

