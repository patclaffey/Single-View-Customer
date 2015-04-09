class SvcCountry:

    def __init__(self, country_code):
        self.country_code = country_code
        self.SVC_country_list = ['ID','ZA']
        self.db_name = ['ID','ZA']
        self.db_collection = ['BT_SVC_ID','BT_SVC_ZA']
        if self.country_code.upper() in self.SVC_country_list:
            self.country_index_num = self.SVC_country_list.index(self.country_code.upper())
        else:
            self.country_index_num = None

    def get_db_name(self):
        if self.country_index_num is not None:
            return self.db_name[self.country_index_num]
        else:
            return None

    def get_db_collection_name(self):
        if self.country_index_num is not None:
            return self.db_collection[self.country_index_num]
        else:
            return None
