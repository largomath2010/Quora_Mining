import sqlite3
import os,logging

class SQL:
    fail_path = os.path.join(os.getcwd(), 'fail_sql.txt')
    select_all_querry='select * from {0}'
    fields_name_querry='pragma table_info({0})'
    select_querry = 'select {0} from {1} where {2}="{3}";'
    update_querry='UPDATE {0} SET {1} WHERE {2} = "{3}";'
    insert_querry='insert into {0} ({1}) values ({2});'
    set_querry='{0}="{1}"'
    def __init__(self,**kwargs):
        self.db_path=kwargs['db_path']
        self.db_table=kwargs['db_table']
        self.db_pool = sqlite3.connect(self.db_path)
        self.cursor = self.db_pool.cursor()

    def select(self,**kwargs):
        list_label=kwargs['list_label']
        des_label = kwargs['des_label']
        source_value=kwargs['source_value']

        sql = self.select_querry.format(','.join(list_label),self.db_table,self.db_table+"."+des_label,source_value)

        return [{tup[0]:tup[1] for tup in zip(list_label,values)} for values in self.cursor.execute(sql)]

    def select_all(self, **kwargs):
        sql = self.select_all_querry.format(self.db_table)
        fields_name=self.fields_name()
        return [dict(zip(fields_name,item)) for item in self.cursor.execute(sql)]

    def fields_name(self,**kwargs):
        sql=self.fields_name_querry.format(self.db_table)
        return [tup[1] for tup in self.cursor.execute(sql)]

    def update(self,**kwargs):
        list_label = kwargs['list_label']
        list_value=kwargs['list_value']
        des_label = kwargs['des_label']
        source_value = kwargs['source_value']

        data=r','.join([self.set_querry.format(field,value) for field,value in zip(list_label,list_value)])
        querry=self.update_querry.format(self.db_table,data,self.db_table + "." + des_label, source_value)
        try:
            self.cursor.execute(querry)
            self.db_pool.commit()
            logging.info('Successfully update: '+des_label+'-'+str(source_value))
            return True
        except Exception as err:
            Error_Text = str(err) + ' while trying to update: ' + des_label+'-'+str(source_value) + '\n'
            logging.warn(Error_Text)
            open(self.fail_path, 'a+').write(Error_Text)
            return False

    def insert(self,**kwargs):
        item = kwargs['item']

        try:
            self.__insert_data(item)
            self.db_pool.commit()
            logging.info('Successfully insert: ' + str(item))
            return True
        except Exception as err:
            Error_Text = str(err) + ' while trying to commit item: ' + str(item) + '\n'
            logging.warn(Error_Text)
            open(self.fail_path, 'a+').write(Error_Text)
            return False

    def __insert_data(self, item):
        labels = r','.join(item.keys())
        data=[str(value) for value in item.values()]
        qm = r','.join([r'?'] * len(data))
        querry = self.insert_querry.format(self.db_table, labels, qm)
        self.cursor.execute(querry, data)

if __name__=='__main__':
    print('Succesfully import')
