# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pandas,json,logging
from scrapy.signals import spider_closed
from pydispatch import dispatcher
from scrapy.utils.project import get_project_settings
from .SQL import SQL

class Answers_Or_Askers(object):
    settings = get_project_settings()
    db_path = settings.get('DB_PATH')
    sql=dict()
    sql['answer']=SQL(db_path=db_path,db_table=settings.get('ANSWERS'))
    sql['asker']=SQL(db_path=db_path,db_table=settings.get('ASKERS'))

    def process_item(self, item, spider):
        self.sql[item['table']].insert(item=item['record'])
        return item

class Question_To_Csv(object):
    settings = get_project_settings()
    db_path = settings.get('DB_PATH')
    questions = SQL(db_path=db_path, db_table=settings.get('QUESTIONS'))

    def process_item(self, item, spider):
        self.questions.insert(item=item)
        return item

class Save_Users(object):
    settings = get_project_settings()
    db_path = settings.get('DB_PATH')
    users = SQL(db_path=db_path, db_table=settings.get('USERS'))

    def process_item(self, item, spider):
        self.users.insert(item=item)
        return item
