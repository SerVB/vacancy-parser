# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import logging
from time import gmtime, strftime
from urllib import parse

import pandas as pd
from sqlalchemy import Table, MetaData, Column, Integer, NVARCHAR, create_engine, select


class VacancyPipeline(object):
    def process_item(self, item, spider):
        return item


def create_my_engine():
    """ Creating connection to the database"""
    engine = None
    try:
        connection_str = open("connection_str.txt", "r", encoding="utf-8").read()
        cs = parse.quote_plus(connection_str)
        pa = "mssql+pyodbc:///?odbc_connect=%s" % cs
        engine = create_engine(pa)
    except Exception as e:
        print(e)
    return engine


class DbPipeline(object):
    collection_name = 'scrapy_items'

    def obtain_ver(self):
        m = MetaData(self.engine)
        t = Table('hh_main', m,
                  Column('date_add', NVARCHAR(None)),
                  Column('ver', Integer),
                  )

        stmt = select([t])

        return 1 + max({0}.union(
            map(
                lambda row: row[1],
                filter(
                    lambda row: row[0] == self.today,
                    self.engine.execute(stmt).fetchall()
                )
            )
        ))

    def __init__(self):
        self.today = strftime("%Y-%m-%d", gmtime())  # "yyyy-mm-dd"
        self.engine = create_my_engine()
        self.ver = self.obtain_ver()

        self.data = []

    def flush_to_db(self):
        logging.info("Flushing %s items..." % len(self.data))

        dff = pd.DataFrame(self.data)

        dff.to_sql("hh_main", con=self.engine, if_exists="append")

        logging.info("%s items have been flushed" % len(self.data))

        self.data.clear()

    def close_spider(self, spider):
        self.flush_to_db()

    def process_item(self, item, spider):
        entry = {
            "Date_add": self.today,
            "ver": self.ver,
        }
        entry.update(item)
        self.data.append(entry)

        if len(self.data) >= 1_000:
            self.flush_to_db()

        return item
