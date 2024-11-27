from unittest import TestCase
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.catalog import Catalog
from unittest.mock import patch
from extensions.apis import *

class CatalogTest(TestCase):
    spark = SparkSession.builder.appName('Test').getOrCreate()

    def setUp(self):
        pass

    def test_methods_exist(self):
        self.assertTrue(self.method_exists(Catalog, 'deepCloneCatalog'))
        self.assertTrue(self.method_exists(Catalog, 'shallowCloneCatalog'))
        self.assertTrue(self.method_exists(Catalog, 'deepCloneSchema'))
        self.assertTrue(self.method_exists(Catalog, 'shallowCloneSchema'))
        self.assertTrue(self.method_exists(Catalog, 'showTablesExtended'))
        self.assertTrue(self.method_exists(DataFrame, 'filterBadRecordsOn'))
        self.assertTrue(self.method_exists(DataFrame, 'filterValidRecordsOn'))

        self.assertTrue(self.method_exists(self.spark.catalog, 'deepCloneCatalog'))
        self.assertTrue(self.method_exists(self.spark.catalog, 'shallowCloneCatalog'))
        self.assertTrue(self.method_exists(self.spark.catalog, 'deepCloneSchema'))
        self.assertTrue(self.method_exists(self.spark.catalog, 'shallowCloneSchema'))
        self.assertTrue(self.method_exists(self.spark.catalog, 'showTablesExtended'))
        

    def method_exists(self, clazz, method_name):
        return hasattr(clazz, method_name) and callable(getattr(clazz, method_name))