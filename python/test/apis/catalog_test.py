from unittest import TestCase
from pyspark.sql import DataFrame
from pyspark.sql.catalog import Catalog
from unittest.mock import patch
from extensions.apis import *

class CatalogTest(TestCase):
    spark = None

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

    def method_exists(self, clazz, method_name):
        return hasattr(clazz, method_name) and callable(getattr(clazz, method_name))