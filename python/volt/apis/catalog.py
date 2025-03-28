from pyspark.sql.catalog import Catalog
from pyspark.sql import DataFrame, Column
from typing import Optional, Union
from py4j.java_gateway import JVMView

from volt.utils.decorators import add_method

class SchemaIdentifier:
    def __init__(self, schema: str, catalog: Optional[str] = None):
        self.schema = schema
        self.catalog = catalog

    def to_jvm(self, jvm: JVMView):
        return (
            jvm
            .com.databricks.volt.sql.command.metadata
            .SchemaIdentifier(self.schema, jvm.scala.Option.apply(self.catalog))
        )


@add_method(Catalog)
def _deepCloneCatalog(
        self,
        targetCatalog: str,
        managedLocation: str="",
        ifNotExists: bool=False,
        isFull: bool=False
    ) -> DataFrame:
    catalog = self._sparkSession._jsparkSession.catalog()
    jdf = (
        self._sparkSession._jvm
        .com.databricks.volt.apis.CatalogExtensions
        .deepCloneCatalog(catalog, targetCatalog, managedLocation, ifNotExists, isFull)
    )
    return DataFrame(jdf, self._sparkSession)

@add_method(Catalog)
def _shallowCloneCatalog(
        self,
        targetCatalog: str,
        managedLocation: str="",
        ifNotExists: bool=False,
        isFull: bool=False
    ) -> DataFrame:
    catalog = self._sparkSession._jsparkSession.catalog
    jdf = (
        self._sparkSession._jvm
        .com.databricks.volt.apis.CatalogExtensions
        .shallowCloneCatalog(catalog, targetCatalog, managedLocation, ifNotExists, isFull)
    )
    return DataFrame(jdf, self._sparkSession)

@add_method(Catalog)
def _deepCloneSchema(
        self,
        targetSchema: SchemaIdentifier,
        managedLocation: str="",
        ifNotExists: bool=False,
        isFull: bool=False
    ) -> DataFrame:
    jvm = self._sparkSession._jvm
    catalog = self._sparkSession._jsparkSession.catalog()
    jdf = (
        jvm
        .com.databricks.volt.apis.CatalogExtensions
        .deepCloneSchema(catalog, targetSchema.to_jvm(jvm), managedLocation, ifNotExists, isFull)
    )
    return DataFrame(jdf, self._sparkSession)

@add_method(Catalog)
def _shallowCloneSchema(
        self,
        targetSchema: SchemaIdentifier,
        managedLocation: str="",
        ifNotExists: bool=False,
        isFull: bool=False
    ) -> DataFrame:
    jvm = self._sparkSession._jvm
    catalog = self._sparkSession._jsparkSession.catalog()
    jdf = (
        jvm
        .com.databricks.volt.apis.CatalogExtensions
        .shallowCloneSchema(catalog, targetSchema.to_jvm(jvm), managedLocation, ifNotExists, isFull)
    )
    return DataFrame(jdf, self._sparkSession)

@add_method(Catalog)
def _showTablesExtended(self, filter: Union[str, Column]) -> DataFrame:
    catalog = self._sparkSession._jsparkSession.catalog()
    if isinstance(filter, Column):
        filter = filter._jc
    jdf = (
        self._sparkSession._jvm
        .com.databricks.volt.apis.CatalogExtensions
        .showTablesExtended(catalog, filter)
    )
    return DataFrame(jdf, self._sparkSession)