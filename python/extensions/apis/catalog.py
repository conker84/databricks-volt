from pyspark.sql.catalog import Catalog
from pyspark.sql import DataFrame
from typing import Optional
from py4j.java_gateway import JVMView

from extensions.utils.decorators import add_method

class SchemaIdentifier:
    def __init__(self, schema: str, catalog: Optional[str] = None):
        self.schema = schema
        self.catalog = catalog

    def to_jvm(self, jvm: JVMView):
        return (
            jvm
            .com.databricks.extensions.sql.command.metadata
            .SchemaIdentifier(self.schema, jvm.scala.Option.apply(self.catalog))
        )


@add_method(Catalog)
def _deepCloneCatalog(
        self,
        targetCatalog: str,
        managedLocation: str="",
        create: bool=True,
        replace: bool=False,
        ifNotExists: bool=False
    ) -> DataFrame:
    catalog = self._sparkSession._jsparkSession.catalog()
    jdf = (
        self._sparkSession._jvm
        .com.databricks.extensions.apis.CatalogExtensions
        .deepCloneCatalog(catalog, targetCatalog, managedLocation, create, replace, ifNotExists)
    )
    return DataFrame(jdf, self._sparkSession)

@add_method(Catalog)
def _shallowCloneCatalog(
        self,
        targetCatalog: str,
        managedLocation: str="",
        create: bool=True,
        replace: bool=False,
        ifNotExists: bool=False
    ) -> DataFrame:
    catalog = self._sparkSession._jsparkSession.catalog
    jdf = (
        self._sparkSession._jvm
        .com.databricks.extensions.apis.CatalogExtensions
        .shallowCloneCatalog(catalog, targetCatalog, managedLocation, create, replace, ifNotExists)
    )
    return DataFrame(jdf, self._sparkSession)

@add_method(Catalog)
def _deepCloneSchema(
        self,
        targetSchema: SchemaIdentifier,
        managedLocation: str="",
        create: bool=True,
        replace: bool=False,
        ifNotExists: bool=False
    ) -> DataFrame:
    jvm = self._sparkSession._jvm
    catalog = self._sparkSession._jsparkSession.catalog()
    jdf = (
        jvm
        .com.databricks.extensions.apis.CatalogExtensions
        .deepCloneSchema(catalog, targetSchema.to_jvm(jvm), managedLocation, create, replace, ifNotExists)
    )
    return DataFrame(jdf, self._sparkSession)

@add_method(Catalog)
def _shallowCloneSchema(
        self,
        targetSchema: SchemaIdentifier,
        managedLocation: str="",
        create: bool=True,
        replace: bool=False,
        ifNotExists: bool=False
    ) -> DataFrame:
    jvm = self._sparkSession._jvm
    catalog = self._sparkSession._jsparkSession.catalog()
    jdf = (
        jvm
        .com.databricks.extensions.apis.CatalogExtensions
        .shallowCloneSchema(catalog, targetSchema.to_jvm(jvm), managedLocation, create, replace, ifNotExists)
    )
    return DataFrame(jdf, self._sparkSession)

@add_method(Catalog)
def _showTablesExtended(self, filter: str="") -> DataFrame:
    catalog = self._sparkSession._jsparkSession.catalog()
    jdf = (
        self._sparkSession._jvm
        .com.databricks.extensions.apis.CatalogExtensions
        .showTablesExtended(catalog, filter)
    )
    return DataFrame(jdf, self._sparkSession)