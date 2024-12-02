from volt.utils.decorators import add_method
from pyspark.sql import DataFrame


@add_method(DataFrame)
def _filterBadRecordsOn(
        self,
        tableName: str
    ) -> DataFrame:
    jdf = (
        self.sparkSession._jvm
        .com.databricks.volt.apis.DataFrameExtensions
        .filterBadRecordsOn(self._jdf, tableName)
    )
    return DataFrame(jdf, self.sparkSession)

@add_method(DataFrame)
def _filterValidRecordsOn(
        self,
        tableName: str
) -> DataFrame:
    jdf = (
        self.sparkSession._jvm
        .com.databricks.volt.apis.DataFrameExtensions
        .filterValidRecordsOn(self._jdf, tableName)
    )
    return DataFrame(jdf, self.sparkSession)