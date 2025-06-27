from pyspark.sql import DataFrame
from dataclasses import dataclass

@dataclass(frozen=True)
class ValidatedDataFrame:
    valid: DataFrame
    invalid: DataFrame