# Volt
Volt is library that aims to simplify the life of Data Engineers within the Databricks environment.

**Nota Bene**: The development is mainly driven by Databricks field engineering. However, the library is not an official product of Databricks.

## Core aspects

It provides APIs in SQL, Scala and Python for:

- Retrieving table metadata in bulk; like retrieving table footprint on the File System and more
- Clone catalog/schemas

## Limitations

Currently we only support Personal Compute as we use some low-level APIs to retrieve the required information (like DeltaLog, for table snapshot without doing a `DESCRIBE DETAIL`)

Another important limitation is that we cannot reuse stored credentials so you need to define secrets with read only access to the cloud storage (see **Read files from S3/Azure** for more details)

## How to install it

First you need this file:
- `volt-<major.minor.patch>.jar`

Then, if required you can also get these files:
- `volt-aws-deps-<major.minor.patch>.jar`
- `volt-azure-deps-<major.minor.patch>.jar`

In order to install all the required dependencies you can choose between two init scripts:
- `install_from_volume.sh` which will install the dependencies from the volume
- `install_from_github.sh` which will install the dependencies from GitHub

For both ways you need also to define the following Spark configuration:

```
spark.sql.extensions com.databricks.volt.sql.SQLExtensions

spark.jars /databricks/jars/volt-${env:VOLT_VERSION}.jar,/databricks/jars/volt-azure-deps-${env:VOLT_VERSION}.jar,/databricks/jars/volt-aws-deps-${env:VOLT_VERSION}.jar
```

### Install from volume
If you choose `install_from_volume.sh` you will need to define two environment variables in order to work:
- `VOLT_VERSION` the version that you want to use
- `VOLUME_PATH` the volume path where the jars are downloaded

### Install from GitHub
If you choose `install_from_github.sh` you will need to define one environment variables in order to work:
- `VOLT_VERSION` the version that you want to download

### Read files from S3

If you want support for reading files from S3 you need to install also:

- `volt-aws-deps-<major.minor.patch>.jar`

Moreover you need to define a secret scope `aws-s3-credentials` and add the following keys with the relative values:
- `client_id`: is the **AWS access key ID**
- `client_secret`: is the **AWS secret access key**
- `session_token` (optional): is the **AWS session token**, if your access needs it

### Read files from Azure

If you want support for reading files from ADLS you need to install also:

- `volt-auzre-deps-<major.minor.patch>.jar`

Moreover you need to define a secret scope `adls-sp-credentials` and add the following keys with the relative values:
- `tenant_id`: is the **AZURE SP tenant ID**
- `client_id`: is the **AZURE SP client ID**
- `client_secret`: is the **AZURE SP client secret**

## Help us grow

If you need a feature please let us know filling an issue. We're glad to help you.

## Clone Catalog/Schemas

Volt provides an easy way, to deep/shallow clone catalog and schemas.

No more complicated configurations like [this](https://community.databricks.com/t5/technical-blog/uc-catalog-cloning-an-automated-approach/ba-p/53460) or scripts like [this](https://github.com/vnderson/databricks-clone-catalog/blob/main/databrics_clone_catalog.ipynb)

The execution of the command will return a report table with the following fields:
- `table_catalog` (StringType)
- `table_schema` (StringType)
- `table_name` (StringType)
- `status` (StringType): it's OK/ERROR
- `status_messages` (ArrayType(StringType)) in case of status ERROR you can get the error message here

### SQL API

In SQL it looks like the following:

```sql
-- example
CREATE CATALOG|SCHEMA target_catalog DEEP|SHALLOW CLONE source_catalog [MANAGED LOCATION '<your-location>']
```

```sql
-- N.b. this actually works
create catalog as_catalog_clone DEEP clone as_catalog;
```

### Scala API

```scala
import com.databricks.volt.apis.CatalogExtensions._
spark.catalog.setCurrentCatalog("as_catalog")
display(spark.catalog.deepCloneCatalog("as_catalog_clone"))
```

### Python API

```python
from volt.apis import *
spark.catalog.setCurrentCatalog("as_catalog")
display(spark.catalog.deepCloneCatalog("as_catalog_clone"))
```

## Retrieve the table metadata for a schema or catalog

How many times you need to get the metadata details of a (DELTA) table in bulk for a specific catalog or schema?

We have a feature for that and it works from SQL/Scala/Python

The result of the invocation is a table with the following colums:

- `table_catalog` (StringType)
- `table_schema` (StringType)
- `table_name` (StringType)
- `table_type` (StringType)
- `data_source_format` (StringType)
- `storage_path` (StringType)
- `created` (TimestampType)
- `created_by` (StringType)
- `last_altered_by` (StringType)
- `last_altered` (TimestampType)
- `liquid_clustering_cols` (ArrayType(StringType))
- `size` (StructType) has the following fields:
    - `full_size_in_gb` (DoubleType)
    - `full_size_in_bytes` (LongType)
    - `last_snapshot_size_in_gb` (DoubleType)
    - `last_snapshot_size_in_bytes` (LongType)
    - `delta_log_size_in_gb` (DoubleType)
    - `delta_log_size_in_bytes` (LongType)

You can filter for all the columns but you need to know that only for the following fields the filter will be pushed down:
- `table_catalog`
- `table_schema`
- `table_name`
- `table_type`
- `data_source_format`
- `storage_path`
- `created`
- `created_by`
- `last_altered_by`
- `last_altered`

Filtering for:
- `liquid_clustering_cols`
- `size`

will cause a full UC scan so please don't filter for it right now (or put the original query in a dataframe and than filter it).

### SQL API

In SQL it looks like the following:

```sql
-- example
show tables extended [WHERE <filters>]
```

```sql
show tables extended WHERE table_catalog = 'as_catalog'
```

### Scala API

```scala
import com.databricks.volt.apis.CatalogExtensions._
import org.apache.spark.sql.functions
display(spark.catalog.showTablesExtended("table_catalog = 'as_catalog'"))
// you can also pass a Column
// display(spark.catalog.showTablesExtended(functions.col("table_catalog").equalTo("as_catalog")))
```

### Python API

```python

from volt.apis import *
import pyspark.sql.functions as F
# display(spark.catalog.showTablesExtended("table_catalog = 'as_catalog'"))
# you can also pass a Column
display(spark.catalog.showTablesExtended(F.col("table_catalog") == "as_catalog"))
```