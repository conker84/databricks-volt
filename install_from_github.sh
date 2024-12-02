#!/bin/bash

# Function to download and move a JAR file
download_and_copy_jar() {
  local jar_name=$1
  local base_url="https://github.com/conker84/databricks-volt/releases/download/$VOLT_VERSION"

  echo "[VOLT] Copying file $jar_name to /databricks/jars"

  if wget "$base_url/$jar_name"; then
    if mv "$jar_name" /databricks/jars/; then
      echo "[VOLT] File $jar_name copied to /databricks/jars successfully."
    else
      echo "[VOLT] Error: Failed to move $jar_name to /databricks/jars."
    fi
  else
    echo "[VOLT] Error: Failed to download $jar_name from $base_url."
  fi
}

echo "[VOLT] Starting file downloads and copies to /databricks/jars"

echo -en "\nspark.sql.extensions com.databricks.volt.sql.SQLExtensions\n" >> /databricks/spark/conf/spark-defaults.conf
EOL

download_and_copy_jar "volt-$VOLT_VERSION.jar"
download_and_copy_jar "volt-aws-deps-$VOLT_VERSION.jar"
download_and_copy_jar "volt-azure-deps-$VOLT_VERSION.jar"

echo "[VOLT] All download attempts completed."