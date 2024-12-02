#!/bin/bash

if [ -d "$VOLUME_PATH" ]; then
  echo "[VOLT] Volume path exists: $VOLUME_PATH"

  FILE_PATH="$VOLUME_PATH/volt-$VOLT_VERSION.jar"
  if [ -f "$FILE_PATH" ]; then
    echo "[VOLT] Copying file $FILE_PATH to /databricks/jars"
    cp "$FILE_PATH" /databricks/jars/
    echo "[VOLT] File $FILE_PATH copied to /databricks/jars"
  else
    echo "[VOLT] File does not exist: $FILE_PATH"
  fi

  AWS_FILE_PATH="$VOLUME_PATH/volt-aws-deps-$VOLT_VERSION.jar"
  if [ -f "$AWS_FILE_PATH" ]; then
    echo "[VOLT] Copying file $AWS_FILE_PATH to /databricks/jars"
    cp "$AWS_FILE_PATH" /databricks/jars/
    echo "[VOLT] File $AWS_FILE_PATH copied to /databricks/jars"
  else
    echo "[VOLT] File does not exist: $AWS_FILE_PATH"
  fi

  AZURE_FILE_PATH="$VOLUME_PATH/volt-azure-deps-$VOLT_VERSION.jar"
  if [ -f "$AZURE_FILE_PATH" ]; then
    echo "[VOLT] Copying file $AZURE_FILE_PATH to /databricks/jars"
    cp "$AZURE_FILE_PATH" /databricks/jars/
    echo "[VOLT] File $AZURE_FILE_PATH copied to /databricks/jars"
  else
    echo "[VOLT] File does not exist: $AZURE_FILE_PATH"
  fi
else
  echo "[VOLT] Volume path does not exist: $VOLUME_PATH"
fi