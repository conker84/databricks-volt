#!/bin/bash

VOLUME_PATH="/Volumes/as_catalog/default/jars"

if [ -d "$VOLUME_PATH" ]; then
  echo "Volume path exists: $VOLUME_PATH"
  
  # List the contents of the volume
  echo "Listing contents of the volume:"
  ls -l "$VOLUME_PATH"
  
  # Example: Read a specific file from the volume
  FILE_PATH="$VOLUME_PATH/extensions-1.0.0-SNAPSHOT-jar-with-dependencies.jar"
  if [ -f "$FILE_PATH" ]; then
    echo "Copying file $FILE_PATH to /databricks/jars"
    cp $FILE_PATH /databricks/jars/
  else
    echo "File does not exist: $FILE_PATH"
  fi
else
  echo "Volume path does not exist: $VOLUME_PATH"
fi

# cp /FileStore/andrea.santurbano/extensions_1_0_0_SNAPSHOT_jar_with_dependencies.jar /databricks/jars/

# cp /Volumes/as_catalog/default/jars/extensions-1.0.0-SNAPSHOT-jar-with-dependencies.jar /databricks/jars/ d