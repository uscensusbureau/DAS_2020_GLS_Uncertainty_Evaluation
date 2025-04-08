#!/bin/bash
# Declare some constants:
SPARKS_DEFAULTS_CONF_FILE="/usr/lib/spark/conf/spark-defaults.conf"
SCRIPT_DIR="$(dirname "$(realpath "$0")")"
REQUIREMENTS_FILE="requirements.txt"
VENV="gls-venv"

# Remove DAS-Specific Spark-Defaults set by the CFT + subsequent bootstraps:
echo "Removing DAS-Specific Spark-Defaults..."
grep -v -E "^(spark.executorEnv.LD_LIBRARY_PATH|spark.pyspark.python|spark.pyspark.driver.python|spark.archives)" "$SPARKS_DEFAULTS_CONF_FILE" > temp.conf && mv -f temp.conf "$SPARKS_DEFAULTS_CONF_FILE"

# Remove any existing venvs
echo "Removing any existing venvs (if any)..."
rm -rf $SCRIPT_DIR/$VENV
rm $SCRIPT_DIR/$VENV.tgz

# Zip up module
echo "Creating .zip file of the repo files."
ZIP_FILE="GLS.zip"
rm -f $ZIP_FILE
zip -r $ZIP_FILE .

# Create a venv
echo "Creating a venv 'gls-venv' and installing requirements..."
python3.11 -m venv $SCRIPT_DIR/$VENV
. $SCRIPT_DIR/$VENV/bin/activate
pip3 install --verbose -r "$SCRIPT_DIR/$REQUIREMENTS_FILE"
pip3 install venv-pack==0.2.0

# Pack the venv and close
echo "Packing the venv up..."
venv-pack -o $SCRIPT_DIR/$VENV.tgz

# End
echo "Finished setting up environment."
which python3
deactivate
