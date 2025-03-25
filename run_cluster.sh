#!/bin/bash
# Check if the script is called properly
if [ -z "$driver" ]; then
    # Can use this for both output and console: driver=GLS/drivers/pr_pl94_2020.py nohup bash run_cluster.sh 2>&1 | tee out_run_gls.log &
    echo "usage example: driver=GLS/drivers/pr_pl94_2020.py nohup bash run_cluster.sh 2>&1 1> out_run_gls.log &"
    exit
fi

# Activate venv
SCRIPT_DIR="$(dirname "$(realpath "$0")")"
VENV="gls-venv"
VENV_DIR="${SCRIPT_DIR}/${VENV}"
echo "Activating venv: ${VENV_DIR}"
. ${VENV_DIR}/bin/activate

# Fetch NODES
NODES=`yarn node -list 2>/dev/null | grep RUNNING | wc -l`
echo
echo AVAILABLE NODES: $NODES
echo

# Set Spark Settings
export PYTHONPATH=$(pwd)
export PYSPARK_PYTHON="${VENV_DIR}/bin/python3"
export PYSPARK_DRIVER_PYTHON="${VENV_DIR}/bin/python3"
export PATH="${VENV_DIR}/bin:$PATH"
export PYTHON="${VENV_DIR}/bin/python3"
export PIP="${VENV_DIR}/bin/pip3"
echo "Settings:"
echo "PYTHONPATH: $PYTHONPATH"
echo "PYSPARK_PYTHON: $PYSPARK_PYTHON"
echo "PYSPARK_DRIVER_PYTHON: $PYSPARK_DRIVER_PYTHON"
echo "PATH: $PATH"
echo "PYTHON: $PYTHON"
echo "PIP: $PIP"

# Run driver
ZIP_FILE="GLS.zip"
spark-submit \
    --driver-memory 40g --num-executors $NODES --executor-memory 110g --executor-cores 10 --driver-cores 20 \
    --conf spark.driver.maxResultSize=0g --conf spark.executor.memoryOverhead=90g --conf spark.sql.shuffle.partitions=2400 \
    --conf spark.dynamicAllocation.enabled=false --conf spark.network.timeout=30000s --conf spark.storage.blockManagerSlaveTimeoutMs=3600000 \
    --conf spark.executor.heartbeatInterval=30s --conf spark.scheduler.listenerbus.eventqueue.capacity=50000 --conf spark.sql.execution.arrow.pyspark.enabled=true \
    --conf spark.submit.deployMode=client --conf spark.task.maxFailures=24 --conf spark.rdd.compress=true --conf spark.io.compression.codec=zstd \
    --conf spark.pyspark.python="${PYSPARK_PYTHON}" --conf spark.pyspark.driver.python="${PYSPARK_DRIVER_PYTHON}" \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON="${PYSPARK_PYTHON}" \
    --py-files $ZIP_FILE \
    --jars ./GLS/jars/bin2la_spark_udf_2.12-0.1.0.jar $driver

# Deactivate venv
deactivate
