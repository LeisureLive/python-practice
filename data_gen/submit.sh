# 仅测试用
export HADOOP_CONF_DIR=$(aradmin config get global -n hadoop_conf_path -w literal)
SPARK_DIR="/home/sa_cluster/mock_data/dlc_spark3/spark-3.1.2-bin-hadoop3.2"
EXECUTOR_MEMORY="2G"
DRIVER_MEMORY="1G"
NUM_EXECUTORS="12"
EXECUTOR_CORES="2"
export PYSPARK_PYTHON=/usr/bin/python3

PROGRAM_ARGS=(
-user_count 2000
-event_login_count 20000
-event_mixed_count 2000
)

SPARK_SUBMIT_PATH="${SPARK_DIR}/bin/spark-submit"
${SPARK_SUBMIT_PATH} \
  --master yarn \
  --deploy-mode client \
  --executor-memory "${EXECUTOR_MEMORY}"  \
  --driver-memory "${DRIVER_MEMORY}" \
  --num-executors "${NUM_EXECUTORS}" \
  --executor-cores "${EXECUTOR_CORES}" \
  --py-files data_gen.zip \
  data_gen/gen_import_data.py \
  ${PROGRAM_ARGS[*]}