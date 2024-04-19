import argparse
import json
import sys

from pyspark.sql import SparkSession

from daily_build_data_gen.data_sender import DataSender

sys.path.append('..')

parser = argparse.ArgumentParser()
parser.add_argument('-idm_version', type=str, default="id2")
parser.add_argument('-target_ips', type=str, default="127.0.0.1")
parser.add_argument('-project', type=str, default="hj1")
parser.add_argument('-basic_data_path', type=str, default="")
parser.add_argument('-data_type', type=str, default="")

args = parser.parse_args()
idm_version = args.idm_version
basic_data_path = args.basic_data_path
data_type = args.data_type

ip_list = args.target_ips.split(",")
server_list = []
for ip in ip_list:
    server = "http://{}:8106/sa?project={}".format(ip, args.project)
    server_list.append(server)


def map_to_result(row):
    result = {}
    for key, value in row.asDict().items():
        if value is None:
            continue
        if key == "identities" or key == "properties":
            result[key] = json.loads(value)
        else:
            result[key] = value
    return result


spark = SparkSession.builder \
    .config("spark.default.parallelism", 1024) \
    .getOrCreate()

source = spark.read.parquet(basic_data_path)

selectExpr = ["type", "anonymous_id", "distinct_id", "properties"]
if data_type == "track" or data_type == "mixed":
    selectExpr.append("event")
    selectExpr.append("time")

if idm_version == "id2":
    selectExpr.append("login_id")
else:
    selectExpr.append("identities")

data_sender = DataSender(server_list)
source.selectExpr(selectExpr).rdd.map(map_to_result).foreachPartition(data_sender.batch_send_to_import_api)

spark.stop()
