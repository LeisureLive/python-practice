import argparse
import base64
import gzip
import json
import random
import urllib
from functools import reduce

import pyspark.sql.functions as fns
import requests
from pyspark.sql import SparkSession, DataFrame

parser = argparse.ArgumentParser()
parser.add_argument('-idm_version', type=str, default="id2")
parser.add_argument('-ips', type=str, default="127.0.0.1")
parser.add_argument('-project', type=str, default="hj1")
parser.add_argument('-login_user_count', type=int, default=0)
parser.add_argument('-not_login_user_count', type=int, default=0)
parser.add_argument('-login_event_count', type=int, default=0)
parser.add_argument('-not_login_event_count', type=int, default=0)
parser.add_argument('-storage_user_data_total_count', type=int, default=0)
parser.add_argument('-storage_user_data_login_count', type=int, default=0)
parser.add_argument('-storage_event_data_total_count', type=int, default=0)
parser.add_argument('-storage_event_data_login_count', type=int, default=0)
parser.add_argument('-user_data_parquet_path', type=str,
                    default="file:///Users/xiaowancheng/test/parq/part-01018-47193ffe-038d-4366-988f-52c756f73812-c000.snappy.parquet")
parser.add_argument('-event_data_parquet_path', type=str,
                    default="file:///Users/xiaowancheng/test/parq/part-01018-47193ffe-038d-4366-988f-52c756f73812-c000.snappy.parquet")
args = parser.parse_args()
ip_list = args.ips.split(",")
print(f"ips = {args.ips}")
server_list = []
for ip in ip_list:
    server = "http://{}:8106/sa?project={}".format(ip, args.project)
    server_list.append(server)


def map_to_json(row):
    result = {}
    for key, value in row.asDict().items():
        if value is None:
            continue
        if key == "identities" or key == "properties":
            result[key] = json.loads(value)
        else:
            result[key] = value
    if result.get("login_id") is not None:
        result["distinct_id"] = result.get("login_id")
        result["identities"]["$identity_login_id"] = result.get("login_id")
    else:
        result["distinct_id"] = result.get("anonymous_id")
    return result


def batch_send_to_import_api(datas):
    batch_size = 100
    batch = []
    for item in datas:
        batch.append(item)
        if len(batch) == batch_size:
            # 攒满一批进行上报
            count = import_api(1, 1, batch, server_list[random.randint(0, len(server_list) - 1)])
            yield count
            batch = []
    # 处理剩余的
    if batch:
        count = import_api(1, 1, batch, server_list[random.randint(0, len(server_list) - 1)])
        yield count


def import_api(gzipType, dataType, jsonString, server):
    Udata = dealwith(gzipType, jsonString)
    if gzipType == 1 and dataType == 1:
        payload = "gzip=1&data_list=%s" % Udata
    elif gzipType == 1 and dataType == 0:
        payload = "gzip=1&data=%s" % Udata
    elif gzipType == 0 and dataType == 1:
        payload = "data_list=%s" % Udata
    elif gzipType == 0 and dataType == 0:
        payload = "data=%s" % Udata
    else:
        print("非法参数！！")
    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'Connection': "close"
    }
    s = requests.session()
    s.keep_alive = False
    response = requests.post(server, data=payload, headers=headers)
    return len(jsonString)


def dealwith(gzipType, jsonString):
    data = json.dumps(jsonString, ensure_ascii=False)
    # print("json: "+data)
    data = data.encode('utf-8')
    if gzipType == 1:
        # gzip压缩
        data = gzip.compress(data)
    Bdata = base64.b64encode(data)
    Udata = urllib.parse.quote(Bdata)
    return Udata


spark = SparkSession.builder \
    .config("spark.default.parallelism", 1024) \
    .getOrCreate()

random_seed = random.randint(1, 100)

user_source = spark.read.parquet(args.user_data_parquet_path)
if args.idm_version == 'id2':
    userDataSelectExpr = ['anonymous_id', 'login_id', 'properties', "'profile_set' as type"]
else:
    userDataSelectExpr = ['anonymous_id', 'login_id', 'properties', 'identities', "'profile_set' as type"]

login_user_source = user_source.selectExpr(userDataSelectExpr) \
    .withColumn("event", fns.lit(None)) \
    .withColumn("time", fns.lit(None)) \
    .withColumn("random_number", fns.rand(random_seed))
login_user_max_id = args.storage_user_data_login_count
login_user_start_num = 0
login_user_end_num = min(login_user_start_num + args.login_user_count, login_user_max_id)
login_user_source_filter = f"login_id IS NOT NULL AND id > {login_user_start_num} AND id <= {login_user_end_num}"
print(f"login_user_source_filter = {login_user_source_filter}")
login_user_source = login_user_source.filter(login_user_source_filter)

not_login_user_source = user_source.selectExpr(userDataSelectExpr) \
    .withColumn("event", fns.lit(None)) \
    .withColumn("time", fns.lit(None)) \
    .withColumn("random_number", fns.rand(random_seed))
not_login_user_max_id = args.storage_user_data_total_count
not_login_user_start_num = login_user_max_id
not_login_user_end_num = min(not_login_user_start_num + args.not_login_user_count, not_login_user_max_id)
not_login_user_source_filter = f"login_id is NULL AND id > {not_login_user_start_num} AND id <= {not_login_user_end_num}"
print(f"not_login_user_source_filter = {not_login_user_source_filter}")
not_login_user_source = not_login_user_source.filter(not_login_user_source_filter)

event_source = spark.read.parquet(args.event_data_parquet_path)
if args.idm_version == 'id2':
    eventDataSelectExpr = ['anonymous_id', 'login_id', 'properties', "'track' as type", "event", "time"]
else:
    eventDataSelectExpr = ['anonymous_id', 'login_id', 'properties', 'identities', "'track' as type", "event", "time"]

login_event_source = event_source.selectExpr(eventDataSelectExpr).withColumn("random_number", fns.rand(random_seed))
login_event_max_id = args.storage_event_data_login_count
login_event_start_num = 0
login_event_end_num = min(login_event_max_id, login_event_start_num + args.login_event_count)
login_event_source_filter = f"login_id IS NOT NULL AND id > {login_event_start_num} AND id <= {login_event_end_num}"
print(f"login_event_source_filter = {login_event_source_filter}")
login_event_source = login_event_source.filter(login_event_source_filter)

not_login_event_source = event_source.selectExpr(eventDataSelectExpr).withColumn("random_number", fns.rand(random_seed))
not_login_event_max_id = args.storage_event_data_total_count
not_login_event_start_num = login_event_max_id
not_login_event_end_num = min(not_login_event_max_id, not_login_event_start_num + args.not_login_event_count)
not_login_event_source_filter = f"login_id is NULL AND id > {not_login_event_start_num} AND id <= {not_login_event_end_num} "
print(f"not_login_event_source_filter = {not_login_event_source_filter}")
not_login_event_source = not_login_event_source.filter(not_login_event_source_filter)

sources = [login_user_source, not_login_user_source, login_event_source, not_login_event_source]
source = reduce(DataFrame.unionAll, sources)
source = source.orderBy("random_number")

total_count = source.drop("random_number").rdd.map(map_to_json).mapPartitions(batch_send_to_import_api).count()
print(f"import data total count={total_count}")
