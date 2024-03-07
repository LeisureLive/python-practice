import argparse
import json

from pyspark.sql.types import StructType, StringType, LongType
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument('-id_mode', type=str, default="id2")
parser.add_argument('-data_type', type=str, default="profile_set")
parser.add_argument('-condition', type=str, default="过滤条件，SQL")
parser.add_argument('-data_path', type=str, default="file:///Users/xiaowancheng/test/parq/part-01018-47193ffe-038d-4366-988f-52c756f73812-c000.snappy.parquet", help='数据存放目录')
parser.add_argument('-output_path', type=str, default="./test", help='数据产出目录')
args = parser.parse_args()
def build_schema(data_type):
    schema = StructType()
    schema.add("id", LongType())
    schema.add("anonymous_id", StringType())
    schema.add("login_id", StringType())
    schema.add("properties", StringType())
    schema.add("identities", StringType())
    if data_type == "track":
        schema.add("event", StringType())
        schema.add("time", LongType())
    return schema

spark = SparkSession.builder\
    .config("spark.default.parallelism", 1024)\
    .getOrCreate()

source = spark.read.parquet(args.data_path)
# 导入 login_id 必须有值
selectExpr = ["login_id as distinct_id", "properties"]

if args.data_type == "track":
    selectExpr.append("'track' as type")
    selectExpr.append("event")
    selectExpr.append("time")
else:
    selectExpr.append("'profile_set' as type")

if args.id_mode == "id2":
    # selectExpr.append("anonymous_id")
    selectExpr.append("login_id")
else:
    selectExpr.append("identities")

def map_to_result(row):
    result = {}
    for key, value in row.asDict().items():
        if value is None:
            continue
        if key == "identities" or key == "properties":
            result[key] = json.loads(value)
        else:
            result[key] = value
    return json.dumps(result, ensure_ascii=False)


source = source.selectExpr(selectExpr)

if args.condition is not None and len(args.condition) > 0:
    source = source.filter(args.condition)

source.rdd.map(map_to_result).toDF(StringType()).write.mode("overwrite").text(args.output_path)


