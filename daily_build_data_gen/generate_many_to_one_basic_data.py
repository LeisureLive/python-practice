import argparse
import sys

sys.path.append('..')
from pyspark.sql import SparkSession

from daily_build_data_gen.user_gen import UserGen

parser = argparse.ArgumentParser()
parser.add_argument('-id2_data_count', type=int, default=0)
parser.add_argument('-id3_data_count', type=int, default=0)
parser.add_argument('-data_path', type=str, default="hdfs:///sa/runtime/daily_benchmark_basic_data",
                    help='数据存放目录，需要先创建好')
parser.add_argument('-engine_type', type=str, default="default", help='数据集归属的引擎类型')

args = parser.parse_args()
id2_data_count = args.id2_data_count
id3_data_count = args.id3_data_count
data_path = args.data_path

spark = SparkSession.builder \
    .config("spark.default.parallelism", 1024) \
    .getOrCreate()

# id2数据集存储路径
data_path = data_path + "/many_to_one_basic_data"
# 多对一用户数据
user_generator = UserGen('id2', id2_data_count, 0.0)
spark.range(0, id2_data_count).rdd.flatMap(user_generator.gen_many_to_one_data) \
    .map(user_generator.convert_to_csv) \
    .toDF(user_generator.get_id3_spark_csv_schema()) \
    .write.mode("overwrite").parquet(data_path)

spark.stop()

