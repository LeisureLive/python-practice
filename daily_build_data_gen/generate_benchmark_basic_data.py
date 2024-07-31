import argparse
import random
import sys

sys.path.append('..')
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from daily_build_data_gen.event_gen import EventGen

from daily_build_data_gen.user_gen import UserGen

parser = argparse.ArgumentParser()
parser.add_argument('-id2_data_count', type=int, default=0)
parser.add_argument('-id3_data_count', type=int, default=0)
parser.add_argument('-data_path', type=str, default="hdfs:///sa/runtime/daily_benchmark_basic_data",
                    help='数据存放目录，需要先创建好')

args = parser.parse_args()
id2_data_count = args.id2_data_count
id3_data_count = args.id3_data_count
data_path = args.data_path

spark = SparkSession.builder \
    .config("spark.default.parallelism", 1024) \
    .getOrCreate()

# 打乱 RDD 中的元素顺序
def randomize_partition(iterator):
    return iter(sorted(iterator, key=lambda _: random.random()))

# id2数据集存储路径
id2_anonymous_new_user_data = data_path + '/id2' + '/anonymous_new_profile_set'
id2_anonymous_old_user_data = data_path + '/id2' + '/anonymous_old_profile_set'
id2_login_new_user_data = data_path + '/id2' + '/login_new_profile_set'
id2_login_old_user_data = data_path + '/id2' + '/login_old_profile_set'
id2_anonymous_new_event_data = data_path + '/id2' + '/anonymous_new_track'
id2_anonymous_old_event_data = data_path + '/id2' + '/anonymous_old_track'
id2_anonymous_mixed_data = data_path + '/id2' + '/anonymous_new_profile_set_track_mix'
id2_anonymous_bind_login_event_data = data_path + '/id2' + '/anonymous_bind_login_track'
id2_login_user_anonymous_event_data = data_path + '/id2' + '/login_user_anonymous_track'

# 匿名新用户数据
user_generator = UserGen('id2', id2_data_count, 0.0)
id2_anonymous_new_user_rdd = spark.range(0, id2_data_count).rdd.map(user_generator.gen_data)\
    .persist(StorageLevel.DISK_ONLY)
id2_anonymous_new_user_rdd \
    .map(user_generator.convert_to_csv) \
    .toDF(user_generator.get_id3_spark_csv_schema()) \
    .write.mode("overwrite").parquet(id2_anonymous_new_user_data)
# 匿名老用户数据(3倍)
id2_anonymous_new_user_rdd.union(id2_anonymous_new_user_rdd).union(id2_anonymous_new_user_rdd)\
    .map(user_generator.gen_old_data)\
    .map(user_generator.convert_to_csv) \
    .toDF(user_generator.get_id3_spark_csv_schema()) \
    .write.mode("overwrite").parquet(id2_anonymous_old_user_data)

# 登录新用户数据
user_generator = UserGen('id2', id2_data_count, 1.0)
id2_login_new_user_rdd = spark.range(0, id2_data_count).rdd.map(user_generator.gen_data).persist(StorageLevel.DISK_ONLY)
id2_login_new_user_rdd \
    .map(user_generator.convert_to_csv) \
    .toDF(user_generator.get_id3_spark_csv_schema()) \
    .write.mode("overwrite").parquet(id2_login_new_user_data)
# 登录老用户数据(3倍)
id2_login_new_user_rdd.union(id2_login_new_user_rdd).union(id2_login_new_user_rdd)\
    .map(user_generator.gen_old_data) \
    .map(user_generator.convert_to_csv) \
    .toDF(user_generator.get_id3_spark_csv_schema()) \
    .write.mode("overwrite").parquet(id2_login_old_user_data)
# 匿名新用户事件
event_generator = EventGen('id2', id2_data_count, 0.0)
id2_anonymous_new_event_rdd = spark.range(0, id2_data_count).rdd.map(event_generator.gen_data) \
    .persist(StorageLevel.DISK_ONLY)
id2_anonymous_new_event_rdd \
    .map(event_generator.convert_to_csv) \
    .toDF(event_generator.get_id3_spark_csv_schema()) \
    .write.mode("overwrite").parquet(id2_anonymous_new_event_data)
# 匿名老用户事件
id2_anonymous_new_event_rdd.union(id2_anonymous_new_event_rdd).union(id2_anonymous_new_event_rdd)\
    .map(event_generator.gen_old_data) \
    .map(event_generator.convert_to_csv) \
    .toDF(event_generator.get_id3_spark_csv_schema()) \
    .write.mode("overwrite").parquet(id2_anonymous_old_event_data)
# 新:老 = 1:50  profile_set:track=1:20
mixed_new_user_count = int(id2_data_count * 3 * 0.05 * 0.02)
mixed_new_event_count = int(id2_data_count * 3 * 0.95 * 0.02)
mixed_old_user_count = int(id2_data_count * 0.05 * 0.98)
mixed_old_event_count = int(id2_data_count * 0.95 * 0.98)
new_user_generator = UserGen('id2', mixed_new_user_count, 0.0)
new_event_generator = EventGen('id2', mixed_new_event_count, 0.0)
spark.range(0, mixed_new_user_count).rdd.map(new_user_generator.gen_data) \
    .union(spark.range(0, mixed_new_event_count).rdd.map(new_event_generator.gen_data)) \
    .union(id2_anonymous_new_user_rdd.union(id2_anonymous_new_user_rdd).union(id2_anonymous_new_user_rdd).filter(lambda x: x['id'] < mixed_old_user_count).map(user_generator.gen_old_data)) \
    .union(id2_anonymous_new_event_rdd.union(id2_anonymous_new_event_rdd).union(id2_anonymous_new_event_rdd).filter(lambda x: x['id'] < mixed_old_event_count).map(event_generator.gen_old_data)) \
    .mapPartitions(randomize_partition)\
    .map(event_generator.convert_to_csv) \
    .toDF(event_generator.get_id3_spark_csv_schema()) \
    .write.mode("overwrite").parquet(id2_anonymous_mixed_data)

# 已存在匿名用户绑定同一个登录id
event_generator = EventGen('id2', id2_data_count, 1.0)
id2_anonymous_new_event_rdd \
    .map(lambda row: event_generator.gen_bind_login_data(row, 2)) \
    .map(event_generator.convert_to_csv) \
    .toDF(event_generator.get_id3_spark_csv_schema()) \
    .write.mode("overwrite").parquet(id2_anonymous_bind_login_event_data)
# 多对一登录用户匿名事件
id2_anonymous_new_event_rdd.union(id2_anonymous_new_event_rdd).union(id2_anonymous_new_event_rdd) \
    .map(event_generator.gen_old_data) \
    .map(event_generator.convert_to_csv) \
    .toDF(event_generator.get_id3_spark_csv_schema()) \
    .write.mode("overwrite").parquet(id2_login_user_anonymous_event_data)

# id3数据集存储路径
id3_anonymous_new_user_data = data_path + '/id3' + '/anonymous_new_profile_set'
id3_anonymous_old_user_data = data_path + '/id3' + '/anonymous_old_profile_set'
id3_anonymous_new_event_data = data_path + '/id3' + '/anonymous_new_track'
id3_anonymous_old_event_data = data_path + '/id3' + '/anonymous_old_track'
id3_anonymous_mixed_data = data_path + '/id3' + '/anonymous_new_profile_set_track_mix'

# 匿名新用户数据
user_generator = UserGen('id3', id3_data_count, 0.0)
id3_anonymous_new_user_rdd = id2_anonymous_new_user_rdd \
    .filter(lambda row: row['id'] < id3_data_count) \
    .map(user_generator.append_identities) \
    .persist(StorageLevel.DISK_ONLY)
id3_anonymous_new_user_rdd \
    .map(user_generator.convert_to_csv) \
    .toDF(user_generator.get_id3_spark_csv_schema()) \
    .write.mode("overwrite").parquet(id3_anonymous_new_user_data)
# 匿名老用户数据
id3_anonymous_new_user_rdd.union(id3_anonymous_new_user_rdd).union(id3_anonymous_new_user_rdd)\
    .map(user_generator.gen_old_data) \
    .map(user_generator.convert_to_csv) \
    .toDF(user_generator.get_id3_spark_csv_schema()) \
    .write.mode("overwrite").parquet(id3_anonymous_old_user_data)

# 匿名新用户事件
event_generator = EventGen('id3', id3_data_count, 0.0)
id3_anonymous_new_event_rdd = id2_anonymous_new_event_rdd \
    .filter(lambda row: row['id'] < id3_data_count) \
    .map(event_generator.append_identities) \
    .persist(StorageLevel.DISK_ONLY)
id3_anonymous_new_event_rdd \
    .map(event_generator.convert_to_csv) \
    .toDF(event_generator.get_id3_spark_csv_schema()) \
    .write.mode("overwrite").parquet(id3_anonymous_new_event_data)
# 匿名老用户事件
id3_anonymous_new_event_rdd.union(id3_anonymous_new_event_rdd).union(id3_anonymous_new_event_rdd) \
    .map(event_generator.gen_old_data) \
    .map(event_generator.convert_to_csv) \
    .toDF(event_generator.get_id3_spark_csv_schema()) \
    .write.mode("overwrite").parquet(id3_anonymous_old_event_data)
# 混合数据 新:老 = 1:50  profile_set:track=1:20
id3_mixed_new_user_count = int(id3_data_count * 3 * 0.05 * 0.02)
id3_mixed_new_event_count = int(id3_data_count * 3 * 0.95 * 0.02)
id3_mixed_old_user_count = int(id3_data_count * 0.05 * 0.98)
id3_mixed_old_event_count = int(id3_data_count * 0.95 * 0.98)
user_generator = UserGen('id3', id3_mixed_new_user_count, 0.0)
event_generator = EventGen('id3', id3_mixed_new_event_count, 0.0)
spark.range(0, id3_mixed_new_user_count).rdd.map(user_generator.gen_data)\
    .union(spark.range(0, id3_mixed_new_event_count).rdd.map(event_generator.gen_data))\
    .union(id3_anonymous_new_user_rdd.union(id3_anonymous_new_user_rdd).union(id3_anonymous_new_user_rdd).filter(lambda x: x['id'] < id3_mixed_old_user_count).map(user_generator.gen_old_data))\
    .union(id3_anonymous_new_event_rdd.union(id3_anonymous_new_event_rdd).union(id3_anonymous_new_event_rdd).filter(lambda x: x['id'] < id3_mixed_old_event_count).map(event_generator.gen_old_data))\
    .mapPartitions(randomize_partition)\
    .map(event_generator.convert_to_csv) \
    .toDF(event_generator.get_id3_spark_csv_schema()) \
    .write.mode("overwrite").parquet(id3_anonymous_mixed_data)

spark.stop()
