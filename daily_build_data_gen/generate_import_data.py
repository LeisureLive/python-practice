import argparse
import random
import sys

from pyspark import StorageLevel
from pyspark.sql import SparkSession

from daily_build_data_gen.data_sender import DataSender
from daily_build_data_gen.profile_set_generator import ProfileSetGenerator
from daily_build_data_gen.track_generator import TrackGenerator

sys.path.append('..')

parser = argparse.ArgumentParser()
parser.add_argument('-idm_version', type=str, default="id2")
parser.add_argument('-ips', type=str, default="127.0.0.1")
parser.add_argument('-project', type=str, default="hj1")
parser.add_argument('-user_count', type=int, default=0)
parser.add_argument('-event_count', type=int, default=0)
parser.add_argument('-login_percent', type=float, default=0.0)
parser.add_argument('-new_user_percent', type=float, default=0.0)
parser.add_argument('-device_id_list_size', type=int, default=1)
parser.add_argument('-input_file_path', type=str, default="")
parser.add_argument('-output_file_path', type=str, default="")

args = parser.parse_args()
idm_version = args.idm_version
user_count = args.user_count
event_count = args.event_count
login_percent = args.login_percent
new_user_percent = args.new_user_percent
device_id_list_size = args.device_id_list_size
input_file_path = args.input_file_path
output_file_path = args.output_file_path

login_user_max_id = int(int(user_count) * login_percent)
login_new_user_max_id = int(login_user_max_id * new_user_percent)
not_login_new_user_max_id = login_user_max_id + int((user_count - login_user_max_id) * new_user_percent)

ip_list = args.ips.split(",")
server_list = []
for ip in ip_list:
    server = "http://{}:8106/sa?project={}".format(ip, args.project)
    server_list.append(server)

spark = SparkSession.builder \
    .config("spark.default.parallelism", 1024) \
    .getOrCreate()

exist_identities = []
if input_file_path != "hdfs:///sa/runtime/import_data_daily_benchmark/":
    exist_identities = spark.read.json(input_file_path).rdd.collect()

profile_set_generator = \
    ProfileSetGenerator(idm_version, user_count, login_percent, new_user_percent, device_id_list_size, exist_identities)
track_generator = \
    TrackGenerator(idm_version, event_count, login_percent, new_user_percent, device_id_list_size, exist_identities)
data_sender = DataSender(server_list)

user_rdd = spark.range(0, user_count).rdd \
    .map(profile_set_generator.gen_data) \
    .persist(StorageLevel.DISK_ONLY)
event_rdd = spark.range(0, event_count).rdd \
    .map(track_generator.gen_data) \
    .persist(StorageLevel.DISK_ONLY)

if user_rdd.isEmpty() is False and event_rdd.isEmpty() is False:
    # 用户和事件都有数据时, 混合乱序后再发送
    user_rdd.union(event_rdd).map(lambda x: (x, random.random())).sortBy(lambda x: x[1]).map(lambda x: x[0]) \
        .foreachPartition(data_sender.batch_send_to_import_api)
elif user_rdd.isEmpty() is False:
    user_rdd.foreachPartition(data_sender.batch_send_to_import_api)
else:
    event_rdd.foreachPartition(data_sender.batch_send_to_import_api)

if output_file_path != "hdfs:///sa/runtime/import_data_daily_benchmark/":
    if user_rdd.isEmpty() is False:
        user_rdd \
            .map(profile_set_generator.filter_identity_info) \
            .toDF() \
            .write.mode("overwrite").json(output_file_path)
    elif event_rdd.isEmpty() is False:
        event_rdd \
            .map(track_generator.filter_identity_info) \
            .toDF() \
            .write.mode("overwrite").json(output_file_path)

spark.stop()
