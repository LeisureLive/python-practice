import time

from pyspark import StorageLevel
from pyspark.sql import SparkSession

import argparse
import sys

sys.path.append('..')
from data_gen.event_gen import EventGen
from data_gen.user_gen import UserGen
'''
https://doc.sensorsdata.cn/pages/viewpage.action?pageId=482183983

手动执行脚本：
mkdir /home/sa_cluster/mock_data
cd /home/sa_cluster/mock_data
wget http://download.sensorsdata.cn/dragon/artifactory/dragon-release/com.sensorsdata.sps/dlc_spark3/dlc_spark3-1.0.0.2.tar
tar -xf dlc_spark3-1.0.0.2.tar
python3 -m pip install pyspark -i https://mirrors.aliyun.com/pypi/simple/ 
zip -r data_gen.zip data_gen/ && scp -r data_gen sa_cluster@10.129.24.15:/home/sa_cluster/mock_data && scp data_gen.zip sa_cluster@10.129.24.15:/home/sa_cluster/mock_data
hdfs dfs -mkdir -p /sa/runtime/test
sh submit.sh
单机要起 yarn：
mothershipadmin start -m yarn
'''


parser = argparse.ArgumentParser()
parser.add_argument('-user_count', type=int, default=22000000, help='importer 导入历史用户量')
parser.add_argument('-login_percent', type=float, default=0.9, help='login 有值占比，会影响到流导入事件的占比')
parser.add_argument('-cookie_percent', type=float, default=0.4, help='cookie 有值占比')
parser.add_argument('-mobile_percent', type=float, default=0.5, help='mobile 有值占比')
parser.add_argument('-idfv_percent', type=float, default=0.8, help='idfv(device_id) 有值占比')
parser.add_argument('-data_path', type=str, default="hdfs:///sa/runtime/test", help='数据存放目录，需要先创建好')

parser.add_argument('-event_login_count', type=int, default=200000000, help='login 事件量')
parser.add_argument('-event_mixed_count', type=int, default=22000000, help='混合事件量')

args = parser.parse_args()
user_count = args.user_count
login_percent = args.login_percent
cookie_percent = args.cookie_percent
mobile_percent = args.mobile_percent
idfv_percent = args.idfv_percent

event_login_count = args.event_login_count
event_mixed_count = args.event_mixed_count

data_path = args.data_path
user_data = data_path + "/" + "user_data"
event_login_data = data_path + "/" + "event_login_data"
event_mixed_data = data_path + "/" + "event_mixed_data"

spark = SparkSession.builder\
    .config("spark.default.parallelism", 1024)\
    .getOrCreate()

user_generator = UserGen(user_count, login_percent, cookie_percent, mobile_percent, idfv_percent)
# 转化出的用户，暂存在磁盘，identities/properties 为 json
user_rdd = spark.range(0, user_count).rdd.map(user_generator.gen_data).persist(StorageLevel.DISK_ONLY)
# 数据落盘
user_rdd\
    .map(user_generator.convert_to_csv)\
    .toDF(user_generator.get_id3_spark_csv_schema())\
    .write.mode("overwrite").parquet(user_data)

# 转化出 login_id 有值的用户，生成事件
login_user_count = int(user_count * login_percent)
event_login_gen = EventGen(login_user_count, event_login_count)
user_rdd\
    .filter(lambda row: row.get('login_id') is not None and len(row.get('login_id')) > 0)\
    .flatMap(event_login_gen.gen_data)\
    .map(event_login_gen.convert_to_csv)\
    .toDF(event_login_gen.get_id3_spark_csv_schema())\
    .write.mode("overwrite").parquet(event_login_data)

# 随机取用户，生成事件
event_mixed_gen = EventGen(user_count, event_mixed_count)
user_rdd\
    .flatMap(event_mixed_gen.gen_data) \
    .map(event_mixed_gen.convert_to_csv) \
    .toDF(event_mixed_gen.get_id3_spark_csv_schema())\
    .write.mode("overwrite").parquet(event_mixed_data)

