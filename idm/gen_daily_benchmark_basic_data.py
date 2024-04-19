import argparse
import json
import sys
import time

import requests

sys.path.append('../')
from idm.tools import common_tools
from idm.tools.common_tools import get_ips_from_hosts
from idm.tools.spark_job import install_requests, install_spark, send_code


def exec_command_and_check(ip, cmd):
    return common_tools.exec_command_and_check(ip, f"su - sa_cluster -c '{cmd}'")


def exec_command_with_root_and_check(ip, cmd):
    return common_tools.exec_command_and_check(ip, f"{cmd}")


def exec_command(ip, cmd):
    return common_tools.exec_command(ip, f"su - sa_cluster -c '{cmd}'")


def kill_running_job(ip, job_name_prefix):
    running_app_ids = exec_command(ip, f"yarn app -list 2>/dev/null|grep {job_name_prefix}").strip()
    for app in running_app_ids.strip().split("\n"):
        app_id = app.split("	")[0].strip()
        if app_id is not None and len(app_id) > 0:
            print(f"find running app. [app={app}]")
            exec_command_and_check(ip, f"yarn app -kill {app_id}")


# 多检查一次
def check_job_status(ip, job_name):
    ret = exec_command(ip, f"yarn app -list -appStates ALL 2>/dev/null|grep {job_name}")
    print(f"find job result. [ret={ret}]")
    if "SUCCEEDED" in ret:
        return True
    return False


def start_spark_job(args, work_path, script_dir):
    # 创建数据目录
    exec_command_and_check(args.target_ip, f"hdfs dfs -mkdir -p {args.data_path}")
    # 检查是否有任务正在跑
    job_name_prefix = "gen_daily_benchmark_basic_data"
    job_name = job_name_prefix + str(int(time.time() * 1000))
    kill_running_job(args.target_ip, job_name_prefix)
    # 产出数据
    spark_submit_cmd = f'''
export HADOOP_CONF_DIR=$(aradmin config get global -n hadoop_conf_path -w literal) && \
export PYSPARK_PYTHON=/usr/bin/python3 && \
cd {work_path} && \
{work_path}/dlc_spark3/spark-3.1.2-bin-hadoop3.2/bin/spark-submit \
  --name {job_name} \
  --master yarn \
  --deploy-mode client \
  --executor-memory "2G"  \
  --driver-memory "1G" \
  --num-executors "{int(int(args.parallel) / 2)}" \
  --executor-cores "2" \
  --py-files daily_build_data_gen.zip \
  {script_dir}/generate_benchmark_basic_data.py \
  -id2_data_count {args.id2_data_count} \
  -id3_data_count {args.id3_data_count} \
  -engine_type {args.engine_type} \
  -data_path {args.data_path}  >> gen_data.log 2>&1
'''
    exec_command_and_check(args.target_ip, spark_submit_cmd)
    if not check_job_status(args.target_ip, job_name):
        raise Exception(f"spark job run failed. [job_name={job_name}]")


def push_result(args, result):
    header = {'content-type': 'application/json'}
    data = {}
    markdown_dict = {}
    markdown_dict.update({"content": f"{build_common_msg(args, result)}"})
    markdown_dict.update({"mentioned_list": ["{}".format(args.build_user_id)]})
    data.update({"msgtype": 'markdown', "markdown": markdown_dict})
    wx_request = requests.post(args.webhook, bytes(json.dumps(data), 'utf-8'), headers=header)
    result = wx_request.json()
    print(result)


def build_common_msg(args, result):
    msg = "【daily-benchmark 基础数据构造工具】"
    msg += "\n" + f"【tag: {args.tag}】"
    msg += "\n" + f"【ip: {args.target_ip}】"
    msg += "\n" + f"【并行度: {args.parallel}】"
    msg += "\n" + f"【id2 场景数据量: {args.id2_data_count}】"
    msg += "\n" + f"【id3 场景数据量: {args.id3_data_count}】"
    msg += "\n" + f"【数据存放目录: {args.data_path}】"
    msg += "\n" + "=========="
    msg += "\n" + result
    msg += "\n" + "=========="
    msg += "\n" + f"[构建地址]({args.build_url})"
    return msg


def process(args):
    ip = args.target_ip
    ip_list = get_ips_from_hosts(ip)
    print("ip_list = %s" % ip_list)
    start_time = time.time()
    result = "构造成功！"
    try:
        # 1、初始化 spark 运行环境
        work_path = "/home/sa_cluster/import_data_benchmark"
        script_dir = "daily_build_data_gen"
        install_requests(ip_list)
        install_spark(ip, work_path, script_dir)
        send_code(ip, work_path, script_dir)

        # 2、开始造数据任务
        start_spark_job(args, work_path, script_dir)
    except Exception as e:
        print(str(e))
        result = f"出现异常: {str(e)}"
    result += f"耗时: {int(time.time() - start_time)} 秒"
    # 推送结果
    push_result(args, result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-build_url', type=str, default="http://build_url", help='build_url')
    parser.add_argument('-build_user_id', type=str, default='hejie', help='build_user_id')
    parser.add_argument('-target_ip', type=str, default='10.129.23.220', help='ip')
    parser.add_argument('-webhook', type=str, default='', help='webhook')
    parser.add_argument('-parallel', type=str, default='24', help='启动的 executor 线程数，每个占 1C/1G')
    parser.add_argument('-tag', type=str, default='默认', help='自定义标记')

    # 生成数据的参数，会透传
    parser.add_argument('-engine_type', type=str, default="fast_mode", help='数据集供哪种引擎使用')
    parser.add_argument('-id2_data_count', type=int, default=3000000, help='id2 场景下各数据集生成的数据量')
    parser.add_argument('-id3_data_count', type=int, default=1500000, help='id3 场景下各数据集生成的数据量')
    parser.add_argument('-data_path', type=str, default="hdfs:///sa/runtime/daily_benchmark_basic_data", help='数据存放目录')
    process(parser.parse_args())
