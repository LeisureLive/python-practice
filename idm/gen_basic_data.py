import json
import subprocess
import argparse
import sys
import time
import os
import requests
sys.path.append('../')
from idm.tools import common_tools

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

def install_requests(ip_list):
    for ip in ip_list:
        exec_command_with_root_and_check(ip,
                                         "/usr/bin/python3 -m pip install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple")
        exec_command_with_root_and_check(ip,
                                         "/usr/bin/python3 -m pip install requests -i https://pypi.tuna.tsinghua.edu.cn/simple")

def install_spark(ip, work_path, script_dir):
    exec_command_and_check(ip, f"mkdir -p {work_path}/{script_dir}")
    # 安装 pyspark
    exec_command_and_check(ip, "python3 -m pip install pyspark -i https://pypi.tuna.tsinghua.edu.cn/simple")
    # 扫描目录，安装 spark
    packages = exec_command_and_check(ip, f"ls {work_path}").strip().split("\n")
    if "dlc_spark3-1.0.0.2.tar" not in packages:
        exec_command_and_check(ip,
                     f"cd {work_path} && wget http://download.sensorsdata.cn/dragon/artifactory/dragon-release/com.sensorsdata.sps/dlc_spark3/dlc_spark3-1.0.0.2.tar")
    if "dlc_spark3" not in packages:
        exec_command_and_check(ip, f"cd {work_path} && tar -xf dlc_spark3-1.0.0.2.tar")


def send_code(ip, work_path, script_dir):
    script_bin_dir = os.path.dirname(os.path.dirname(os.path.abspath("__file__")))
    # 压缩，提交 spark 需要用
    subprocess.check_call(f"cd {script_bin_dir} && zip -r data_gen.zip data_gen/", shell=True)
    common_tools.cp_to(ip, f"{script_bin_dir}/data_gen.zip", f"{work_path}/data_gen.zip")
    for root, dirs, files in os.walk(f"{script_bin_dir}/{script_dir}"):
        for file in files:
            common_tools.cp_to(ip, f"{script_bin_dir}/{script_dir}/{file}", f"{work_path}/{script_dir}/{file}")

def start_spark_job(args, work_path, script_dir):
    # 创建数据目录
    exec_command_and_check(args.ip, f"hdfs dfs -mkdir -p {args.data_path}")
    # 检查是否有任务正在跑
    job_name_prefix = "gen_basic_data_spark_job"
    job_name = job_name_prefix+str(int(time.time() * 1000))
    kill_running_job(args.ip, job_name_prefix)
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
  --num-executors "{int(int(args.parallel)/2)}" \
  --executor-cores "2" \
  --py-files data_gen.zip \
  {script_dir}/gen_import_data.py \
  -user_count {args.user_count} \
  -login_percent {args.login_percent} \
  -cookie_percent {args.cookie_percent} \
  -mobile_percent {args.mobile_percent} \
  -idfv_percent {args.idfv_percent} \
  -data_path {args.data_path} \
  -event_login_count {args.event_login_count} \
  -event_mixed_count {args.event_mixed_count} >> gen_data.log 2>&1
'''
    exec_command_and_check(args.ip, spark_submit_cmd)
    if not check_job_status(args.ip, job_name):
        raise Exception(f"spark job run failed. [job_name={job_name}]")

def create_table(ip, data_path, script_path):
    path_expr = data_path.replace("/", "\\/")
    cmd = f'sed -i "s/@data_path/{path_expr}/g" {script_path}/create_table.sql'
    exec_command_and_check(ip, cmd)
    create_res = exec_command_and_check(ip, f"impala-shell -f {script_path}/create_table.sql")
    print(create_res)


def process(args):
    # 测试机工作目录
    work_path = "/home/sa_cluster/mock_data"
    # 脚本目录
    script_dir = "data_gen"
    result = "构造成功！"
    start_time = time.time()
    try:
        install_spark(args.ip, work_path, script_dir)
        # 把代码传到机器上
        send_code(args.ip, work_path, script_dir)
        # 开始造数据任务
        start_spark_job(args, work_path, script_dir)
        # 开始创建表
        create_table(args.ip, args.data_path, work_path+"/"+script_dir)
    except Exception as e:
        print(str(e))
        result = f"出现异常: {str(e)}"
    result += f"耗时: {int(time.time() - start_time)} 秒"
    # 推送结果
    push_result(args, result)

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
    msg = "【基础数据构造工具】"
    msg += "\n" + f"【tag: {args.tag}】"
    msg += "\n" + f"【ip: {args.ip}】"
    msg += "\n" + f"【并行度: {args.parallel}】"
    msg += "\n" + f"【importer 导入历史用户量: {args.user_count}】"
    msg += "\n" + f"【login 事件量: {args.event_login_count}】"
    msg += "\n" + f"【混合事件量: {args.event_mixed_count}】"
    msg += "\n" + f"【login 有值占比: {args.login_percent}】"
    msg += "\n" + f"【cookie 有值占比: {args.cookie_percent}】"
    msg += "\n" + f"【mobile 有值占比: {args.mobile_percent}】"
    msg += "\n" + f"【idfv(device_id) 有值占比: {args.idfv_percent}】"
    msg += "\n" + f"【数据存放目录: {args.data_path}】"
    msg += "\n" + f"【数据表: default.user_data / default.event_login_data / default.event_mixed_data】"
    msg += "\n" + "=========="
    msg += "\n" + result
    msg += "\n" + "=========="
    msg += "\n" + f"[构建地址]({args.build_url})"
    return msg

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-build_url', type=str, default="http://build_url", help='build_url')
    parser.add_argument('-build_user_id', type=str, default='hejie', help='build_user_id')
    parser.add_argument('-ip', type=str, default='127.0.0.1', help='ip')
    parser.add_argument('-webhook', type=str, default='', help='webhook')
    parser.add_argument('-parallel', type=str, default='24', help='启动的 executor 线程数，每个占 1C/1G')
    parser.add_argument('-tag', type=str, default='默认', help='自定义标记')

    # 生成数据的参数，会透传
    parser.add_argument('-user_count', type=int, default=22000000, help='importer 导入历史用户量')
    parser.add_argument('-login_percent', type=float, default=0.9, help='login 有值占比，会影响到流导入事件的占比')
    parser.add_argument('-cookie_percent', type=float, default=0.4, help='cookie 有值占比')
    parser.add_argument('-mobile_percent', type=float, default=0.5, help='mobile 有值占比')
    parser.add_argument('-idfv_percent', type=float, default=0.8, help='idfv(device_id) 有值占比')
    parser.add_argument('-data_path', type=str, default="hdfs:///sa/runtime/test", help='数据存放目录，三种数据名字先写死了')
    parser.add_argument('-event_login_count', type=int, default=200000000, help='login 事件量')
    parser.add_argument('-event_mixed_count', type=int, default=22000000, help='混合事件量')
    process(parser.parse_args())



