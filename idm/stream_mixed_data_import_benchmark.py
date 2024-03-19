import argparse
import json
import sys
import time

import requests

sys.path.append('../')
from idm import idm_benchmark, gen_basic_data
from idm.tools import common_tools


def exec_command_and_check(ip, cmd):
    return common_tools.exec_command_and_check(ip, f"su - sa_cluster -c '{cmd}'")


def exec_command(ip, cmd):
    return common_tools.exec_command(ip, f"su - sa_cluster -c '{cmd}'")


def process(args):
    # 测试机工作目录
    work_path = "/home/sa_cluster/hj"
    script_dir = "data_gen"
    skip_init = args.skip_init == "true"
    data_storage_ip = args.data_storage_ip
    data_storage_ip_list = common_tools.get_ips_from_hosts(data_storage_ip)
    target_ip = args.target_ip
    target_ip_list = common_tools.get_ips_from_hosts(target_ip)
    target_ips = ",".join(target_ip_list)
    target_env_version = common_tools.get_env_version(target_ip)

    project = args.project
    total_count = args.login_user_count + args.not_login_user_count + args.login_event_count + args.not_login_event_count

    try:
        # 1. 在数据存储所在集群安装 pyspark 及相关依赖包
        gen_basic_data.install_requests(data_storage_ip_list)
        gen_basic_data.install_spark(data_storage_ip, work_path, script_dir)
        gen_basic_data.send_code(data_storage_ip, work_path, script_dir)
        # 2. 对被压测集群进行调优, 创建项目
        idm_benchmark.optimize_skv(target_ip, skip_init)
        idm_benchmark.open_idm_optimize_trigger(target_ip, target_env_version, skip_init)
        idm_benchmark.create_new_project(target_ip, target_env_version, project, args.id_mode, args.idm_engine_type,
                                         skip_init)
        exec_command_and_check(target_ip, "skvadmin balance start -m skv_offline")
        exec_command_and_check(target_ip,
                               "sbpadmin business_config set -p integrator -n scheduler -k max_before_deviation_hour_cluster -v 24000 --unstable")
        # 3. 导入数据
        common_tools.pause_import_and_wait_consume_latency(target_ip, target_env_version)
        common_tools.start_import_and_pause_handler(target_ip, target_env_version)
        start_time = int(time.time())
        start_spark_job(args, work_path, script_dir, target_ips)
        end_time = int(time.time())
        total_cost = end_time - start_time
        # 4. 收集结果
        common_tools.start_handler(target_ip, target_env_version)
        if target_env_version == 'new':
            qps = common_tools.collect_sdi_qps(target_ip, total_count)
        else:
            qps = common_tools.collect_extractor_qps(target_ip, total_count)
        result = build_result(args, target_ip_list, target_env_version, total_count, total_cost, qps)
    except Exception as e:
        print(str(e))
        result = f"出现异常: {str(e)}"
    # 6、输出数据
    push_result(args, result)


def build_result(args, target_ip_list, target_env_version, total_count, total_cost, qps):
    node_num = len(target_ip_list)
    result = ""
    result += "\n" + "【数据接入场景测试-流导入】"
    result += "\n" + f"【环境 IP: {target_ip_list}】"
    if target_env_version == 'new':
        result += "\n" + f"【环境类型: 新架构】"
        sdi_version = common_tools.get_sdi_version(target_ip_list[0])
        horizon_version = common_tools.get_horizon_version(target_ip_list[0])
        result += "\n" + f"【SDI 版本: {sdi_version}】"
        result += "\n" + f"【Horizon 版本: {horizon_version}】"
    else:
        result += "\n" + f"【环境类型: 老架构】"
        sdf_version = common_tools.get_sdf_version(target_ip_list[0])
        result += "\n" + f"【SDF 版本: {sdf_version}】"
    result += "\n" + f"【测试项目名: {args.project}】"
    result += "\n" + f"【id_mode: {args.id_mode}】"
    result += "\n" + f"【idm_engine_type: {args.idm_engine_type}】"
    result += "\n" + f"【tag: {args.tag}】"
    result += "\n" + f"【上报数据总量: {total_count}】"
    result += "\n" + f"【登录用户 profile_set 数据总量: {args.login_user_count}】"
    result += "\n" + f"【匿名用户 profile_set 数据总量: {args.not_login_user_count}】"
    result += "\n" + f"【登录用户 track 数据总量: {args.login_event_count}】"
    result += "\n" + f"【匿名用户 track 数据总量: {args.not_login_event_count}】"
    result += "\n" + f"【上报总耗时: {total_cost}s】"
    result += "\n" + f"【上报 QPS: {int(total_count / total_cost)}】"
    result += "\n" + f"【平均处理 QPS: {int(qps['avg_qps']) * node_num}】"
    result += "\n" + f"【最小处理 QPS: {int(qps['min_qps']) * node_num}】"
    result += "\n" + f"【最大处理 QPS: {int(qps['max_qps']) * node_num}】"
    result += "\n" + "=========="
    return result


def start_spark_job(args, work_path, script_dir, target_ips):
    job_name_prefix = 'stream_data_send'
    job_name = job_name_prefix + str(int(time.time() * 1000))
    gen_basic_data.kill_running_job(args.data_storage_ip, job_name_prefix)
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
      --num-executors "{int(int(args.data_prepare_parallel) / 2)}" \
      --executor-cores "2" \
      --py-files data_gen.zip \
      {script_dir}/stream_data_send.py \
      -idm_version {args.id_mode} \
      -ips {target_ips} \
      -project {args.project} \
      -login_user_count {args.login_user_count} \
      -not_login_user_count {args.not_login_user_count} \
      -login_event_count {args.login_event_count} \
      -not_login_event_count {args.not_login_event_count} \
      -user_data_parquet_path {args.user_data_path} \
      -event_data_parquet_path {args.event_data_path} \
      -storage_user_data_total_count {args.storage_user_data_total_count} \
      -storage_user_data_login_count {args.storage_user_data_login_count} \
      -storage_event_data_total_count {args.storage_event_data_total_count} \
      -storage_event_data_login_count {args.storage_event_data_login_count} \
       >> stream_data_send.log 2>&1
    '''
    exec_command_and_check(args.data_storage_ip, spark_submit_cmd)
    if not gen_basic_data.check_job_status(args.data_storage_ip, job_name):
        raise Exception(f"spark job run failed. [job_name={job_name}]")


def push_result(args, result):
    if len(result) > 30000:
        result = result[:30000] + "......"

    header = {'content-type': 'application/json'}
    data = {}
    markdown_dict = {}
    markdown_dict.update({"content": f"{result}"})
    markdown_dict.update({"mentioned_list": ["{}".format(args.build_user_id)]})
    data.update({"msgtype": 'markdown', "markdown": markdown_dict})
    json_data = json.dumps(data)
    print(json_data)
    wx_request = requests.post(args.webhook, bytes(json_data, 'utf-8'), headers=header)
    result = wx_request.json()
    print(result)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-build_url', type=str, default="http://build_url", help='build_url')
    parser.add_argument('-build_user_id', type=str, default='hejie', help='build_user_id')
    parser.add_argument('-target_ip', type=str, default='10.129.24.159', help='被压测集群的ip')
    parser.add_argument('-webhook', type=str,
                        default='https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=3a443a3b-e44c-4051-b3e0-3ed2084d7945',
                        help='webhook')
    parser.add_argument('-tag', type=str, default='老架构id2混合流导入', help='自定义标记')
    parser.add_argument('-skip_init', type=str, default="true", help='跳过开关、项目初始化')
    parser.add_argument('-project', type=str, default='import_data_test', help='导入项目名')
    parser.add_argument('-id_mode', type=str, default='id2', help='id2 / id3')
    parser.add_argument('-idm_engine_type', type=str, default='default', help='default/fast_mode')
    parser.add_argument('-login_user_count', type=int, default=200000, help='login user count')
    parser.add_argument('-not_login_user_count', type=int, default=0, help='not login user count')
    parser.add_argument('-login_event_count', type=int, default=7200000, help='login event count')
    parser.add_argument('-not_login_event_count', type=int, default=800000, help='not login event count')
    parser.add_argument('-data_storage_ip', type=str, default='10.129.25.11', help='数据存储所在集群ip')
    parser.add_argument('-user_data_path', type=str, default='hdfs:///sa/runtime/test/user_data', help='用户数据存放目录')
    parser.add_argument('-storage_user_data_total_count', type=int, default=22000000,
                        help='storage user_data total count')
    parser.add_argument('-storage_user_data_login_count', type=int, default=19800000,
                        help='storage login user_data  count')
    parser.add_argument('-storage_event_data_total_count', type=int, default=22000000,
                        help='storage event_data total count')
    parser.add_argument('-storage_event_data_login_count', type=int, default=19800000,
                        help='storage login event_data  count')
    parser.add_argument('-event_data_path', type=str, default='hdfs:///sa/runtime/test/event_mixed_data',
                        help='事件数据存放目录')
    parser.add_argument('-data_prepare_parallel', type=str, default='24', help='准备数据时，启动的 executor 线程数，每个占 1C/1G')
    process(parser.parse_args())
