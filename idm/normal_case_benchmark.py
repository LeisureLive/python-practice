import argparse
import json
import subprocess
import time
import uuid
import os
import requests
import sys

sys.path.append('../')
from idm import idm_benchmark, gen_basic_data
from idm.tools import common_tools


def exec_command_and_check(ip, cmd):
    return common_tools.exec_command_and_check(ip, f"su - sa_cluster -c '{cmd}'")


def exec_command(ip, cmd):
    return common_tools.exec_command(ip, f"su - sa_cluster -c '{cmd}'")


def process(param):
    # 测试机工作目录
    work_path = "/home/sa_cluster/mock_data"
    script_dir = "data_gen"
    skip_init = param.skip_init == "true"
    exec_ip = param.ip
    project = param.project
    env_version = common_tools.get_env_version(exec_ip)
    try:
        # 1、对 skv 内存进行调优
        idm_benchmark.optimize_skv(exec_ip, skip_init)
        # 2、尝试开启 idm 的优化开关, 非特定版本可能会出现开启失败情况
        idm_benchmark.open_idm_optimize_trigger(exec_ip, env_version, skip_init)
        # 3、新建项目
        idm_benchmark.create_new_project(exec_ip, env_version, project, param.id_mode, param.idm_engine_type, skip_init)
        # 4、生成数据
        gen_basic_data.install_spark(exec_ip, work_path, script_dir)
        gen_basic_data.send_code(exec_ip, work_path, script_dir)
        profile_set_file = start_spark_job(param, work_path, script_dir, "profile_set")
        track_file = start_spark_job(param, work_path, script_dir, "track")
        # profile_set_file = gen_data_file(param.ip, param.id_mode, "profile_set", "where login_id is not null")
        # track_file = gen_data_file(param.ip, param.id_mode, "track", "")
        # 5、开始导入
        profile_set_result = exec_importer(param.ip, param.project, profile_set_file)
        track_result = exec_importer(param.ip, param.project, track_file)
        result = build_result(profile_set_result, track_result)
    except Exception as e:
        print(str(e))
        result = f"出现异常: {str(e)}"
    # 6、输出数据
    push_result(param, result)


def start_spark_job(args, work_path, script_dir, data_type):
    output_path = f"/sa/runtime/normal_case_benchmark_temp/{data_type}"
    if data_type == "profile_set":
        condition = "login_id is not null"
        data_path = f"{args.data_path}/user_data"
    else:
        condition = ""
        data_path = f"{args.data_path}/event_login_data"

    # 创建数据目录
    exec_command_and_check(args.ip, f"hdfs dfs -mkdir -p {output_path}")
    # 检查是否有任务正在跑
    job_name_prefix = "convert_basic_data_spark_job"
    job_name = job_name_prefix + str(int(time.time() * 1000))
    gen_basic_data.kill_running_job(args.ip, job_name_prefix)
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
  --num-executors "{int(int(args.data_prepare_parallel) / 2)}" \
  --executor-cores "2" \
  --py-files data_gen.zip \
  {script_dir}/json_converter.py \
  -id_mode {args.id_mode} \
  -data_type {data_type} \
  -condition "{condition}" \
  -data_path {data_path} \
  -output_path {output_path} \
   >> gen_data.log 2>&1
'''
    exec_command_and_check(args.ip, spark_submit_cmd)
    if not gen_basic_data.check_job_status(args.ip, job_name):
        raise Exception(f"spark job run failed. [job_name={job_name}]")
    return output_path


def build_result(profile_set_result, track_result):
    result = ""
    result += "\n" + "===== profile_set_result ====="
    result += "\n" + f"【数据总量: {profile_set_result['all_read_count']}】"
    result += "\n" + f"【入库数量: {profile_set_result['all_import_count']}】"
    result += "\n" + f"【无效数量: {profile_set_result['all_skipped_count']}】"
    result += "\n" + f"【QPS: {int(profile_set_result['all_read_count']) / int(profile_set_result['cost'])}】"
    result += "\n" + "===== track_result ====="
    result += "\n" + f"【数据总量: {track_result['all_read_count']}】"
    result += "\n" + f"【入库数量: {track_result['all_import_count']}】"
    result += "\n" + f"【无效数量: {track_result['all_skipped_count']}】"
    result += "\n" + f"【QPS: {int(track_result['all_read_count']) / int(track_result['cost'])}】"
    result += "\n" + "=========="
    return result


def push_result(args, result):
    if len(result) > 30000:
        result = result[:30000] + "......"

    header = {'content-type': 'application/json'}
    data = {}
    markdown_dict = {}
    markdown_dict.update({"content": f"{build_common_msg(args, result)}"})
    markdown_dict.update({"mentioned_list": ["{}".format(args.build_user_id)]})
    data.update({"msgtype": 'markdown', "markdown": markdown_dict})
    json_data = json.dumps(data)
    print(json_data)
    wx_request = requests.post(args.webhook, bytes(json_data, 'utf-8'), headers=header)
    result = wx_request.json()
    print(result)


def build_common_msg(args, result):
    msg = "【数据接入场景测试-批导入】"
    msg += "\n" + f"【tag: {args.tag}】"
    msg += "\n" + f"【ip: {args.ip}】"
    msg += "\n" + f"【id_mode: {args.id_mode}】"
    msg += "\n" + f"【idm_engine_type: {args.idm_engine_type}】"
    msg += "\n" + result
    msg += "\n" + f"[构建地址]({args.build_url})"
    return msg


def exec_importer(ip, project, hdfs_path):
    temp_name = str(uuid.uuid4())
    cluster = common_tools.check_is_cluster(ip)
    if not cluster:
        raise Exception("not support.")
    else:
        cmd = f"integratoradmin importer run --project {project} --path hdfs://{hdfs_path} --parallelism 3 --yjm 2048 --ytm 4096 --job_name {temp_name}"
    start_time = time.time()
    exec_command_and_check(ip, cmd)
    cost = time.time() - start_time
    check_cmd = f'''metadb_cli -usc_dba -D metadata --skip-column-names <<< "select counter_info from integrator_importer_record where job_name=\\"{temp_name}\\""'''
    check_result = exec_command(ip, check_cmd)
    if check_result.strip() == "NULL":
        raise Exception(f"import job run failed. [job_name={temp_name}]")
    json_result = json.loads(check_result)
    json_result['cost'] = cost
    return json_result


def gen_data_file(ip, id_mode, data_type, condition):
    hdfs_file_path = f"/sa/runtime/normal_case_benchmark_temp/{data_type}"
    exec_command_and_check(ip, f"hdfs dfs -mkdir -p {hdfs_file_path}")
    exec_command_and_check(ip, f"hdfs dfs -rm -r -f -skipTrash {hdfs_file_path}/*")
    sql = '''create external table normal_case_benchmark_temp 
    stored as textfile 
    location '{hdfs_file_path}' 
    as select concat(
    '{"type":"{data_type}",', 
    {event_data}
    '"distinct_id":"', login_id, '",', 
    {identities_values}
     '"properties":', properties, 
     '}')
      from {table_name} {condition}'''
    sql = sql.replace("{data_type}", data_type)
    sql = sql.replace("{hdfs_file_path}", hdfs_file_path)
    sql = sql.replace("{condition}", condition)
    if data_type == "track":
        sql = sql.replace("{event_data}", ''''"event":"',event,'",',
    '"time":',cast(time as string),',',''')
        sql = sql.replace("{table_name}", "event_login_data")
    else:
        sql = sql.replace("{event_data}", "")
        sql = sql.replace("{table_name}", "user_data")

    if id_mode == "id2":
        sql = sql.replace("{identities_values}", ''''"anonymous_id":"', anonymous_id, '",',
        '"login_id":"', login_id, '",',''')
    else:
        sql = sql.replace("{identities_values}", ''''"identities":', identities, ',',''')

    temp_table_name = "normal_case_benchmark_temp"
    sql_list = f"drop table if exists {temp_table_name};\n"
    sql_list += f"{sql};\n"
    sql_list += f"drop table {temp_table_name};"
    exec_sql(ip, sql_list)
    exec_command(ip, f"hdfs dfs -rm -r -f -skipTrash {hdfs_file_path}/_impala_insert_staging")
    print(f"gen_profile_set_file. [ip={ip}, path={hdfs_file_path}]")
    return hdfs_file_path


def exec_sql(ip, sql):
    sql_file = gen_temp_sql_file(ip, sql)
    cmd = f"impala-shell -d default -f {sql_file}"
    exec_command_and_check(ip, cmd)


def gen_temp_sql_file(ip, sql):
    temp_path = "/tmp/normal_case_benchmark_temp.sql"
    with open(temp_path, "w") as f:
        f.write(sql)
    common_tools.cp_to(ip, temp_path, temp_path)
    return temp_path


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-build_url', type=str, default="http://build_url", help='build_url')
    parser.add_argument('-build_user_id', type=str, default='hejie', help='build_user_id')
    parser.add_argument('-ip', type=str, default='127.0.0.1', help='ip')
    parser.add_argument('-webhook', type=str, default='', help='webhook')
    parser.add_argument('-tag', type=str, default='默认', help='自定义标记')
    parser.add_argument('-skip_init', type=str, default="false", help='跳过开关、项目初始化')
    parser.add_argument('-project', type=str, default='xwc_test', help='导入项目名')
    parser.add_argument('-id_mode', type=str, default='id2', help='id2 / id3')
    parser.add_argument('-idm_engine_type', type=str, default='default', help='default/fast_mode')

    parser.add_argument('-data_path', type=str, default='hdfs:///sa/runtime/test', help='数据存放目录，三种数据名字先写死了')
    parser.add_argument('-data_prepare_parallel', type=str, default='24', help='准备数据时，启动的 executor 线程数，每个占 1C/1G')
    process(parser.parse_args())
