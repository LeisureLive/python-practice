import os
import subprocess

from idm.tools.common_tools import exec_command_and_check, exec_command, cp_to


def install_spark(ip, work_path, script_dir):
    exec_command_and_check(ip, f"su - sa_cluster -c 'mkdir -p {work_path}/{script_dir}' ")
    # 安装 pyspark
    exec_command_and_check(ip,
                           "su - sa_cluster -c 'python3 -m pip install pyspark -i https://pypi.tuna.tsinghua.edu.cn/simple' ")
    # 扫描目录，安装 spark
    packages = exec_command_and_check(ip, f"ls {work_path}").strip().split("\n")
    if "dlc_spark3-1.0.0.2.tar" not in packages:
        exec_command_and_check(ip,
                               f"su - sa_cluster -c 'cd {work_path} && wget http://download.sensorsdata.cn/dragon/artifactory/dragon-release/com.sensorsdata.sps/dlc_spark3/dlc_spark3-1.0.0.2.tar' ")
    if "dlc_spark3" not in packages:
        exec_command_and_check(ip, f"su - sa_cluster -c 'cd {work_path} && tar -xf dlc_spark3-1.0.0.2.tar' ")


def install_requests(ip_list):
    for ip in ip_list:
        exec_command_and_check(ip,
                               "/usr/bin/python3 -m pip install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple")
        exec_command_and_check(ip,
                               "/usr/bin/python3 -m pip install requests -i https://pypi.tuna.tsinghua.edu.cn/simple")


def send_code(ip, work_path, script_dir):
    exec_command_and_check(ip, f"su - sa_cluster -c 'mkdir -p {work_path}/{script_dir}' ")
    script_bin_dir = os.path.dirname(os.path.dirname(os.path.abspath("__file__")))
    # 压缩，提交 spark 需要用
    subprocess.check_call(f"cd {script_bin_dir} && zip -r daily_build_data_gen.zip daily_build_data_gen/", shell=True)
    cp_to(ip, f"{script_bin_dir}/daily_build_data_gen.zip", f"{work_path}/daily_build_data_gen.zip")
    for root, dirs, files in os.walk(f"{script_bin_dir}/{script_dir}"):
        for file in files:
            cp_to(ip, f"{script_bin_dir}/{script_dir}/{file}", f"{work_path}/{script_dir}/{file}")


def start_spark_job(exec_ip, work_path, script_path, job_name, idm_version, ips, project, user_count, event_count,
                    login_percent, new_user_percent, input_file_dir, output_file_dir):
    hdfs_file_dir = "hdfs:///sa/runtime/import_data_daily_benchmark"
    input_file_dir = hdfs_file_dir + "/" + input_file_dir
    output_file_dir = hdfs_file_dir + "/" + output_file_dir
    # 创建数据目录
    exec_command_and_check(exec_ip, f"su - sa_cluster -c 'hdfs dfs -mkdir -p {hdfs_file_dir}' ")
    kill_running_job(exec_ip, job_name)
    spark_submit_cmd = f''' su - sa_cluster -c ' 
    export HADOOP_CONF_DIR=$(aradmin config get global -n hadoop_conf_path -w literal) && \
    export PYSPARK_PYTHON=/usr/bin/python3 && \
    cd {work_path} && \
    {work_path}/dlc_spark3/spark-3.1.2-bin-hadoop3.2/bin/spark-submit \
      --name {job_name} \
      --master yarn \
      --deploy-mode client \
      --executor-memory "2G"  \
      --driver-memory "1G" \
      --num-executors "10" \
      --executor-cores "2" \
      --py-files daily_build_data_gen.zip \
      {script_path}/generate_import_data.py \
      -idm_version {idm_version} \
      -ips "{ips}" \
      -project "{project}" \
      -user_count {user_count} \
      -event_count {event_count} \
      -login_percent {login_percent} \
      -new_user_percent {new_user_percent} \
      -input_file_path {input_file_dir} \
      -output_file_path {output_file_dir} \
       >> gen_data.log 2>&1 '
    '''
    exec_command_and_check(exec_ip, spark_submit_cmd)
    if not check_job_status(exec_ip, job_name):
        raise Exception(f"spark job run failed. [job_name={job_name}]")
    return output_file_dir


def kill_running_job(ip, job_name_prefix):
    running_app_ids = \
        exec_command(ip, f"su - sa_cluster -c 'sudo yarn app -list 2>/dev/null|grep {job_name_prefix}' ").strip()
    for app in running_app_ids.strip().split("\n"):
        app_id = app.split("	")[0].strip()
        if app_id is not None and len(app_id) > 0:
            print(f"find running app. [app={app}]")
            exec_command_and_check(ip, f"yarn app -kill {app_id}")


def check_job_status(ip, job_name):
    ret = exec_command(ip, f"su - sa_cluster -c 'yarn app -list -appStates ALL 2>/dev/null|grep {job_name}' ")
    print(f"find job result. [ret={ret}]")
    if "SUCCEEDED" in ret:
        return True
    return False
