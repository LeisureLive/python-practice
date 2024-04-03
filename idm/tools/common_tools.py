import base64
import datetime
import gzip
import json
import os
import re
import time
import urllib

import paramiko
import requests

false = False
true = True
username = 'root'


def get_pwd():
    url = 'http://security.sensorsdata.cn/qa_auth'
    header = {
        'x-qa-tools': 'Oj4QluoQ6ZT6VzACWY9Bqr5gzVFPFhoUPJIqzmZTRxc'
    }
    resp = requests.get(url, headers=header, params=None)
    if resp.status_code == 404:
        print("get pwd error, url = %s, status_code = %s, resp_content = %s" % (url, resp.status_code, resp.text))
        return "uXkmsrTHQp#c8EbZ"
    elif resp.status_code != 200:
        # raise RuntimeError('get %s error %s, %s' % (url, resp.status_code, resp.text))
        print("get pwd error, url = %s, status_code = %s, resp_content = %s" % (url, resp.status_code, resp.text))
        # print(resp.text)
        return "uXkmsrTHQp#c8EbZ"
    return resp.text


def get_env_version(ip):
    horizon_version = get_horizon_version(ip)
    if horizon_version != 'unknown':
        return 'new'
    else:
        return 'old'


def get_sdi_version(ip):
    versions = exec_command(ip, "su - sa_cluster -c 'aradmin version' ")
    matches = re.findall(r"│\s+integrator\s+│\s+(\d+\.\d+\.\d+\.\d+)\s+│\s+(\w+)\s+│", versions)

    if matches:
        version, level = matches[0]
        print(f"SDI Version: {version}")
        print(f"SDI Level: {level}")
        return version + ' ' + level
    else:
        print("未找到匹配的信息")
        return 'unknown'


def get_horizon_version(ip):
    versions = exec_command(ip, "su - sa_cluster -c 'aradmin version' ")
    matches = re.findall(r"│\s+horizon\s+│\s+(\d+\.\d+\.\d+\.\d+)\s+│\s+(\w+)\s+│", versions)

    if matches:
        version, level = matches[0]
        print(f"HORIZON Version: {version}")
        print(f"HORIZON Level: {level}")
        return version + ' ' + level
    else:
        print("未找到匹配的信息")
        return 'unknown'


def get_sdf_version(ip):
    versions = exec_command(ip, "su - sa_cluster -c 'aradmin version' ")
    matches = re.findall(r"│\s+sdf\s+│\s+(\d+\.\d+\.\d+\.\d+)\s+│\s+(\w+)\s+│", versions)

    if matches:
        version, level = matches[0]
        print(f"SDF Version: {version}")
        print(f"SDF Level: {level}")
        return version + ' ' + level
    else:
        print("未找到匹配的信息")
        return 'unknown'


def optimize_skv(ip, skip_init):
    if skip_init:
        return
    result = exec_command(ip, 'su - sa_cluster -c "skvadmin health -m skv_offline"')
    if result.__contains__("expected val is"):
        print("skv 内存需要调优")
        role_config_group = ""
        cache_capacity_size = ""
        write_buffer_size = ""
        match_role_config_group = re.search(r'role_config_group_replica_server_\d+', result)
        match_cache_capacity = re.search(r'rocksdb_block_cache_capacity for [^ ]+ expected val is (\d+),', result)
        match_write_buffer = re.search(r'rocksdb_total_size_across_write_buffer for [^ ]+ expected val is (\d+),',
                                       result)
        if match_role_config_group:
            role_config_group = match_role_config_group.group(0)
        if match_cache_capacity:
            cache_capacity_size = match_cache_capacity.group(1)
        if match_write_buffer:
            write_buffer_size = match_write_buffer.group(1)

        if cache_capacity_size != "":
            exec_command(ip,
                         ' su - sa_cluster -c \'mothershipadmin role_config_group config set -m skv_offline --namespace replica_server.ini -r replica_server --role_config_group {} -k "pegasus.server|rocksdb_block_cache_capacity" -v {} --yes\' '
                         .format(role_config_group, cache_capacity_size))
        if write_buffer_size is not None:
            exec_command(ip,
                         ' su - sa_cluster -c \'mothershipadmin role_config_group config set -m skv_offline --namespace replica_server.ini -r replica_server --role_config_group {} -k "pegasus.server|rocksdb_total_size_across_write_buffer" -v {} --yes\' '
                         .format(role_config_group, write_buffer_size))
        exec_command(ip, 'su - sa_cluster -c "mothershipadmin restart -m skv_offline"')
    else:
        print("skv 内存不需要调优")


# 创建项目
def create_new_project(ip, env_version, project_name, idm_mode, idm_engine_type, skip_init):
    if skip_init:
        return
    if env_version == 'new':
        exec_command(ip,
                     'su - sa_cluster -c "sbpadmin project create -c {} -n {} --disable-schema-limited"'
                     .format(project_name, project_name))
        time.sleep(5)
        if idm_mode == 'id2':
            if idm_engine_type == 'default':
                exec_command(ip,
                             'su - sa_cluster -c "horizonadmin identity_tool change_version -p {} -t open_multi_signup"'
                             .format(project_name))
            elif idm_engine_type == 'fast_mode':
                exec_command(ip,
                             'su - sa_cluster -c "horizonadmin identity_tool change_version -p {} -t open_fast_mode"'
                             .format(project_name))
        elif idm_mode == 'id3':
            exec_command(ip,
                         'su - sa_cluster -c "horizonadmin identity_tool change_version -p {} -t open_id_mapping_v3"'
                         .format(project_name))
            if idm_engine_type == 'fast_mode':
                exec_command(ip,
                             'su - sa_cluster -c "horizonadmin identity_tool change_version -p {} -t open_fast_mode"'
                             .format(project_name))
    else:
        exec_command(ip,
                     'su - sa_cluster -c "sbpadmin project create -c {} -n {} --disable-schema-limited"'
                     .format(project_name, project_name))
        time.sleep(5)
        if idm_mode == 'id2':
            # 开启多对一
            exec_command(ip,
                         'su - sa_cluster -c "sbpadmin project update -n {} --enable-new-signup"'
                         .format(project_name, project_name))
        elif idm_mode == 'id3':
            exec_command(ip,
                         'su - sa_cluster -c "sdfadmin enable_id_mapping_v3 change_to_v3 -p {} -r "'
                         .format(project_name))


def open_idm_optimize_trigger(ip, env_version, skip_init):
    if skip_init:
        return
    if env_version == 'new':
        open_idm_optimize_trigger_in_new_env(ip)
    else:
        open_idm_optimize_trigger_in_old_env(ip)


def open_idm_optimize_trigger_in_new_env(ip):
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k id_mapping_is_open_direct_skv -v true --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k id_mapping_engine_open_concurrent -v true --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k id_mapping_direct_skv_thread_pool_size -v 10 --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p horizon -n identity_skv_proxy -k enable_read_async -v true --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p horizon -n identity_skv_proxy -k enable_write_async -v true --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "aradmin config set server -p horizon -m identity_skv_proxy -n mem_mb -v 4096" ')
    exec_command(ip,
                 'su - sa_cluster -c \'aradmin ss set -p horizon -m identity_skv_proxy -r identity_skv_proxy -n mem_limit -v "4096Mi" \' ')
    exec_command(ip,
                 'su - sa_cluster -c \'aradmin ss set -p horizon -m identity_skv_proxy -r identity_skv_proxy -n jvm_xmx -v "4096Mi" \' ')
    exec_command(ip,
                 'su - sa_cluster -c "aradmin config set server -p edge -m edge -n mem_mb -v 1024 " ')
    if check_is_cluster(ip):
        exec_command(ip,
                     'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k id_mapping_batch_process_pack_max_size -v 2000 --unstable" ')
        exec_command(ip,
                     'su - sa_cluster -c "aradmin config set server -m scheduler -p integrator -n job_manager_tm_mem_mb -v 8192" ')
    else:
        # 单机环境需要调整 scheduler 进程的内存大小
        exec_command(ip,
                     'su - sa_cluster -c "aradmin config set server -m scheduler -p integrator -n mem_mb -v 4096" ')

    restart_module(ip, "edge", "edge")
    restart_module(ip, "horizon", "identity_skv_proxy")
    restart_module(ip, "integrator", "scheduler")


def open_idm_optimize_trigger_in_old_env(ip):
    exec_command(ip,
                 'su - sa_cluster -c "aradmin ss set -p sdf -m extractor -r extractor -n mem_limit -v "8192Mi"" ')
    exec_command(ip,
                 'su - sa_cluster -c "aradmin ss set -p sdf -m extractor -r extractor -n jvm_xmx -v "8192Mi"" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p sdf -n extractor -k id_mapping_batch_process_pack_max_size -v 2000 --unstable" ')

    exec_command(ip,
                 'su - sa_cluster -c "aradmin ss set -p sdf -m id_mapping_skv_proxy -r id_mapping_skv_proxy -n mem_limit -v "4096Mi"" ')
    exec_command(ip,
                 'su - sa_cluster -c "aradmin ss set -p sdf -m id_mapping_skv_proxy -r id_mapping_skv_proxy -n jvm_xmx -v "4096Mi"" ')

    exec_command(ip,
                 'su - sa_cluster -c "aradmin config set server -p sdf -m id_mapping_skv_proxy -n mem_mb -v 4096" ')

    restart_module(ip, "sdf", "id_mapping_skv_proxy")
    restart_module(ip, "sdf", "extractor")


def close_mock_idm(ip):
    exec_command(ip,
                 "su - sa_cluster -c 'sbpadmin business_config set -p integrator -n scheduler -k id_mapping_is_open_mock -v false --unstable ' ")


def open_idm_mock(ip):
    exec_command(ip,
                 "su - sa_cluster -c 'sbpadmin business_config set -p integrator -n scheduler -k id_mapping_is_open_mock -v true --unstable ' ")


def exec_command_and_check(ip, cmd):
    print(
        f'exec_command_and_check. [ip={ip}, cmd={cmd}, start_time={datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}]')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=ip, username=username, password=get_pwd(), timeout=120)
    stdin, stdout, stderr = ssh.exec_command(cmd)
    out = stdout.read()
    err = stderr.read()
    return_code = stdout.channel.recv_exit_status()
    if return_code != 0:
        raise RuntimeError(f"exec_command error: {err.decode()}. cmd: {cmd}")
    result = out
    if not result:
        result = stderr.read()
    print(result.decode())
    return result.decode()


def exec_command(ip, cmd, withstderr=False):
    print(f'exec_command. [ip={ip}, cmd={cmd}, start_time={datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}]')
    retries = 1
    ssh = paramiko.SSHClient()
    while retries <= 3:
        try:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname=ip, username=username, password=get_pwd(), timeout=120)
            stdin, stdout, stderr = ssh.exec_command(cmd)
            result = stdout.read()
            if withstderr:
                result += stderr.read()
            if not result:
                result = stderr.read()
            print(result.decode())
            return result.decode()
        except paramiko.SSHException as e:
            print(f"SSH connection failed: {e}")
            retries += 1
            time.sleep(retries * 10)
            print(f"Retrying... ({retries}/3)")
        except Exception as e:
            print(f"An error occurred: {e}")
            retries += 1
            time.sleep(retries * 10)
            print(f"Retrying... ({retries}/3)")
        finally:
            if ssh.get_transport() is not None:
                ssh.close()


def split_list(input_list, batch_size):
    """
    将列表切分为指定批次大小的多个子列表

    Parameters:
    input_list (list): 要切分的列表
    batch_size (int): 每个批次的大小

    Returns:
    list of lists: 切分后的子列表
    """
    return [input_list[i:i + batch_size] for i in range(0, len(input_list), batch_size)]


def exec_importer(ip, project, file, import_mode):
    remote_file_path = "/sensorsdata/main/runtime/idm-case/"
    exec_command(ip, f"su - sa_cluster -c 'rm -r {remote_file_path}'")
    exec_command(ip, f"su - sa_cluster -c 'mkdir -p {remote_file_path}'")
    cp_to(ip, f"./{file}", f"{remote_file_path + file}")
    hdfs_path = "/sa/runtime/sgx-idm-temp"
    exec_command(ip, f"su - sa_cluster -c 'hdfs dfs -rm -r -f -skipTrash {hdfs_path}'")
    exec_command(ip, f"su - sa_cluster -c 'hdfs dfs -mkdir -p {hdfs_path}'")
    exec_command(ip, f"su - sa_cluster -c 'hdfs dfs -put {remote_file_path + file} {hdfs_path}'")
    cluster = check_is_cluster(ip)
    if import_mode == "importer_v2":
        cmd = f"integratoradmin importer_v2 --method submit_task --project {project} --path hdfs://{hdfs_path} --parallelism 3 --yjm 2048 --ytm 4096"
    elif import_mode == "importer":
        if not cluster:
            cmd = f"integratoradmin importer run --project {project} --path file://{remote_file_path + file} --parallelism 1 --mem_mb 4096"
        else:
            cmd = f"integratoradmin importer run --project {project} --path hdfs://{hdfs_path} --parallelism 3 --yjm 2048 --ytm 4096"
    elif import_mode == "hdfs_importer":
        cmd = f"hdfs_importer --project {project} --path {hdfs_path} --mapper_max_memory_size_mb 2048 --reduce_max_memory_size_mb 4096 --event_mapper_max_size 3 --item_mapper_max_size 3 --profile_mapper_max_size 3"
    else:
        cmd = ""
    start_time = time.time()
    exec_command(ip, 'su - sa_cluster -c "{}"'.format(cmd))
    return time.time() - start_time


def get_ips_from_hosts(ip):
    paragraph = exec_command(ip, "su - sa_cluster -c 'cat /etc/hosts' ")

    ip_list = []
    lines = paragraph.split('\n')
    for line in lines:
        if match_valid_host(line):
            ip = extract_ip_from_line(line)
            if ip:
                ip_list.append(ip)
    ip_set = set(ip_list)
    return list(ip_set)


def match_valid_host(host_line):
    pattern = r'(hybrid|data)\d{1,2}'
    return bool(re.search(pattern, host_line))


def extract_ip_from_line(line):
    pattern = r'(\d+\.\d+\.\d+\.\d+)'
    match = re.search(pattern, line)
    if match:
        return match.group(1)
    return None


def pause_import_and_wait_consume_latency(ip, env_version):
    print("暂停导入并等待数据延迟被消费完成")
    if env_version == 'new':
        pause_module(ip, "edge", "edge")
        start_module(ip, "integrator", "scheduler")
        waiting_sdi_consume_latency(ip)
    else:
        pause_module(ip, "edge", "edge")
        start_module(ip, "sdf", "extractor")
        waiting_extractor_consume_latency(ip)


def start_import_and_pause_handler(ip, env_version):
    print("开启导入并暂停数据处理, 堆积压测数据")
    if env_version == 'new':
        pause_module(ip, "integrator", "scheduler")
        start_module(ip, "edge", "edge")
        clear_sdi_scheduler_log(ip)
    else:
        pause_module(ip, "sdf", "extractor")
        start_module(ip, "edge", "edge")
        clear_extractor_log(ip)


def start_handler(ip, env_version):
    print("开启数据处理, 消费堆积数据")
    if env_version == 'new':
        start_module(ip, "integrator", "scheduler")
    else:
        start_module(ip, "sdf", "extractor")


def restart_module(ip, product, module):
    exec_command(ip, 'su - sa_cluster -c "aradmin restart -p {} -m {}" '.format(product, module))


def pause_module(ip, product, module):
    exec_command(ip, 'su - sa_cluster -c "aradmin pause -p {} -m {} -d 86400" '.format(product, module))


def start_module(ip, product, module):
    exec_command(ip, 'su - sa_cluster -c "aradmin start -p {} -m {}" '.format(product, module))


def check_sdi_exists_latency(ip):
    result = exec_command(ip, 'su - sa_cluster -c "integratoradmin check_latency"')
    if result.__contains__("don't exist latency"):
        print("检测 sdi 无延迟")
        return False
    else:
        return True


def waiting_sdi_consume_latency(ip):
    i = 0
    while True:
        result = exec_command(ip, 'su - sa_cluster -c "integratoradmin check_latency"')
        if result.__contains__("don't exist latency") or get_sdi_latency_total_size(result) < 10:
            print("检测 sdi 无延迟")
            break
        elif i >= 120:
            print("检测 sdi 存在延迟, 已等待超过40分钟, 请检查服务状态!")
            time.sleep(20)
            i = i + 1
        else:
            i = i + 1
            time.sleep(20)
            print("检测 sdi 存在延迟, 已等待 {}s".format(20 * (i - 1)))


def get_sdi_latency_total_size(result):
    pattern = re.compile(r'delay_record_size=(\d+)')
    delay_sizes = pattern.findall(result)

    total_delay_size = 0
    for delay_size in delay_sizes:
        total_delay_size += int(delay_size)
    return total_delay_size


def waiting_extractor_consume_latency(ip):
    i = 0
    while True:
        result = exec_command(ip, 'su - sa_cluster -c "sdfadmin latency extractor"')
        if result.__contains__("total_latency_count"):
            data = json.loads(result)
            total_latency_count_value = data.get("total_latency_count", None)
            if total_latency_count_value is not None and total_latency_count_value <= 10:
                print("检测 extractor 无延迟")
                break
            else:
                i = i + 1
                print("检测 extractor 存在延迟, 已等待 {}s".format(20 * (i - 1)))
        else:
            i = i + 1
            time.sleep(20)
            print("检测 extractor 延迟失败, 已等待 {}s".format(20 * (i - 1)))


def clear_sdi_scheduler_log(ip):
    is_cluster = check_is_cluster(ip)
    if not is_cluster:
        # 单机环境每个 case 压测前清空日志, 避免统计 qps 时拿到上一次的
        exec_command(ip, 'su - sa_cluster -c "echo > /sensorsdata/main/logs/integrator/scheduler/chain.log"')


def clear_extractor_log(ip):
    exec_command(ip, 'su - sa_cluster -c "echo > /sensorsdata/main/logs/sdf/extractor/extractor.log"')


def check_is_cluster(ip):
    s = paramiko.SSHClient()
    s.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    s.connect(hostname=ip, username=username, password=get_pwd())
    stdin, stdout, stderr = s.exec_command("cat /etc/hosts")
    cluster = False
    for line in stdout:
        print(line)
        if ("hybrid03" in line):
            cluster = True
    print("cluster is %s" % cluster)
    return cluster


def cp_to(ip, src, target):
    print('start put %s to %s' % (src, target))
    s = paramiko.Transport(ip, 22)
    s.connect(username=username, password=get_pwd())
    sftp = paramiko.SFTPClient.from_transport(s)
    sftp.put(src, target)


def cp_from(ip, src, target):
    print('start get %s to %s' % (src, target))
    s = paramiko.Transport(ip, 22)
    s.connect(username=username, password=get_pwd())
    sftp = paramiko.SFTPClient.from_transport(s)
    sftp.get(src, target)
    time.sleep(10)


def import_api(gzipType, dataType, jsonString, server):
    Udata = dealwith(gzipType, jsonString)
    if gzipType == 1 and dataType == 1:
        payload = "gzip=1&data_list=%s" % Udata
    elif gzipType == 1 and dataType == 0:
        payload = "gzip=1&data=%s" % Udata
    elif gzipType == 0 and dataType == 1:
        payload = "data_list=%s" % Udata
    elif gzipType == 0 and dataType == 0:
        payload = "data=%s" % Udata
    else:
        print("非法参数！！")
    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'Connection': "close"
    }
    s = requests.session()
    s.keep_alive = False
    response = requests.post(server, data=payload, headers=headers)
    print(response)


def dealwith(gzipType, jsonString):
    data = json.dumps(jsonString, ensure_ascii=False)
    # print("json: "+data)
    data = data.encode('utf-8')
    if gzipType == 1:
        # gzip压缩
        data = gzip.compress(data)
    Bdata = base64.b64encode(data)
    Udata = urllib.parse.quote(Bdata)
    return Udata


def collect_sdi_qps(ip, data_count):
    i = 0
    while check_sdi_exists_latency(ip):
        time.sleep(20)
        print("等待 sdi 数据处理完成, 已等待{}s".format(i * 20))
        i += 1
        if i * 20 == 600:
            pause_module(ip, 'edge', 'edge')
    print("sdi 数据处理完成, 开始统计qps")
    qps = check_sdi_qps(ip, 30, data_count)
    return qps


def check_sdi_qps(ip, line_count, data_count):
    is_cluster = check_is_cluster(ip)
    if is_cluster:
        exec_command(ip,
                     'su - sa_cluster -c "yarn app -list | grep chain | awk -F \' \' \'{print $1}\'| xargs yarn logs -applicationId | grep \'speed info\' | tail -n ' + str(
                         line_count) + ' > /home/sa_cluster/log_data.log"')
    else:
        exec_command(ip,
                     'su - sa_cluster -c "grep \\"entry processed speed info\\" /sensorsdata/main/logs/integrator/scheduler/chain.log | tail -n ' + str(
                         line_count) + ' > /home/sa_cluster/log_data.log"')

    time.sleep(5)
    identification = time.time()
    local_file_name = "log_data_{}.log".format(identification)
    if os.path.exists(local_file_name):
        os.remove(local_file_name)
    cp_from(ip, "/home/sa_cluster/log_data.log", "./{}".format(local_file_name))
    log_data = open(local_file_name, 'r')
    speed_data_list = []

    for line in log_data:
        match = re.search(r'speed=(\d+)', line)
        if match:
            speed_value = match.group(1)
            speed_data_list.append(int(speed_value))
    print("original data list:" + str(speed_data_list))
    # 处理所有的 qps 数据，去除其中无效的(为0的、头尾的)
    speed_data_list_fix = []
    for i in range(len(speed_data_list)):
        if speed_data_list[i] > 10 and speed_data_list[i] < data_count:
            speed_data_list_fix.append(speed_data_list[i])
    if len(speed_data_list_fix) >= 4:
        remove_count = len(speed_data_list_fix) / 4
        i = 1
        while i <= remove_count:
            speed_data_list_fix.pop()
            speed_data_list_fix.pop(0)
            i += 1
    print("last data list:" + str(speed_data_list_fix))

    if len(speed_data_list_fix) > 0:
        avg_qps = round(sum(speed_data_list_fix) / len(speed_data_list_fix))
        max_qps = round(max(speed_data_list_fix))
        min_qps = round(min(speed_data_list_fix))
    else:
        avg_qps = 0
        max_qps = 0
        min_qps = 0
    qps_detail = {}
    print("max_qps：" + str(max_qps))
    print("min_qps：" + str(min_qps))
    print("avg_qps：" + str(avg_qps))
    qps_detail.update({"avg_qps": str(avg_qps)})
    qps_detail.update({"max_qps": str(max_qps)})
    qps_detail.update({"min_qps": str(min_qps)})
    return qps_detail


def collect_extractor_qps(ip, data_count):
    i = 0
    while check_extractor_exists_latency(ip):
        time.sleep(20)
        print("等待 extractor 数据处理完成, 已等待{}s".format(i * 20))
        i += 1
        if i * 20 == 360:
            pause_module(ip, 'edge', 'edge')
    print("extractor 数据处理完成, 开始统计qps")
    qps = check_extractor_qps(ip, data_count)
    return qps


def check_extractor_exists_latency(ip):
    result = exec_command(ip, 'su - sa_cluster -c "sdfadmin latency extractor"')
    if result.__contains__("total_latency_count"):
        data = json.loads(result)
        total_latency_count_value = data.get("total_latency_count", None)
        if total_latency_count_value is not None and total_latency_count_value == 0:
            return False
        else:
            return True
    else:
        return True


def check_extractor_qps(ip, data_count):
    exec_command(ip,
                 'su - sa_cluster -c "grep speed /sensorsdata/main/logs/sdf/extractor/extractor.log  > /home/sa_cluster/log_data.log"')

    time.sleep(5)
    identification = time.time()
    local_file_name = "log_data_{}.log".format(identification)
    if os.path.exists(local_file_name):
        os.remove(local_file_name)
    cp_from(ip, "/home/sa_cluster/log_data.log", "./{}".format(local_file_name))
    log_data = open(local_file_name, 'r')
    speed_data_list = []

    for line in log_data:
        match = re.search(r'Extractor send speed: (\d+) records/sec', line)
        if match:
            speed_value = match.group(1)
            speed_data_list.append(int(speed_value))
    print("original data list:" + str(speed_data_list))
    # 处理所有的 qps 数据，去除其中无效的(为0的、头尾的)
    speed_data_list_fix = []
    for i in range(len(speed_data_list)):
        if speed_data_list[i] > 10 and speed_data_list[i] < data_count:
            speed_data_list_fix.append(speed_data_list[i])
    if len(speed_data_list_fix) >= 4:
        remove_count = len(speed_data_list_fix) / 4
        i = 1
        while i <= remove_count:
            speed_data_list_fix.pop()
            speed_data_list_fix.pop(0)
            i += 1
    print("last data list:" + str(speed_data_list_fix))

    if len(speed_data_list_fix) > 0:
        avg_qps = round(sum(speed_data_list_fix) / len(speed_data_list_fix))
        max_qps = round(max(speed_data_list_fix))
        min_qps = round(min(speed_data_list_fix))
    else:
        avg_qps = 0
        max_qps = 0
        min_qps = 0
    qps_detail = {}
    print("max_qps：" + str(max_qps))
    print("min_qps：" + str(min_qps))
    print("avg_qps：" + str(avg_qps))
    qps_detail.update({"avg_qps": str(avg_qps)})
    qps_detail.update({"max_qps": str(max_qps)})
    qps_detail.update({"min_qps": str(min_qps)})
    return qps_detail