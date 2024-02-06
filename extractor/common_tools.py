import base64
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
password = 'MhxzKhl2015'


def get_pwd():
    url = 'http://security.sensorsdata.cn/qa_auth'
    header = {
        'x-qa-tools': 'Oj4QluoQ6ZT6VzACWY9Bqr5gzVFPFhoUPJIqzmZTRxc'
    }
    resp = requests.get(url, headers=header, params=None)
    if resp.status_code == 404:
        return None
    elif resp.status_code != 200:
        raise RuntimeError('get %s error %s, %s' % (url, resp.status_code, resp.text))
    # print(resp.text)
    return resp.text


def exec_command(ip, cmd):
    print('cmd:' + cmd)
    retries = 0
    ssh = paramiko.SSHClient()
    while retries < 3:
        try:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname=ip, username=username, password=get_pwd(), timeout=120)
            stdin, stdout, stderr = ssh.exec_command(cmd)
            result = stdout.read()
            if not result:
                result = stderr.read()
            print(result.decode())
            return result.decode()
        except paramiko.SSHException as e:
            print(f"SSH connection failed: {e}")
            retries += 1
            print(f"Retrying... ({retries}/3)")
        except Exception as e:
            print(f"An error occurred: {e}")
            retries += 1
            print(f"Retrying... ({retries}/3)")
        finally:
            if ssh.get_transport() is not None:
                ssh.close()
    return ""


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


def match_valid_host(host_line):
    pattern = r'(hybrid|data)\d{1,2}'
    return bool(re.search(pattern, host_line))


def extract_ip_from_line(line):
    pattern = r'(\d+\.\d+\.\d+\.\d+)'
    match = re.search(pattern, line)
    if match:
        return match.group(1)
    return None


def restart_module(ip, product, module):
    exec_command(ip, 'su - sa_cluster -c "aradmin restart -p {} -m {}" '.format(product, module))
    exec_command(ip, 'su - sa_cluster -c "spadmin restart -p {} -m {}" '.format(product, module))


def pause_module(ip, product, module):
    exec_command(ip, 'su - sa_cluster -c "aradmin pause -p {} -m {} -d 86400" '.format(product, module))
    exec_command(ip, 'su - sa_cluster -c "spadmin stop -p {} -m {}" '.format(product, module))


def start_module(ip, product, module):
    exec_command(ip, 'su - sa_cluster -c "aradmin start -p {} -m {}" '.format(product, module))
    exec_command(ip, 'su - sa_cluster -c "spadmin start -p {} -m {}" '.format(product, module))


def check_extractor_exists_latency(ip):
    result = exec_command(ip, 'su - sa_cluster -c "sdfadmin latency extractor"')
    if result.__contains__("total_latency_count"):
        data = json.loads(result)
        total_latency_count_value = data.get("total_latency_count", None)
        if total_latency_count_value is not None and total_latency_count_value == 0:
            return True
        else:
            return False
    else:
        return False


def waiting_extractor_consume_latency(ip):
    i = 0
    while True:
        result = exec_command(ip, 'su - sa_cluster -c "sdfadmin latency extractor"')
        if result.__contains__("total_latency_count"):
            data = json.loads(result)
            total_latency_count_value = data.get("total_latency_count", None)
            if total_latency_count_value is not None and total_latency_count_value == 0:
                print("检测 extractor 无延迟")
                break
            else:
                i = i + 1
                print("检测 extractor 存在延迟, 已等待 {}s".format(20 * (i - 1)))
        else:
            i = i + 1
            time.sleep(20)
            print("检测 extractor 延迟失败, 已等待 {}s".format(20 * (i - 1)))


def clear_log(ip_list):
    for ip in ip_list:
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


def collect_extractor_qps(ip, data_count):
    i = 0
    while check_extractor_exists_latency(ip):
        time.sleep(20)
        print("等待 extractor 数据处理完成, 已等待{}s".format(i * 20))
        i += 1
    print("extractor 数据处理完成, 开始统计qps")
    qps = check_extractor_qps(ip, 30, data_count)
    return qps


def check_extractor_qps(ip, line_count, data_count):
    exec_command(ip,
                 'su - sa_cluster -c "grep speed /sensorsdata/main/logs/sdf/extractor/extractor.log | tail -n 80  > /home/sa_cluster/log_data.log"')

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
