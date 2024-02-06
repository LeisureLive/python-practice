import base64
import gzip
import json
import os
import re
import subprocess
import time
import urllib

import requests

false = False
true = True


def exec_command(cmd):
    print('cmd:' + cmd)
    try:
        # 执行命令并捕获输出
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
        stdout = result.stdout
        if not stdout:
            stdout = result.stderr
        print(stdout)
        return stdout
    except subprocess.CalledProcessError as e:
        # 如果命令执行失败，捕获异常并打印错误信息
        print(f"Error executing command: {e}" + e.stderr)
        return e.stderr


def restart_module(product, module):
    exec_command('aradmin restart -p {} -m {}'.format(product, module))
    exec_command('spadmin restart -p {} -m {}'.format(product, module))


def pause_module(product, module):
    exec_command('aradmin pause -p {} -m {} -d 86400'.format(product, module))
    exec_command('spadmin pause -p {} -m {}'.format(product, module))


def start_module(product, module):
    exec_command('aradmin start -p {} -m {}'.format(product, module))
    exec_command('spadmin start -p {} -m {}'.format(product, module))


def check_sdi_exists_latency():
    result = exec_command('integratoradmin check_latency')
    if result.__contains__("don't exist latency"):
        print("检测 sdi 无延迟")
        return False
    else:
        return True


def waiting_sdi_consume_latency():
    i = 0
    while True:
        result = exec_command('integratoradmin check_latency')
        if result.__contains__("don't exist latency"):
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


def clear_log():
    is_cluster = check_is_cluster()
    if not is_cluster:
        # 单机环境每个 case 压测前清空日志, 避免统计 qps 时拿到上一次的
        exec_command('echo > /sensorsdata/main/logs/integrator/scheduler/chain.log')


def check_is_cluster():
    result = exec_command("cat /etc/hosts")
    cluster = False
    if result.__contains__("hybrid03") or result.__contains__("data01"):
        cluster = True
    print("cluster is %s" % cluster)
    return cluster


def mv_from(src, target):
    print('mv from %s to %s' % (src, target))
    exec_command("mv %s %s" % (src, target))
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


def collect_sdi_qps(data_count):
    i = 0
    while check_sdi_exists_latency():
        time.sleep(20)
        print("等待 sdi 数据处理完成, 已等待{}s".format(i * 20))
        i += 1
    print("sdi 数据处理完成, 开始统计qps")
    qps = check_sdi_qps(30, data_count)
    return qps


def check_sdi_qps(line_count, data_count):
    is_cluster = check_is_cluster()
    if is_cluster:
        exec_command(
            'yarn app -list | grep chain | awk -F \' \' \'{print $1}\'| xargs yarn logs -applicationId | grep \'speed info\' | tail -n ' + str(
                line_count) + ' > /home/sa_cluster/log_data.log')
    else:
        exec_command(
            'grep \\"entry processed speed info\\" /sensorsdata/main/logs/integrator/scheduler/chain.log | tail -n ' + str(
                line_count) + ' > /home/sa_cluster/log_data.log')

    time.sleep(5)
    identification = time.time()
    local_file_name = "log_data_{}.log".format(identification)
    if os.path.exists(local_file_name):
        os.remove(local_file_name)
    mv_from("/home/sa_cluster/log_data.log", "./{}".format(local_file_name))
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
