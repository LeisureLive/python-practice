import datetime
import json
import re

import paramiko
import requests

from idm_for_customer.common_tools import check_is_cluster
from idm_for_customer.idm_benchmark import _make_common_content_template

virtual_field_pattern = re.compile(r'([$a-zA-Z_][$a-zA-Z#\d_]*)\.([$a-zA-Z_][$a-zA-Z#\d_]*)', re.I)


def collect_qps(data_count):
    time = datetime.datetime.strptime("2023-10-31T16:52:24", "%Y-%m-%dT%H:%M:%S")
    log_time_day = time.strftime("%Y-%m-%dT")
    log_time_hours = time.strftime("%H")
    log_time_hours_0 = time.strftime("%H")[0]
    log_time_hours_1 = time.strftime("%H")[1]
    log_time_minute = time.strftime("%M")[0]
    # 获取指定时间的日志
    if log_time_minute == '5':
        log_time = log_time_day + '[' + log_time_hours + '-' + str(int(log_time_hours) + 1) + ']'
    else:
        log_time = log_time_day + log_time_hours + ':' + '[' + log_time_minute + '-' + str(
            (int(log_time_minute) + 2)) + ']'

    if data_count >= 10000000:
        log_time = log_time_day + log_time_hours_0 + '[' + log_time_hours_1 + '-' + str(
            (int(log_time_hours_1) + 1)) + ']'
    elif data_count > 5000000:
        log_time = log_time_day + log_time_hours_0 + '[' + log_time_hours_1 + '-' + str(
            (int(log_time_hours_1) + 1)) + ']'
    return log_time


def handleQpsResult():
    qps_list1 = ["100", "200"]
    qps_list2 = ["200", "400"]
    qps_result = []
    qps_list1.insert(0, "profile_set 单个属性")
    qps_list2.insert(0, "profile_set 125个属性")
    qps_result.append(qps_list1)
    qps_result.append(qps_list2)
    for qps in qps_result:
        print("id2" + qps[0] + ":" + "avg_qps=" + qps[1] + ", max_qps=" + qps[2])


def getSpeed():
    # 给定的字符串
    text = "2023-10-31T19:29:46.681+0800 INFO - clear_subscribe_data_progress [] ClearingWorker []: entry processed speed info. [p=0, o=6195660, pro_time=1698751786, rec_time=1698751536, duration=61, speed=1833]"
    # 使用正则表达式匹配 speed 值
    match = re.search(r'speed=(\d+)', text)

    if match:
        speed_value = match.group(1)
        print(f"提取到的 speed 值是: {speed_value}")
    else:
        print("未找到 speed 值")


def push_result(ips, build_user_id, build_url, webhook, cmpt_project_qps_list, id3_project_qps_list):
    cucumber_dict = {}
    cucumber_dict.update({"机器 IP": ips})
    ip_list = ips.split(",")
    env_type = "单机"
    if check_is_cluster(ip_list[0]):
        env_type = "集群"
    cucumber_dict.update({"环境类型": env_type})
    for case_qps in cmpt_project_qps_list:
        key = "[兼容模式] " + str(case_qps['title'])
        value = " avg_qps=" + case_qps['avg_qps'] + ", max_qps=" + case_qps['max_qps'] \
                + ", min_qps=" + case_qps['min_qps']
        cucumber_dict.update({key: value})

    for case_qps in id3_project_qps_list:
        key = "[id3_mode_cases 模式] " + str(case_qps['title'])
        value = " avg_qps=" + case_qps['avg_qps'] + ", max_qps=" + case_qps['max_qps'] \
                + ", min_qps=" + case_qps['min_qps']
        cucumber_dict.update({key: value})

    markdown_dict = {}
    data = {}
    markdown_dict.update({"content": _make_common_content_template(True, build_url, cucumber_dict)})
    data.update({"userid": build_user_id})
    data.update({"msgtype": 'markdown', "message": markdown_dict})
    if webhook is None:
        pass
    else:
        header = {'content-type': 'application/json'}
        markdown_dict.update({"content": _make_common_content_template(True, build_url, cucumber_dict)})
        markdown_dict.update({"mentioned_list": ["{}".format(build_user_id)]})
        data.update({"msgtype": 'markdown', "markdown": markdown_dict})
        print(data)
        wx_request = requests.post(webhook, bytes(json.dumps(data), 'utf-8'), headers=header)
        result = wx_request.json()
        print(result)


def check_is_cluster(ip):
    s = paramiko.SSHClient()
    s.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    s.connect(hostname=ip, username=username, password=password)
    stdin, stdout, stderr = s.exec_command("cat /etc/hosts")
    cluster = False
    for line in stdout:
        print(line)
        if ("hybrid03" in line):
            cluster = True
    print("cluster is %s" % cluster)
    return cluster


def __check_joined_virtual_property(virtual_expression: str):
    result = virtual_field_pattern.findall(virtual_expression)
    if result is None:
        return False
    for match in result:
        if match[0] != 'events' and match[0] != 'users':
            return True
    return False


def get_ips_from_host():
    paragraph = """
    127.0.0.1 VM-29-103-centos localhost.localdomain localhost localhost4.localdomain4 localhost4
    ::1 VM-29-103-centos localhost.localdomain localhost localhost6.localdomain6 localhost6
    10.129.23.133 hybrid03.classic-tx-beijing-01.org-sep-8133.deploy.sensorsdata.cloud hybrid03
    10.129.26.87 hybrid02.classic-tx-beijing-01.org-sep-8133.deploy.sensorsdata.cloud hybrid02
    10.129.29.103 hybrid01.classic-tx-beijing-01.org-sep-8133.deploy.sensorsdata.cloud hybrid01
    10.129.29.103 registry.classic-tx-beijing-01.org-sep-8133.deploy.sensorsdata.cloud
    """

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

if __name__ == '__main__':
    get_ips_from_host()
