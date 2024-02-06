import argparse
import json
import re
import sys
import time

import requests

from extractor.cases.id2_cases.idm_profile_set_v2_distinct_new_less_props import \
    IdmProfileSetV2DistinctNewUserLessPropsCase
from extractor.cases.id2_cases.idm_profile_set_v2_distinct_new_more_props_anonymous_user import \
    IdmProfileSetV2DistinctNewUserMorePropsCase
from extractor.cases.id2_cases.idm_profile_set_v2_distinct_old_more_props_anonymous_user import \
    IdmProfileSetV2DistinctOldUserMorePropsCase
from extractor.cases.id2_cases.idm_track_profile_v2_mixed_user import IdmTrackProfileV2MixedUserCase
from extractor.cases.id3_cases.idm_track_profile_v3_mixed_user import IdmTrackProfileV3MixedUserCase

sys.path.append('..')

from extractor.cases.id3_cases.idm_profile_set_v3_distinct_new_more_props import \
    IdmProfileSetV3DistinctNewUserMorePropsCase
from extractor.cases.id3_cases.idm_profile_set_v3_distinct_old_more_props import \
    IdmProfileSetV3DistinctOldUserMorePropsCase
from extractor.cases.id3_cases.idm_track_v3_distinct_new_user import IdmTrack3DistinctNewUserCase
from extractor.cases.id3_cases.idm_track_v3_distinct_old_user import IdmTrack3DistinctOldUserCase
from extractor.cases.id2_cases.idm_profile_set_v2_distinct_new_more_props_login_user import \
    IdmProfileSetV2DistinctNewLoginUserMorePropsCase
from extractor.cases.id2_cases.idm_profile_set_v2_distinct_old_more_props_login_user import \
    IdmProfileSetV2DistinctOldLoginUserMorePropsCase
from extractor.cases.id2_cases.idm_track_v2_distinct_new_user import IdmTrackV2DistinctNewUserCase
from extractor.cases.id2_cases.idm_track_v2_distinct_old_user import IdmTrackV2DistinctOldUserCase
from extractor.common_tools import check_is_cluster, restart_module, start_module, waiting_extractor_consume_latency, \
    pause_module, \
    clear_log, exec_command, get_ips_from_hosts, get_sdf_version

global id2_project_qps_list
global id3_project_qps_list


def open_idm_optimize_trigger(ip):
    exec_command(ip,
                 'su - sa_cluster -c "aradmin ss set -p sdf -m extractor -r extractor -n mem_limit -v "8192Mi"" ')
    exec_command(ip,
                 'su - sa_cluster -c "aradmin ss set -p sdf -m extractor -r extractor -n jvm_xmx -v "8192Mi"" ')

    exec_command(ip,
                 'su - sa_cluster -c "aradmin ss set -p sdf -m id_mapping_skv_proxy -r id_mapping_skv_proxy -n mem_limit -v "8192Mi"" ')
    exec_command(ip,
                 'su - sa_cluster -c "aradmin ss set -p sdf -m id_mapping_skv_proxy -r id_mapping_skv_proxy -n jvm_xmx -v "8192Mi"" ')

    # exec_command(ip,
    #              'su - sa_cluster -c "aradmin ss set -p sdf -m data_loader -r data_loader -n mem_limit -v "4096Mi"" ')
    # exec_command(ip,
    #              'su - sa_cluster -c "aradmin ss set -p sdf -m data_loader -r data_loader -n jvm_xmx -v "4096Mi"" ')

    restart_module(ip, "sdf", "id_mapping_skv_proxy")
    restart_module(ip, "sdf", "extractor")


def optimize_skv(ip):
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
def create_new_project(ip, project_name, idm_mode):
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin project create -c {} -n {} --disable-schema-limited"'
                 .format(project_name, project_name))
    if idm_mode == 'v3.0':
        time.sleep(5)
        exec_command(ip,
                     'su - sa_cluster -c "sdfadmin enable_id_mapping_v3 change_to_v3 -p {} -r "'
                     .format(project_name))


def _make_common_content_template(status, build_url, cucumber_dict: dict):
    """
    大众模版，用来发送普通信息的
    """
    describe_result = ''
    if status:
        for item in cucumber_dict:
            # describe_result += '<font color=\"info\">{}: {}</font> \n >'.format(item, cucumber_dict[item])
            if item == 'StartedByUser':
                describe_result += '<font color=\"info\">{}</font>: <font color=\"info\"><@{}></font>\n' \
                    .format(item, cucumber_dict[item])
            else:
                describe_result += '<font color=\"info\">{}</font>: <font color=\"info\">{}</font>\n' \
                    .format(item, cucumber_dict[item])
        describe_result += '[构建地址]({})\n'.format(build_url)
    else:
        for item in cucumber_dict:
            describe_result += '<font color=\"warning\">{}: {}</font> \n >'.format(item, cucumber_dict[item])
        describe_result += '<font color=\"warning\">构建失败！！！</font> \n >'
        describe_result += '<font color=\"comment\">构建地址:{}</font> \n >'.format(build_url)
        describe_result += '[构建地址]({}) \n >'.format(build_url)

    result = '## BENCHMARK 测试结果 \n >' + describe_result
    return result


def push_result(ip_list, build_user_id, build_url, webhook):
    cucumber_dict = {}
    cucumber_dict.update({"机器 IP": ip_list})
    env_type = "单机"
    is_cluster = check_is_cluster(ip_list[0])
    if is_cluster:
        env_type = "集群"
    node_num = len(ip_list)
    cucumber_dict.update({"环境类型": env_type})
    sdf_version = get_sdf_version(ip_list[0])
    cucumber_dict.update({'sdf 版本': sdf_version})
    for case_qps in id2_project_qps_list:
        key = "[兼容模式] " + str(case_qps['title'])
        if is_cluster:
            avg_qps = int(case_qps['avg_qps']) * node_num
            max_qps = int(case_qps['max_qps']) * node_num
            min_qps = int(case_qps['min_qps']) * node_num
            value = " avg_qps=" + str(avg_qps) + ", max_qps=" + str(max_qps) \
                    + ", min_qps=" + str(min_qps)
            cucumber_dict.update({key: value})
        else:
            value = " avg_qps=" + case_qps['avg_qps'] + ", max_qps=" + case_qps['max_qps'] \
                    + ", min_qps=" + case_qps['min_qps']
            cucumber_dict.update({key: value})

    for case_qps in id3_project_qps_list:
        key = "[id3模式] " + str(case_qps['title'])
        if is_cluster:
            avg_qps = int(case_qps['avg_qps']) * node_num
            max_qps = int(case_qps['max_qps']) * node_num
            min_qps = int(case_qps['min_qps']) * node_num
            value = " avg_qps=" + str(avg_qps) + ", max_qps=" + str(max_qps) \
                    + ", min_qps=" + str(min_qps)
            cucumber_dict.update({key: value})
        else:
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-build_url', type=str, default=None, help='build_url')
    parser.add_argument('-build_user_id', type=str, default='hejie', help='build_user_id')
    parser.add_argument('-ip', type=str, default='127.0.0.1', help='ip')
    parser.add_argument('-webhook', type=str, default='', help='webhook')
    parser.add_argument('-list_count', type=int, default=100, help='list_count')
    parser.add_argument('-id2_mode_data_count', type=int, default=0, help='id2_mode_data_count')
    parser.add_argument('-id3_mode_data_count', type=int, default=0, help='id3_mode_data_count')
    args = parser.parse_args()

    # 0、解析参数
    ip = args.ip
    ip_list = get_ips_from_hosts(ip)
    print("ip_list = %s" % ip_list)
    exec_ip = ip_list[0]
    list_count = args.list_count
    id2_mode_data_count = args.id2_mode_data_count
    id3_mode_data_count = args.id3_mode_data_count
    # 1、对 skv 内存进行调优
    # optimize_skv(exec_ip)
    # 2、尝试开启 idm_for_customer 的优化开关, 非特定版本可能会出现开启失败情况
    # open_idm_optimize_trigger(exec_ip)
    # 3、对兼容模式项目进行测试
    id2_project_qps_list = []
    if id2_mode_data_count > 0:
        # 尝试创建项目, 已存在不会报错
        # create_new_project(exec_ip, "benchmark_extractor_id2", 'v2.0')
        identification = time.time()
        test_cases = [
            IdmProfileSetV2DistinctNewUserLessPropsCase(args.build_user_id, identification),
            IdmProfileSetV2DistinctNewUserMorePropsCase(args.build_user_id, identification),
            IdmProfileSetV2DistinctOldUserMorePropsCase(args.build_user_id, identification),
            # IdmProfileSetV2DistinctNewLoginUserMorePropsCase(args.build_user_id, identification),
            # IdmProfileSetV2DistinctOldLoginUserMorePropsCase(args.build_user_id, identification),
            IdmTrackV2DistinctNewUserCase(),
            IdmTrackV2DistinctOldUserCase(args.build_user_id, identification),
            IdmTrackProfileV2MixedUserCase(args.build_user_id, identification)
        ]
        servers = []
        for ip in ip_list:
            server = "http://{}:8106/sa?project={}".format(ip, "benchmark_extractor_id2")
            servers.append(server)

        for test_case in test_cases:
            start_module(exec_ip, "sdf", "extractor")
            pause_module(exec_ip, "edge", "edge")
            waiting_extractor_consume_latency(exec_ip)
            pause_module(exec_ip, "sdf", "extractor")
            start_module(exec_ip, "edge", "edge")
            clear_log(ip_list)
            test_case.do_test(servers, id2_mode_data_count, list_count)
            start_module(exec_ip, "sdf", "extractor")
            time.sleep(120)
            pause_module(ip, "edge", "edge")
            id2_project_qps_list.append(test_case.collect_qps(exec_ip, id2_mode_data_count))

    # 5、对 id3_mode_cases 项目进行测试
    id3_project_qps_list = []
    if id3_mode_data_count > 0:
        # create_new_project(exec_ip, "benchmark_extractor_id3", 'v3.0')
        identification = time.time()
        test_cases = [
            # IdmProfileSetV3DistinctNewUserLessPropsCase(args.build_user_id, identification),
            IdmProfileSetV3DistinctNewUserMorePropsCase(args.build_user_id, identification),
            IdmProfileSetV3DistinctOldUserMorePropsCase(args.build_user_id, identification),
            IdmTrack3DistinctNewUserCase(),
            IdmTrack3DistinctOldUserCase(args.build_user_id, identification),
            IdmTrackProfileV3MixedUserCase(args.build_user_id, identification)
        ]
        servers = []
        for ip in ip_list:
            server = "http://{}:8106/sa?project={}".format(ip, "benchmark_extractor_id3")
            servers.append(server)

        for test_case in test_cases:
            start_module(exec_ip, "sdf", "extractor")
            pause_module(exec_ip, "edge", "edge")
            waiting_extractor_consume_latency(exec_ip)
            pause_module(exec_ip, "sdf", "extractor")
            start_module(exec_ip, "edge", "edge")
            clear_log(ip_list)
            test_case.do_test(servers, id3_mode_data_count, list_count)
            start_module(exec_ip, "sdf", "extractor")
            time.sleep(120)
            pause_module(ip, "edge", "edge")
            id3_project_qps_list.append(test_case.collect_qps(exec_ip, id3_mode_data_count))

    # 6、推送结果
    push_result(ip_list, args.build_user_id, args.build_url, args.webhook)