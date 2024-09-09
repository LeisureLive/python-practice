import argparse
import json
import sys
import time

import requests

from idm.cases.id2_mode_cases.idm_profile_set_v2_distinct_new_more_props_anonymous_user import \
    IdmProfileSetV2DistinctNewUserMorePropsCase
from idm.cases.id2_mode_cases.idm_profile_set_v2_distinct_new_more_props_login_user import \
    IdmProfileSetV2DistinctNewLoginUserMorePropsCase
from idm.cases.id2_mode_cases.idm_profile_set_v2_distinct_old_more_props_anonymous_user import \
    IdmProfileSetV2DistinctOldUserMorePropsCase
from idm.cases.id2_mode_cases.idm_profile_set_v2_distinct_old_more_props_login_user import \
    IdmProfileSetV2DistinctOldLoginUserMorePropsCase
from idm.cases.id2_mode_cases.idm_track_profile_v2_mixed_user import IdmTrackProfileV2MixedUserCase
from idm.cases.id2_mode_cases.idm_track_v2_distinct_new_login_multi_user import IdmTrackV2DistinctNewLoginMultiUserCase
from idm.cases.id2_mode_cases.idm_track_v2_distinct_new_user import IdmTrackV2DistinctNewUserCase
from idm.cases.id2_mode_cases.idm_track_v2_distinct_old_anonymous_multi_user import \
    IdmTrackV2DistinctAnonymousMultiUserCase
from idm.cases.id2_mode_cases.idm_track_v2_distinct_old_user import IdmTrackV2DistinctOldUserCase
from idm.cases.id3_mode_cases.idm_profile_set_v3_distinct_new_more_props import \
    IdmProfileSetV3DistinctNewUserMorePropsCase
from idm.cases.id3_mode_cases.idm_profile_set_v3_distinct_old_more_props import \
    IdmProfileSetV3DistinctOldUserMorePropsCase
from idm.cases.id3_mode_cases.idm_track_profile_v3_mixed_user import IdmTrackProfileV3MixedUserCase
from idm.cases.id3_mode_cases.idm_track_v3_distinct_new_user import IdmTrack3DistinctNewUserCase
from idm.cases.id3_mode_cases.idm_track_v3_distinct_old_user import IdmTrack3DistinctOldUserCase
from idm.tools.common_tools import check_is_cluster, get_ips_from_hosts, get_sdi_version, \
    get_horizon_version, \
    get_env_version, pause_import_and_wait_consume_latency, pause_edge, \
    get_sdf_version, create_new_project, optimize_skv, open_idm_optimize_trigger, optimize_kafka, \
    balance_skv, wait_profile_stream_consume_latency, pause_handler_and_start_edge, \
    check_edge_latency_and_start_handler, start_grafana, get_prometheus_ip
from idm.tools.email_tool import send_benchmark_result

sys.path.append('..')

global id2_project_qps_list
global id3_project_qps_list
global mock_idm_case_qps_list


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

    result = '## SDH 架构导入流 BENCHMARK 测试结果 \n >' + describe_result
    return result


def push_result(ip_list, env_version, idm_engine_type, build_user_id, build_url, webhook, import_mode):
    cucumber_dict = {}
    cucumber_dict.update({"执行模式": import_mode})
    cucumber_dict.update({"机器 IP": ip_list})

    if env_version != 'old-env':
        cucumber_dict.update({"环境类型": "SDH 架构"})
        if env_version == 'SDH-131':
            sdi_version = get_sdi_version(ip_list[0])
            cucumber_dict.update({'sdi 版本': sdi_version})
        horizon_version = get_horizon_version(ip_list[0])
        cucumber_dict.update({'horizon 版本': horizon_version})
        if idm_engine_type == 'default':
            id2_engine_name = "[兼容模式] "
            id3_engine_name = "[ID3 模式] "
            cucumber_dict.update({"IDM 引擎": "兼容模式引擎 & ID3引擎"})
        else:
            id2_engine_name = "[高性能尽可能关联 2 id] "
            id3_engine_name = "[高性能 ID3] "
            cucumber_dict.update({"IDM 引擎": "高性能引擎"})
    else:
        cucumber_dict.update({"环境类型": "SDF 架构"})
        sdf_version = get_sdf_version(ip_list[0])
        cucumber_dict.update({'sdf 版本': sdf_version})
        id2_engine_name = "[ID2 多对一] "
        id3_engine_name = "[ID3 模式] "

    for case_qps in id2_project_qps_list:
        key = id2_engine_name + str(case_qps['title'])
        if import_mode != "chain":
            value = " avg_qps=" + str(int(case_qps['avg_qps']))
        else:
            value = " avg_qps=" + case_qps['avg_qps'] + ", max_qps=" + case_qps['max_qps']
        cucumber_dict.update({key: value})

    for case_qps in id3_project_qps_list:
        key = id3_engine_name + str(case_qps['title'])
        if import_mode != "chain":
            value = " avg_qps=" + str(int(case_qps['avg_qps']))
        else:
            value = " avg_qps=" + case_qps['avg_qps'] + ", max_qps=" + case_qps['max_qps']
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
    parser.add_argument('-ip', type=str, default='10.129.26.26', help='ip')
    parser.add_argument('-webhook', type=str, default='', help='webhook')
    parser.add_argument('-list_count', type=int, default=100, help='list_count')
    parser.add_argument('-id2_mode_data_count', type=int, default=0, help='id2_mode_data_count')
    parser.add_argument('-id3_mode_data_count', type=int, default=10000, help='id3_mode_data_count')
    parser.add_argument('-mock_idm_data_count', type=int, default=0, help='mock_idm_data_count')
    parser.add_argument('-id2_mode_project_name', type=str, default='benchmark_id2', help='id2_mode_project_name')
    parser.add_argument('-id3_mode_project_name', type=str, default='benchmark_id3', help='id3_mode_project_name')
    parser.add_argument('-mock_idm_project_name', type=str, default='benchmark_mock_idm', help='mock_idm_project_name')
    parser.add_argument('-idm_engine_type', type=str, default='default', help='default/fast_mode')
    parser.add_argument('-skip_init', type=str, default="true", help='跳过开关、项目初始化')
    parser.add_argument('-import_mode', type=str, default="chain",
                        help='导入模式：chain / hdfs_importer / importer / importer_v2')
    parser.add_argument('-result_delivery_method', type=str, default="push",
                        help='结果通知方式: push(微信机器人推送)/email(邮件通知), 多个通知方式之间使用 ; 进行分隔')
    parser.add_argument('-receiver_emails', type=str, help='邮件通知接收人地址, 多个邮箱地址使用 ; 进行分隔')
    args = parser.parse_args()

    # 0、解析参数
    ip = args.ip
    skip_init = args.skip_init == "true"
    import_mode = args.import_mode
    ip_list = get_ips_from_hosts(ip)
    print("ip_list = %s" % ip_list)
    exec_ip = ip_list[0]
    list_count = args.list_count
    id2_mode_data_count = args.id2_mode_data_count
    id3_mode_data_count = args.id3_mode_data_count
    mock_idm_data_count = args.mock_idm_data_count
    idm_engine_type = args.idm_engine_type
    id2_mode_project_name = args.id2_mode_project_name + "_" + idm_engine_type
    id3_mode_project_name = args.id3_mode_project_name + "_" + idm_engine_type
    mock_idm_project_name = args.mock_idm_project_name + "_" + idm_engine_type
    # 环境版本
    env_version = get_env_version(exec_ip)
    # 1、对 skv 内存进行调优
    optimize_skv(exec_ip, skip_init)
    # 2、尝试开启 idm/chain 的优化开关, 非特定版本可能会出现开启失败情况
    open_idm_optimize_trigger(exec_ip, env_version, skip_init)
    optimize_kafka(exec_ip, env_version, skip_init)
    start_grafana(exec_ip)
    prometheus_ip = get_prometheus_ip(exec_ip)
    print(f"prometheus_ip = {prometheus_ip}")
    # 3、对 id2 项目进行测试
    id2_project_qps_list = []
    id2_case_start_time_list = []
    if id2_mode_data_count > 0:
        # 尝试创建项目, 已存在会报错
        create_new_project(exec_ip, env_version, id2_mode_project_name, 'id2', idm_engine_type, skip_init)
        # 尝试执行 skv balance, 避免数据不均衡
        balance_skv(exec_ip)
        identification = time.time()
        test_cases = [
            # IdmProfileSetV2DistinctNewUserLessPropsCase(args.build_user_id, identification),
            IdmProfileSetV2DistinctNewUserMorePropsCase(args.build_user_id, identification),
            IdmProfileSetV2DistinctOldUserMorePropsCase(args.build_user_id, identification),
            IdmProfileSetV2DistinctNewLoginUserMorePropsCase(args.build_user_id, identification),
            IdmProfileSetV2DistinctOldLoginUserMorePropsCase(args.build_user_id, identification),
            IdmTrackV2DistinctNewUserCase(args.build_user_id, identification),
            IdmTrackV2DistinctOldUserCase(args.build_user_id, identification),
            IdmTrackProfileV2MixedUserCase(args.build_user_id, identification),
            IdmTrackV2DistinctNewLoginMultiUserCase(args.build_user_id, identification),
            IdmTrackV2DistinctAnonymousMultiUserCase(args.build_user_id, identification)
        ]
        servers = []
        for ip in ip_list:
            server = "http://{}:8106/sa?project={}".format(ip, id2_mode_project_name)
            servers.append(server)

        pause_import_and_wait_consume_latency(exec_ip, env_version)
        for i in range(len(test_cases)):
            test_case = test_cases[i]
            if import_mode != "chain":
                test_case.do_import_test(exec_ip, id2_mode_project_name, id2_mode_data_count, import_mode)
                id2_project_qps_list.append(test_case.collect_import_qps(id2_mode_data_count))
            else:
                # 停止 edge
                pause_edge(exec_ip)
                # 开始导入数据，制造堆积
                test_case.do_test(servers, id2_mode_data_count, list_count)
                if i > 0:
                    # 等待上个 CASE 处理无延迟后, 统计上个 CASE qps
                    last_case = test_cases[i - 1]
                    case_start_time = id2_case_start_time_list[i - 1]
                    id2_project_qps_list.append(last_case.collect_qps(exec_ip, case_start_time))
                    wait_profile_stream_consume_latency(exec_ip, env_version)
                # 停止 chain 开启 edge, 将数据堆积在 chain 的上游
                pause_handler_and_start_edge(exec_ip, env_version)
                # 检查 edge 无延迟后, 开启 chain 处理
                check_edge_latency_and_start_handler(ip_list, env_version)
                id2_case_start_time_list.append(int(time.time()))
                if i == len(test_cases) - 1:
                    id2_project_qps_list.append(test_case.collect_qps(exec_ip, id2_case_start_time_list[i]))

    # 5、对 id3 项目进行测试
    id3_project_qps_list = []
    id3_case_start_time_list = []
    if id3_mode_data_count > 0:
        create_new_project(exec_ip, env_version, id3_mode_project_name, 'id3', idm_engine_type, skip_init)
        # 尝试执行 skv balance, 避免数据不均衡
        balance_skv(exec_ip)
        identification = time.time()
        test_cases = [
            # IdmProfileSetV3DistinctNewUserLessPropsCase(args.build_user_id, identification),
            IdmProfileSetV3DistinctNewUserMorePropsCase(args.build_user_id, identification),
            IdmProfileSetV3DistinctOldUserMorePropsCase(args.build_user_id, identification),
            IdmTrack3DistinctNewUserCase(args.build_user_id, identification),
            IdmTrack3DistinctOldUserCase(args.build_user_id, identification),
            IdmTrackProfileV3MixedUserCase(args.build_user_id, identification)
        ]
        servers = []
        for ip in ip_list:
            server = "http://{}:8106/sa?project={}".format(ip, id3_mode_project_name)
            servers.append(server)

        pause_import_and_wait_consume_latency(exec_ip, env_version)
        for i in range(len(test_cases)):
            test_case = test_cases[i]
            if import_mode != "chain":
                test_case.do_import_test(exec_ip, id3_mode_project_name, id3_mode_data_count, import_mode)
                id3_project_qps_list.append(test_case.collect_import_qps(id3_mode_data_count))
            else:
                # 停止 edge
                pause_edge(exec_ip)
                # 开始导入数据，制造堆积
                test_case.do_test(servers, id3_mode_data_count, list_count)
                if i > 0:
                    # 等待上个 CASE 处理无延迟后, 统计上个 CASE qps
                    last_case = test_cases[i - 1]
                    case_start_time = id3_case_start_time_list[i - 1]
                    id3_project_qps_list.append(last_case.collect_qps(exec_ip, case_start_time))
                    wait_profile_stream_consume_latency(exec_ip, env_version)
                # 停止 chain 开启 edge, 将数据堆积在 chain 的上游
                pause_handler_and_start_edge(exec_ip, env_version)
                # 检查 edge 无延迟后, 开启 chain 处理
                check_edge_latency_and_start_handler(ip_list, env_version)
                id3_case_start_time_list.append(int(time.time()))
                if i == len(test_cases) - 1:
                    id3_project_qps_list.append(test_case.collect_qps(exec_ip, id3_case_start_time_list[i]))

    # 6、mock idm 进行性能测试
    mock_idm_case_qps_list = []
    # 7、推送结果
    if args.result_delivery_method.__contains__('email'):
        send_benchmark_result(ip_list, env_version, idm_engine_type, id2_project_qps_list, id3_project_qps_list,
                              mock_idm_case_qps_list,
                              'enjoyleisure8027@163.com', 'HSUJWIYVQGDMFXDH', args.receiver_emails)
    if args.result_delivery_method.__contains__('push'):
        push_result(ip_list, env_version, idm_engine_type, args.build_user_id, args.build_url, args.webhook,
                    import_mode)
