import argparse
import datetime
import json
import sys
import time

import requests

sys.path.append('../')
from idm.gen_daily_benchmark_basic_data import process_with_param
from idm.test_cases.id2.profile_set_distinct_new_anonymous_user import IdmProfileSetV2DistinctNewAnonymousUserCase
from idm.test_cases.id2.profile_set_distinct_new_login_user import IdmProfileSetV2DistinctNewLoginUserCase
from idm.test_cases.id2.profile_set_distinct_old_anonymous_user import IdmProfileSetV2DistinctOldAnonymousUserCase
from idm.test_cases.id2.profile_set_distinct_old_login_user import IdmProfileSetV2DistinctOldLoginUserCase
from idm.test_cases.id2.profile_track_mixed_distinct_user import IdmProfileTrackMixedUserCase
from idm.test_cases.id2.track_distinct_new_anonymous_user import IdmTrackV2DistinctNewAnonymousUserCase
from idm.test_cases.id2.track_distinct_old_anonymous_user import IdmTrackV2DistinctOldAnonymousUserCase
from idm.test_cases.id2.track_old_anonymous_user_bind_loginid import IdmTrackAnonymousUserBindLoginIdCase
from idm.test_cases.id2.track_old_multi_login_user_only_with_anonymousid import \
    IdmTrackMultiLoginUserOnlyWithAnonymousIdCase
from idm.test_cases.id3.profile_set_distinct_new_user import IdmProfileSetV3DistinctNewUserCase
from idm.test_cases.id3.profile_set_distinct_old_user import IdmProfileSetV3DistinctOldUserCase
from idm.test_cases.id3.profile_track_mixed_distinct_user import IdmProfileTrackV3MixedDistinctUserCase
from idm.test_cases.id3.track_distinct_new_user import IdmTrackV3DistinctNewUserCase
from idm.test_cases.id3.track_distinct_old_user import IdmTrackV3DistinctOldUserCase
from idm.tools.common_tools import get_ips_from_hosts, get_env_version, optimize_skv, open_idm_optimize_trigger, \
    create_new_project, \
    check_is_cluster, get_sdi_version, get_horizon_version, get_sdf_version, pause_import_and_wait_consume_latency, \
    optimize_kafka, balance_skv, start_import_and_pause_handler, start_handler, start_grafana, \
    get_prometheus_ip, pause_edge, wait_profile_stream_consume_latency, pause_handler_and_start_edge, \
    check_edge_latency_and_start_handler
from idm.tools.email_tool import send_benchmark_result
from idm.tools.spark_job import install_spark, send_code, install_requests

global id2_project_qps_list
global id3_project_qps_list
global mock_idm_case_qps_list


def push_result(ip_list, env_version, idm_engine_type, build_user_id, build_url, webhook, import_mode):
    cucumber_dict = {}
    cucumber_dict.update({"执行模式": import_mode})
    cucumber_dict.update({"机器 IP": ip_list})

    is_cluster = check_is_cluster(ip_list[0])
    node_num = len(ip_list)
    if env_version != 'old-env':
        cucumber_dict.update({"环境类型": "SDH 架构"})
        sdi_version = get_sdi_version(ip_list[0])
        horizon_version = get_horizon_version(ip_list[0])
        cucumber_dict.update({'sdi 版本': sdi_version})
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
        elif is_cluster:
            avg_qps = int(case_qps['avg_qps']) * node_num
            max_qps = int(case_qps['max_qps']) * node_num
            min_qps = int(case_qps['min_qps']) * node_num
            value = " avg_qps=" + str(avg_qps) + ", max_qps=" + str(max_qps) \
                    + ", min_qps=" + str(min_qps)
        else:
            value = " avg_qps=" + case_qps['avg_qps'] + ", max_qps=" + case_qps['max_qps'] \
                    + ", min_qps=" + case_qps['min_qps']
        cucumber_dict.update({key: value})

    for case_qps in id3_project_qps_list:
        key = id3_engine_name + str(case_qps['title'])
        if import_mode != "chain":
            value = " avg_qps=" + str(int(case_qps['avg_qps']))
        elif is_cluster:
            avg_qps = int(case_qps['avg_qps']) * node_num
            max_qps = int(case_qps['max_qps']) * node_num
            min_qps = int(case_qps['min_qps']) * node_num
            value = " avg_qps=" + str(avg_qps) + ", max_qps=" + str(max_qps) \
                    + ", min_qps=" + str(min_qps)
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-build_url', type=str, default=None, help='build_url')
    parser.add_argument('-build_user_id', type=str, default='hejie', help='build_user_id')
    parser.add_argument('-target_ip', type=str, default='10.129.26.245', help='target ip')
    parser.add_argument('-webhook', type=str, default='', help='webhook')
    parser.add_argument('-id2_mode_data_count', type=int, default=1500000, help='id2_mode_data_count')
    parser.add_argument('-id3_mode_data_count', type=int, default=1200000, help='id3_mode_data_count')
    parser.add_argument('-id2_mode_project_name', type=str, default='benchmark_id2', help='id2_mode_project_name')
    parser.add_argument('-id3_mode_project_name', type=str, default='benchmark_id3', help='id3_mode_project_name')
    parser.add_argument('-idm_engine_type', type=str, default='default', help='default/fast_mode')
    parser.add_argument('-skip_gen_data', type=str, default="false", help='跳过造数')
    parser.add_argument('-skip_init', type=str, default="false", help='跳过开关、项目初始化')
    parser.add_argument('-import_mode', type=str, default="chain",
                        help='导入模式：chain / hdfs_importer / importer / importer_v2')
    parser.add_argument('-result_delivery_method', type=str, default="push",
                        help='结果通知方式: push(微信机器人推送)/email(邮件通知), 多个通知方式之间使用 ; 进行分隔')
    parser.add_argument('-receiver_emails', type=str, default="", help='邮件通知接收人地址, 多个邮箱地址使用 ; 进行分隔')
    args = parser.parse_args()

    # 0、解析参数
    target_ip = args.target_ip
    skip_gen_data = args.skip_gen_data == 'true'
    skip_init = args.skip_init == "true"
    import_mode = args.import_mode
    target_ip_list = get_ips_from_hosts(target_ip)
    print("target_ip_list = %s" % target_ip_list)
    id2_mode_data_count = args.id2_mode_data_count
    id3_mode_data_count = args.id3_mode_data_count
    idm_engine_type = args.idm_engine_type

    # 造数
    if skip_gen_data is False:
        process_with_param(target_ip, id2_mode_data_count, id3_mode_data_count)

    id2_mode_project_name = args.id2_mode_project_name + "_" + idm_engine_type + "_" \
                            + datetime.datetime.now().strftime("%Y_%m_%d")
    id3_mode_project_name = args.id3_mode_project_name + "_" + idm_engine_type + "_" \
                            + datetime.datetime.now().strftime("%Y_%m_%d")
    env_version = get_env_version(target_ip)

    # 1、对 skv 内存进行调优
    optimize_skv(target_ip, skip_init)
    # 2、尝试开启 idm 的优化开关, 非特定版本可能会出现开启失败情况
    open_idm_optimize_trigger(target_ip, env_version, skip_init)
    optimize_kafka(target_ip, env_version, skip_init)
    start_grafana(target_ip)
    prometheus_ip = get_prometheus_ip(target_ip)
    print(f"prometheus_ip = {prometheus_ip}")
    # 3、初始化 spark 运行环境
    work_path = "/home/sa_cluster/import_data_benchmark"
    script_dir = "daily_build_data_gen"
    install_requests(target_ip_list)
    install_spark(target_ip, work_path, script_dir)
    send_code(target_ip, work_path, script_dir)
    # 4、对 id2 项目进行测试
    id2_project_qps_list = []
    if id2_mode_data_count > 0:
        # 尝试创建项目, 已存在不会报错
        create_new_project(target_ip, env_version, id2_mode_project_name, 'id2', idm_engine_type, skip_init)
        # 尝试执行 skv balance, 避免数据不均衡
        balance_skv(target_ip)
        test_cases = [
            IdmProfileSetV2DistinctNewAnonymousUserCase(),
            IdmProfileSetV2DistinctOldAnonymousUserCase(),
            IdmProfileSetV2DistinctNewLoginUserCase(),
            IdmProfileSetV2DistinctOldLoginUserCase(),
            IdmTrackV2DistinctNewAnonymousUserCase(),
            IdmTrackV2DistinctOldAnonymousUserCase(),
            IdmProfileTrackMixedUserCase(),
            IdmTrackAnonymousUserBindLoginIdCase(),
            IdmTrackMultiLoginUserOnlyWithAnonymousIdCase()
        ]

        pause_import_and_wait_consume_latency(target_ip, env_version)
        for test_case in test_cases:
            pause_edge(target_ip)
            test_case.do_test(target_ip, ",".join(target_ip_list), id2_mode_project_name, id2_mode_data_count)
            wait_profile_stream_consume_latency(target_ip, env_version)
            # 停止 chain 开启 edge, 将数据堆积在 chain 的上游
            pause_handler_and_start_edge(target_ip, env_version)
            # 检查 edge 无延迟后, 开启 chain 处理
            check_edge_latency_and_start_handler(target_ip_list, env_version)
            case_start_time = int(time.time())
            id2_project_qps_list.append(test_case.collect_qps(target_ip, case_start_time))

    # 5、对 id3 项目进行测试
    id3_project_qps_list = []
    if id3_mode_data_count > 0:
        create_new_project(target_ip, env_version, id3_mode_project_name, 'id3', idm_engine_type, skip_init)
        # 尝试执行 skv balance, 避免数据不均衡
        balance_skv(target_ip)
        test_cases = [
            IdmProfileSetV3DistinctNewUserCase(),
            IdmProfileSetV3DistinctOldUserCase(),
            IdmTrackV3DistinctNewUserCase(),
            IdmTrackV3DistinctOldUserCase(),
            IdmProfileTrackV3MixedDistinctUserCase()
        ]

        pause_import_and_wait_consume_latency(target_ip, env_version)
        for test_case in test_cases:
            pause_edge(target_ip)
            test_case.do_test(target_ip, ",".join(target_ip_list), id3_mode_project_name, id3_mode_data_count)
            wait_profile_stream_consume_latency(target_ip, env_version)
            # 停止 chain 开启 edge, 将数据堆积在 chain 的上游
            pause_handler_and_start_edge(target_ip, env_version)
            # 检查 edge 无延迟后, 开启 chain 处理
            check_edge_latency_and_start_handler(target_ip_list, env_version)
            case_start_time = int(time.time())
            id3_project_qps_list.append(test_case.collect_qps(target_ip, case_start_time))

    # 6、mock idm 进行性能测试
    mock_idm_case_qps_list = []
    # 7、推送结果
    if args.result_delivery_method.__contains__('email'):
        send_benchmark_result(target_ip_list, env_version, idm_engine_type, id2_project_qps_list, id3_project_qps_list,
                              mock_idm_case_qps_list, 'enjoyleisure8027@163.com', 'HSUJWIYVQGDMFXDH',
                              args.receiver_emails)
    if args.result_delivery_method.__contains__('push'):
        push_result(target_ip_list, env_version, idm_engine_type, args.build_user_id, args.build_url, args.webhook,
                    import_mode)
