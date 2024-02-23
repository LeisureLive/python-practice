import argparse
import json
import re
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
from idm.cases.mock_idm_cases.profile_set_v2_distinct_new_more_props_anonymous_user import \
    MockIdmProfileSetDistinctNewUserMorePropsCase
from idm.cases.mock_idm_cases.track_v2_distinct_new_user import MockIdmTrackDistinctNewUserCase
from idm.tools.common_tools import exec_command, check_is_cluster, restart_module, pause_module, start_module, \
    waiting_sdi_consume_latency, clear_log, get_ips_from_hosts, get_sdi_version, get_horizon_version, close_mock_idm, \
    open_idm_mock
from idm.tools.email_tool import send_benchmark_result

sys.path.append('..')

global id2_project_qps_list
global id3_project_qps_list
global mock_idm_case_qps_list


def open_idm_optimize_trigger(ip, skip_init):
    if skip_init:
        return
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k id_mapping_is_open_direct_skv -v true --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k id_mapping_engine_open_concurrent -v true --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k id_mapping_direct_skv_thread_pool_size -v 4 --unstable" ')
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
                     'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k id_mapping_batch_process_pack_max_size -v 4096 --unstable" ')
        exec_command(ip,
                     'su - sa_cluster -c "aradmin config set server -m scheduler -p integrator -n job_manager_tm_mem_mb -v 8192" ')
    else:
        # 单机环境需要调整 scheduler 进程的内存大小
        exec_command(ip,
                     'su - sa_cluster -c "aradmin config set server -m scheduler -p integrator -n mem_mb -v 4096" ')

    restart_module(ip, "edge", "edge")
    restart_module(ip, "horizon", "identity_skv_proxy")
    restart_module(ip, "integrator", "scheduler")


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
def create_new_project(ip, project_name, idm_mode, idm_engine_type, skip_init):
    if skip_init:
        return
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


def push_result(ip_list, idm_engine_type, build_user_id, build_url, webhook, import_mode):
    cucumber_dict = {}
    cucumber_dict.update({"执行模式": import_mode})
    cucumber_dict.update({"机器 IP": ip_list})
    env_type = "单机"
    is_cluster = check_is_cluster(ip_list[0])
    if is_cluster:
        env_type = "集群"
    sdi_version = get_sdi_version(ip_list[0])
    horizon_version = get_horizon_version(ip_list[0])
    node_num = len(ip_list)
    cucumber_dict.update({"环境类型": env_type})
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-build_url', type=str, default=None, help='build_url')
    parser.add_argument('-build_user_id', type=str, default='hejie', help='build_user_id')
    parser.add_argument('-ip', type=str, default='127.0.0.1', help='ip')
    parser.add_argument('-webhook', type=str, default='', help='webhook')
    parser.add_argument('-list_count', type=int, default=100, help='list_count')
    parser.add_argument('-id2_mode_data_count', type=int, default=0, help='id2_mode_data_count')
    parser.add_argument('-id3_mode_data_count', type=int, default=0, help='id3_mode_data_count')
    parser.add_argument('-mock_idm_data_count', type=int, default=0, help='mock_idm_data_count')
    parser.add_argument('-id2_mode_project_name', type=str, default='benchmark_id2', help='id2_mode_project_name')
    parser.add_argument('-id3_mode_project_name', type=str, default='benchmark_id3', help='id3_mode_project_name')
    parser.add_argument('-mock_idm_project_name', type=str, default='benchmark_mock_idm', help='mock_idm_project_name')
    parser.add_argument('-idm_engine_type', type=str, default='default', help='default/fast_mode')
    parser.add_argument('-skip_init', type=str, default="false", help='跳过开关、项目初始化')
    parser.add_argument('-import_mode', type=str, default="chain",
                        help='导入模式：chain / hdfs_importer / importer / importer_v2')
    parser.add_argument('-result_delivery_method', type=str, default="push", help='结果通知方式: push(微信机器人推送)/email(邮件通知)')
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
    # 1、对 skv 内存进行调优
    optimize_skv(exec_ip, skip_init)
    # 2、尝试开启 idm 的优化开关, 非特定版本可能会出现开启失败情况
    open_idm_optimize_trigger(exec_ip, skip_init)
    # 3、对 id2 项目进行测试
    id2_project_qps_list = []
    if id2_mode_data_count > 0:
        # 尝试创建项目, 已存在不会报错
        project_name = args.id2_mode_project_name
        create_new_project(exec_ip, project_name, 'id2', idm_engine_type, skip_init)
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
            server = "http://{}:8106/sa?project={}".format(ip, project_name)
            servers.append(server)

        for test_case in test_cases:
            if import_mode != "chain":
                test_case.do_import_test(exec_ip, project_name, id2_mode_data_count, import_mode)
                id2_project_qps_list.append(test_case.collect_import_qps(id2_mode_data_count))
            else:
                pause_module(exec_ip, "edge", "edge")
                start_module(exec_ip, "integrator", "scheduler")
                waiting_sdi_consume_latency(exec_ip)
                pause_module(exec_ip, "integrator", "scheduler")
                start_module(exec_ip, "edge", "edge")
                clear_log(exec_ip)
                test_case.do_test(servers, id2_mode_data_count, list_count)
                start_module(exec_ip, "integrator", "scheduler")
                id2_project_qps_list.append(test_case.collect_qps(exec_ip, id2_mode_data_count))

    # 5、对 id3 项目进行测试
    id3_project_qps_list = []
    if id3_mode_data_count > 0:
        project_name = args.id3_mode_project_name
        create_new_project(exec_ip, project_name, 'id3', idm_engine_type, skip_init)
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
            server = "http://{}:8106/sa?project={}".format(ip, project_name)
            servers.append(server)

        for test_case in test_cases:
            if import_mode != "chain":
                test_case.do_import_test(exec_ip, project_name, id3_mode_data_count, import_mode)
                id3_project_qps_list.append(test_case.collect_import_qps(id3_mode_data_count))
            else:
                pause_module(exec_ip, "edge", "edge")
                start_module(exec_ip, "integrator", "scheduler")
                waiting_sdi_consume_latency(exec_ip)
                pause_module(exec_ip, "integrator", "scheduler")
                start_module(exec_ip, "edge", "edge")
                clear_log(exec_ip)
                test_case.do_test(servers, id3_mode_data_count, list_count)
                start_module(exec_ip, "integrator", "scheduler")
                id3_project_qps_list.append(test_case.collect_qps(exec_ip, id3_mode_data_count))

    # 6、对 mock idm case 进行测试
    mock_idm_case_qps_list = []
    if mock_idm_data_count > 0:
        # 尝试创建项目, 已存在不会报错
        project_name = args.mock_idm_project_name
        create_new_project(exec_ip, project_name, 'id2', idm_engine_type, skip_init)
        identification = time.time()
        # 开启 mock idm
        open_idm_mock(exec_ip)
        test_cases = [
            MockIdmProfileSetDistinctNewUserMorePropsCase(),
            MockIdmTrackDistinctNewUserCase()
        ]

        servers = []
        for ip in ip_list:
            server = "http://{}:8106/sa?project={}".format(ip, project_name)
            servers.append(server)

        for test_case in test_cases:
            pause_module(exec_ip, "edge", "edge")
            start_module(exec_ip, "integrator", "scheduler")
            waiting_sdi_consume_latency(exec_ip)
            pause_module(exec_ip, "integrator", "scheduler")
            start_module(exec_ip, "edge", "edge")
            clear_log(exec_ip)
            test_case.do_test(servers, mock_idm_data_count, list_count)
            start_module(exec_ip, "integrator", "scheduler")
            mock_idm_case_qps_list.append(test_case.collect_qps(exec_ip, mock_idm_data_count))
        close_mock_idm(exec_ip)

    # 7、推送结果
    if args.result_delivery_method == 'email':
        send_benchmark_result(ip_list, idm_engine_type, id2_project_qps_list, id3_project_qps_list,
                              mock_idm_case_qps_list,
                              'enjoyleisure8027@163.com', 'HSUJWIYVQGDMFXDH', args.receiver_emails)
    else:
        push_result(ip_list, idm_engine_type, args.build_user_id, args.build_url, args.webhook, import_mode)
