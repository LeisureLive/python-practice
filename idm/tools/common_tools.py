import base64
import datetime
import gzip
import json
import os
import re
import sys
import time
import urllib

import paramiko
import requests

from idm.tools.EnvType import EnvType

false = False
true = True
username = 'root'

prometheus_ip = None

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
    if horizon_version != 'unknown' and horizon_version.startswith('1.3.1'):
        return 'SDH-131'
    elif horizon_version != 'unknown' and horizon_version >= '1.3.2':
        return 'SDH-132'
    else:
        return 'old-env'


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


def balance_skv(ip):
    exec_command(ip, 'su - sa_cluster -c "skvadmin balance start -m skv_offline" ')


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
    if env_version != 'old-env':
        create_new_project_in_sdh(ip, project_name, idm_mode, idm_engine_type)
    else:
        create_new_project_in_sdf(ip, project_name, idm_mode)


def create_new_project_in_sdh(ip, project_name, idm_mode, idm_engine_type):
    horizon_version = get_horizon_version(ip)
    if horizon_version < "1.3.1":
        create_new_project_before_sdh131(ip, project_name, idm_mode, idm_engine_type)
    else:
        create_new_project_after_sdh131(ip, project_name, idm_mode, idm_engine_type)


def create_new_project_before_sdh131(ip, project_name, idm_mode, idm_engine_type):
    exec_command_and_check(ip,
                           'su - sa_cluster -c "sbpadmin project create -c {} -n {} --disable-schema-limited"'
                           .format(project_name, project_name))
    time.sleep(5)
    exec_command(ip,
                 'su - sa_cluster -c "horizonadmin identity_tool change_version -p {} -t back_to_cmpt_one"'
                 .format(project_name))
    if idm_mode == 'id2':
        exec_command_and_check(ip,
                               'su - sa_cluster -c "horizonadmin identity_tool change_version -p {} -t open_multi_signup"'
                               .format(project_name))
        if idm_engine_type == 'fast_mode':
            exec_command_and_check(ip,
                                   'su - sa_cluster -c "horizonadmin identity_tool change_version -p {} -t open_fast_mode"'
                                   .format(project_name))
    elif idm_mode == 'id3':
        exec_command_and_check(ip,
                               'su - sa_cluster -c "horizonadmin identity_tool change_version -p {} -t open_id_mapping_v3"'
                               .format(project_name))
        if idm_engine_type == 'fast_mode':
            exec_command_and_check(ip,
                                   'su - sa_cluster -c "horizonadmin identity_tool change_version -p {} -t open_fast_mode"'
                                   .format(project_name))


def create_new_project_after_sdh131(ip, project_name, idm_mode, idm_engine_type):
    # 默认创建项目时高性能尽可能关联 2id
    exec_command_and_check(ip,
                           'su - sa_cluster -c "sbpadmin project create -c {} -n {} --disable-schema-limited"'
                           .format(project_name, project_name))
    time.sleep(5)
    project_id = find_project_id_by_name(ip, project_name)
    if idm_mode == 'id2' and idm_engine_type == 'default':
        # 切回兼容模式
        exec_command_and_check(ip,
                               f'''su - sa_cluster -c 'metadb_cli -usc_dba -Dhorizon_db -e "update sdh_identity_project_config set id_mapping_version=\\"v3.0_cmpt_one\\", idmapping_strategy=\\"MAPPING_ONCE\\", idmapping_exe_engine = \\"ENGINE_STANDARD\\" where project_id = {project_id}" ' ''')
        exec_command_and_check(ip,
                               'su - sa_cluster -c "horizonadmin identity_tool change_version -p {} -t open_multi_signup" '
                               .format(project_name))

    elif idm_mode == 'id3':
        if idm_engine_type == 'default':
            # 切回兼容模式
            exec_command_and_check(ip,
                                   f'''su - sa_cluster -c 'metadb_cli -usc_dba -Dhorizon_db -e "update sdh_identity_project_config set id_mapping_version=\\"v3.0_cmpt_one\\", idmapping_strategy=\\"MAPPING_ONCE\\", idmapping_exe_engine = \\"ENGINE_STANDARD\\" where project_id = {project_id}" ' ''')
            exec_command_and_check(ip,
                                   'su - sa_cluster -c "horizonadmin identity_tool change_version -p {} -t open_id_mapping_v3" '
                                   .format(project_name))
        elif idm_engine_type == 'fast_mode':
            exec_command_and_check(ip,
                                   'su - sa_cluster -c "horizonadmin identity_tool enable_multi_id -p {}" '
                                   .format(project_name))

        # 添加预置用户关联
        completeIdentityConfigForMultiId(ip, project_name)


def find_project_id_by_name(ip, project_name):
    project_id_result = exec_command(ip,
                                     f'''su - sa_cluster -c 'metadb_cli -usc_dba -Dmetadata -e "select id from sbp_project where name =\\"{project_name}"\\"  ' ''')
    if project_id_result.__contains__("doesn't exist") or project_id_result.__contains__("ERROR"):
        project_id_result = exec_command(ip,
                                         f'''su - sa_cluster -c 'metadb_cli -usc_dba -Dsbp_db -e "select id from sbp_project where name =\\"{project_name}"\\"  ' ''')

    match = re.search(r'\d+', project_id_result)
    if match:
        return match.group()
    else:
        raise Exception(f"can't find project_id by name, [name={project_name}]")


def completeIdentityConfigForMultiId(ip, project_name):
    super_api_token = exec_command(ip,
                                   'su - sa_cluster -c "aradmin config get global -n super_api_token -w literal" ')
    super_api_token = super_api_token.replace("\n", "")
    # 查询当前的用户关联配置
    current_timestamp = str(int(time.time() * 1000))
    cmd = f'''
        curl -X GET \
        -H "Content-Type: application/json;charset=UTF-8" \
        'http://{ip}:8107/api/v3/horizon/v1/web/identity/list_identity?token={super_api_token}&project={project_name}&schema_name=users&time={current_timestamp}'
    '''
    identity_result = exec_command_and_check(ip, cmd)
    if not identity_result.__contains__("SUCCESS"):
        raise Exception(f"获取当前用户关联配置失败, [project_name={project_name}, resp={identity_result}]")

    identity_result = json.loads(identity_result)
    identity_datas = identity_result['data']
    need_add_identity_names = ['$identity_mobile', '$identity_email', '$identity_taobao_ouid', '$identity_idfv',
                               '$identity_cookie_id']
    identity_infos = {"$identity_mobile": "用户手机号标识", "$identity_email": "用户邮箱标识", "$identity_taobao_ouid": "淘宝用户 ouid",
                      "$identity_idfv": "IDFV", "$identity_cookie_id": "Web cookie ID"}
    modify_identity_request_body = {"identities": [], "schema_name": "users"}
    for identity_data in identity_datas:
        current_priority = identity_data['priority']
        if identity_data['identity'] in need_add_identity_names:
            need_add_identity_names.remove(identity_data['identity'])

        if identity_data['identity'] == '$identity_anonymous_id':
            # 此时将需要添加的id加入，放在 $identity_anonymous_id 和 $identity_distinct_id 之前
            for need_add_identity_name in need_add_identity_names:
                new_identity = {}
                new_identity['enabled'] = true
                new_identity['is_preset'] = false
                new_identity['uploaded'] = false
                new_identity['priority'] = current_priority
                current_priority = current_priority + 1
                new_identity['cname'] = identity_infos.get(need_add_identity_name)
                new_identity['mode'] = "add"
                new_identity['creator'] = "平台管理员"
                new_identity['identity'] = need_add_identity_name
                modify_identity_request_body['identities'].append(new_identity)

        # $identity_anonymous_id 和 $identity_distinct_id 的优先级后移
        if identity_data['identity'] == '$identity_anonymous_id' or identity_data[
            'identity'] == '$identity_distinct_id':
            identity_data['priority'] = int(identity_data['priority']) + len(need_add_identity_names)

        # 添加到请求体中
        identity_data['last_modified_time'] = None
        identity_data['uploaded'] = true
        modify_identity_request_body['identities'].append(identity_data)

    if len(need_add_identity_names) == 0:
        # 没有需要添加的用户标识
        return

    request_body = json.dumps(modify_identity_request_body, ensure_ascii=false)
    cmd = f'''
        curl -X POST \
        'http://{ip}:8107/api/v3/horizon/v1/web/identity/batch_save?token={super_api_token}&project={project_name}' \
        -H 'Content-Type: application/json;charset=UTF-8' \
        -d '{request_body}'
        '''
    exec_command_and_check(ip, cmd)


def create_new_project_in_sdf(ip, project_name, idm_mode):
    exec_command_and_check(ip,
                           'su - sa_cluster -c "sbpadmin project create -c {} -n {} --disable-schema-limited"'
                           .format(project_name, project_name))
    time.sleep(5)
    if idm_mode == 'id2':
        # 开启多对一
        exec_command(ip,
                     'su - sa_cluster -c "sbpadmin project update -n {} --enable-new-signup"'
                     .format(project_name))
    elif idm_mode == 'id3':
        exec_command(ip,
                     'su - sa_cluster -c "sdfadmin enable_id_mapping_v3 change_to_v3 -p {} -r "'
                     .format(project_name))


def optimize_kafka(ip, env_version, skip_init):
    if skip_init:
        return
    if env_version != 'old-env':
        # kafka 数据过期时间统一调整为 1 天
        exec_command_and_check(ip,
                               "su - sa_cluster -c 'kafka-configs --zookeeper localhost:2181  --alter --entity-name integrator_input_topic --entity-type topics --add-config retention.ms=86400000' ")
        exec_command_and_check(ip,
                               "su - sa_cluster -c 'kafka-configs --zookeeper localhost:2181  --alter --entity-name event_topic --entity-type topics --add-config retention.ms=86400000' ")
        exec_command_and_check(ip,
                               "su - sa_cluster -c 'kafka-configs --zookeeper localhost:2181  --alter --entity-name horizon_stream_profile_command_topic --entity-type topics --add-config retention.ms=86400000' ")
    else:
        exec_command_and_check(ip,
                               "su - sa_cluster -c 'kafka-configs --zookeeper localhost:2181  --alter --entity-name sdf_input_topic --entity-type topics --add-config retention.ms=86400000' ")

def start_grafana(ip):
    exec_command(ip, "su - sa_cluster -c 'smadmin grafana enable' ")

def open_idm_optimize_trigger(ip, env_version, skip_init):
    if skip_init:
        return
    if env_version == 'SDH-131':
        open_idm_optimize_trigger_in_SDH_131(ip)
    elif env_version == 'SDH-132':
        open_idm_optimize_trigger_in_SDH_132(ip)
    else:
        open_idm_optimize_trigger_in_old_env(ip)


def open_idm_optimize_trigger_in_SDH_131(ip):
    # chain
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k redis_batch_process_pack_timeout_sec -v 1 --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k id_mapping_batch_process_pack_timeout_sec -v 1 --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k supply_data_batch_process_pack_timeout_sec -v 1 --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "aradmin config set server -p integrator -m scheduler -n job_manager_tm_mem_mb -v 8192" ')
    exec_command(ip,
                 'su - sa_cluster -c "aradmin config set server -p integrator -m scheduler -n job_manager_buffer_size -v 16384" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k id_mapping_batch_process_pack_max_size -v 4096 --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k supply_data_batch_process_pack_max_size -v 2048 --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k channel_batch_process_pack_max_size -v 1024 --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k first_time_batch_process_pack_max_size -v 1024 --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k last_seen_time_batch_process_pack_max_size -v 1024 --unstable" ')

    # idm
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k id_mapping_is_open_direct_skv -v true --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k id_mapping_engine_open_concurrent -v true --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k id_mapping_direct_skv_thread_pool_size -v 8 --unstable" ')
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
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p integrator -n scheduler -k max_before_deviation_hour_cluster -v 24000 --unstable" ')
    if get_env_type(ip) == EnvType.MINI:
        # 调整 yarn 内存到 26G, mini 集群默认是 23G
        result = exec_command(ip,
                              'su - sa_cluster -c "mothershipadmin config search -m yarn -k yarn.nodemanager.resource.memory-mb" ')
        pattern = re.compile(r'role_config_group_nodemanager_\d+')
        matches = pattern.search(result)
        if matches:
            role_group = matches.group()
            cmd = f"mothershipadmin role_config_group config set -m yarn -r nodemanager --role_config_group {role_group} --namespace yarn-site -k yarn.nodemanager.resource.memory-mb -v 26624 --yes"
            exec_command(ip, f'su - sa_cluster -c "{cmd}"')
    else:
        # 单机环境需要调整 scheduler 进程的内存大小
        exec_command(ip,
                     'su - sa_cluster -c "aradmin config set server -m scheduler -p integrator -n mem_mb -v 4096" ')

    restart_module(ip, "edge", "edge")
    restart_module(ip, "horizon", "identity_skv_proxy")
    restart_module(ip, "integrator", "scheduler")


def open_idm_optimize_trigger_in_SDH_132(ip):
    # chain
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p horizon -n inflow -k redis_batch_process_pack_timeout_sec -v 1 --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p horizon -n inflow -k id_mapping_batch_process_pack_timeout_sec -v 1 --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p horizon -n inflow -k supply_data_batch_process_pack_timeout_sec -v 1 --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p horizon -n inflow -k  job_manager_tm_mem_mb -v 8192" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p horizon -n inflow -k  job_manager_buffer_size -v 16384" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p horizon -n inflow -k id_mapping_batch_process_pack_max_size -v 4096 --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p horizon -n inflow -k supply_data_batch_process_pack_max_size -v 2048 --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p horizon -n inflow -k channel_batch_process_pack_max_size -v 1024 --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p horizon -n inflow -k first_time_batch_process_pack_max_size -v 1024 --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p horizon -n inflow -k last_seen_time_batch_process_pack_max_size -v 1024 --unstable" ')

    # idm
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p horizon -n inflow -k id_mapping_is_open_direct_skv -v true --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p horizon -n inflow -k id_mapping_engine_open_concurrent -v true --unstable" ')
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p horizon -n inflow -k id_mapping_direct_skv_thread_pool_size -v 8 --unstable" ')
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
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p horizon -n inflow -k max_before_deviation_hour_cluster -v 24000 --unstable" ')
    if get_env_type(ip) == EnvType.MINI:
        # 调整 yarn 内存到 26G, mini 集群默认是 23G
        result = exec_command(ip,
                              'su - sa_cluster -c "mothershipadmin config search -m yarn -k yarn.nodemanager.resource.memory-mb" ')
        pattern = re.compile(r'role_config_group_nodemanager_\d+')
        matches = pattern.search(result)
        if matches:
            role_group = matches.group()
            cmd = f"mothershipadmin role_config_group config set -m yarn -r nodemanager --role_config_group {role_group} --namespace yarn-site -k yarn.nodemanager.resource.memory-mb -v 26624 --yes"
            exec_command(ip, f'su - sa_cluster -c "{cmd}"')
    else:
        # 单机环境需要调整 scheduler 进程的内存大小
        exec_command(ip,
                     'su - sa_cluster -c "aradmin config set server -m horizon -p stream_manager -n mem_mb -v 4096" ')

    restart_module(ip, "edge", "edge")
    restart_module(ip, "horizon", "identity_skv_proxy")
    restart_module(ip, "horizon", "stream_manager")


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
    exec_command(ip,
                 'su - sa_cluster -c "sbpadmin business_config set -p sdf -n extractor -k max_before_deviation_hour_cluster -v 24000 --unstable" ')

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
    if env_version == 'SDH-131':
        pause_module(ip, "edge", "edge")
        start_module(ip, "integrator", "scheduler")
        waiting_sdi_consume_latency(ip, env_version)
        wait_profile_stream_consume_latency(ip, env_version)
    elif env_version == 'SDH-132':
        pause_module(ip, "edge", "edge")
        start_module(ip, "horizon", "stream_manager")
        waiting_sdi_consume_latency(ip, env_version)
        wait_profile_stream_consume_latency(ip, env_version)
    else:
        pause_module(ip, "edge", "edge")
        start_module(ip, "sdf", "extractor")
        waiting_extractor_consume_latency(ip)

def start_import_and_pause_handler(ip, env_version):
    print("开启导入并暂停数据处理, 堆积压测数据")
    if env_version == 'SDH-131':
        pause_module(ip, "integrator", "scheduler")
        start_module(ip, "edge", "edge")
        clear_sdi_scheduler_log_before_sdh132(ip)
    elif env_version == 'SDH-132':
        pause_module(ip, "horizon", "stream_manager")
        start_module(ip, "edge", "edge")
        clear_sdi_scheduler_log_after_sdh132(ip)
    else:
        pause_module(ip, "sdf", "extractor")
        start_module(ip, "edge", "edge")
        clear_extractor_log(ip)


def pause_edge(ip):
    print("暂停edge")
    pause_module(ip, "edge", "edge")


def wait_profile_stream_consume_latency(ip, env_version):
    i = 0
    while True:
        if env_version == 'old-env':
            break
        else:
            result = exec_command(ip,
                                  'su - sa_cluster -c "/sensorsdata/main/program/kafka/kafka/kafka_broker/bin/kafka-consumer-groups.sh --bootstrap-server hybrid01:9092 --describe --group horizon_stream_profile_command_group | awk \'{print \$6}\'" ')
            pattern = r'\b\d+\b'
            matches = re.findall(pattern, result)
            lag_sum = sum(int(match) for match in matches)
            if lag_sum == 0:
                print("检测 profile-stream 无延迟")
                break
            elif i >= 120:
                print("检测 profile-stream 存在延迟, 已等待超过40分钟, 请检查服务状态!")
                i = i + 1
                time.sleep(20)
            else:
                i = i + 1
                time.sleep(20)
                print("检测 profile-stream 存在延迟, 已等待 {}s".format(20 * (i - 1)))


def pause_handler_and_start_edge(ip, env_version):
    print("停止数据处理, 开启 edge, 将数据堆积在 process-chain 上游")
    if env_version == 'SDH-131':
        pause_module(ip, "integrator", "scheduler")
        clear_sdi_scheduler_log_before_sdh132(ip)
    elif env_version == 'SDH-132':
        pause_module(ip, "horizon", "stream_manager")
        clear_sdi_scheduler_log_after_sdh132(ip)
    else:
        pause_module(ip, "sdf", "extractor")
        clear_extractor_log(ip)
    start_module(ip, "edge", "edge")


def check_edge_latency_and_start_handler(target_ip_list, env_version):
    print("检查 edge 延迟情况")
    wait_edge_consume_latency(target_ip_list)
    ip = target_ip_list[0]
    print("检查 edge 无延迟, 开始数据处理")
    if env_version == 'SDH-131':
        start_module(ip, "integrator", "scheduler")
    elif env_version == 'SDH-132':
        start_module(ip, "horizon", "stream_manager")
    else:
        start_module(ip, "sdf", "extractor")


def wait_edge_consume_latency(target_ip_list):
    ip = target_ip_list[0]
    i = 0
    while True:
        result = exec_command(ip, 'su - sa_cluster -c "edgeadmin check_latency" ')
        total_latency_size_pattern = r'"total_latency_size": (\d+)'
        total_latency_file_count_pattern = r'"total_latency_file_count": (\d+)'
        match_total_latency_size_group = re.search(total_latency_size_pattern, result)
        match_total_latency_file_count_group = re.search(total_latency_file_count_pattern, result)
        total_latency_size = sys.maxsize
        total_latency_file_count = sys.maxsize
        if match_total_latency_size_group:
            total_latency_size = int(match_total_latency_size_group.group(1))
        if match_total_latency_file_count_group:
            total_latency_file_count = int(match_total_latency_file_count_group.group(1))

        if total_latency_size <= 50000 * len(target_ip_list) and total_latency_file_count == 0:
            print("检测 edge 无延迟")
            break
        elif i >= 15 and total_latency_size <= 100000 * len(target_ip_list):
            print("检测 edge 无延迟")
            break
        elif i >= 120:
            print("检测 edge 存在延迟, 已等待超过40分钟, 请检查服务状态!")
            i = i + 1
            time.sleep(20)
        else:
            i = i + 1
            print("检测 edge 存在延迟, 已等待 {}s".format(20 * i))
            time.sleep(20)


def start_handler(ip, env_version):
    print("开启数据处理, 消费堆积数据")
    if env_version == 'SDH-131':
        start_module(ip, "integrator", "scheduler")
    elif env_version == 'SDH-132':
        start_module(ip, "horizon", "stream_manager")
    else:
        start_module(ip, "sdf", "extractor")


def restart_module(ip, product, module):
    exec_command(ip, 'su - sa_cluster -c "aradmin restart -p {} -m {}" '.format(product, module))


def pause_module(ip, product, module):
    exec_command(ip, 'su - sa_cluster -c "aradmin pause -p {} -m {} -d 86400" '.format(product, module))


def start_module(ip, product, module):
    exec_command(ip, 'su - sa_cluster -c "aradmin start -p {} -m {}" '.format(product, module))


def check_sdi_exists_latency(ip, env_version):
    if env_version == 'SDH-131':
        result = exec_command(ip, 'su - sa_cluster -c "integratoradmin check_latency" ')
    else:
        result = exec_command(ip, 'su - sa_cluster -c "horizonadmin inflow check_latency" ')
    if result.__contains__("don't exist latency") or get_sdi_latency_total_size(result) < 50:
        print("检测 processor-chain 无延迟")
        return False
    else:
        return True


def waiting_sdi_consume_latency(ip, env_version):
    i = 0
    while True:
        if env_version == 'SDH-131':
            result = exec_command(ip, 'su - sa_cluster -c "integratoradmin check_latency" ')
        else:
            result = exec_command(ip, 'su - sa_cluster -c "horizonadmin inflow check_latency" ')
        if result.__contains__("don't exist latency") or get_sdi_latency_total_size(result) < 50:
            print("检测 processor-chain 无延迟")
            break
        elif i >= 120:
            print("检测 processor-chain 存在延迟, 已等待超过40分钟, 请检查服务状态!")
            time.sleep(20)
            i = i + 1
        else:
            i = i + 1
            time.sleep(20)
            print("检测 processor-chain 存在延迟, 已等待 {}s".format(20 * (i - 1)))


def get_sdi_latency_total_size(result):
    pattern = re.compile(r'delay_record_size=(\d+)')
    delay_sizes = pattern.findall(result)

    if len(delay_sizes) == 0:
        return sys.maxsize

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


def clear_sdi_scheduler_log_before_sdh132(ip):
    is_cluster = check_is_cluster(ip)
    if not is_cluster:
        # 单机环境每个 case 压测前清空日志, 避免统计 qps 时拿到上一次的
        exec_command(ip, 'su - sa_cluster -c "echo > /sensorsdata/main/logs/integrator/scheduler/chain.log"')


def clear_sdi_scheduler_log_after_sdh132(ip):
    is_cluster = check_is_cluster(ip)
    if not is_cluster:
        # 单机环境每个 case 压测前清空日志, 避免统计 qps 时拿到上一次的
        exec_command(ip,
                     'su - sa_cluster -c "rm /sensorsdata/main/logs/horizon/stream_manager/*flink-horizon-stream_manager-processorchain.log"')


def clear_extractor_log(ip):
    exec_command(ip, 'su - sa_cluster -c "echo > /sensorsdata/main/logs/sdf/extractor/extractor.log"')


def check_is_cluster(ip):
    s = paramiko.SSHClient()
    s.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    s.connect(hostname=ip, username=username, password=get_pwd())
    is_simplified_cluster = s.exec_command(
        "su - sa_cluster -c 'aradmin config get global -n simplified_cluster -w literal' ")
    if 'True' in is_simplified_cluster:
        return False
    else:
        return True


def get_env_type(ip):
    s = paramiko.SSHClient()
    s.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    s.connect(hostname=ip, username=username, password=get_pwd())
    is_simplified_cluster = s.exec_command(
        "su - sa_cluster -c 'aradmin config get global -n simplified_cluster -w literal' ")
    if 'True' in is_simplified_cluster:
        return EnvType.SIMPLIFY

    stdin, stdout, stderr = s.exec_command("cat /etc/hosts")
    env_type = EnvType.SIMPLIFY
    for line in stdout:
        print(line)
        if "hybrid03" in line:
            env_type = EnvType.MINI
            break
        if "data01" in line:
            env_type = EnvType.STANDARD
            break
    print("env_type is %s" % env_type)
    return env_type


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


def collect_sdi_qps(ip, cast_start_time):
    env_version = get_env_version(ip)
    i = 0
    while check_sdi_exists_latency(ip, env_version):
        time.sleep(20)
        print("等待 sdi 数据处理完成, 已等待{}s".format(i * 20))
        i += 1
    print("sdi 数据处理完成, 开始统计qps")
    cast_end_time = int(time.time())
    qps = check_sdi_qps(ip, cast_start_time, cast_end_time)
    return qps


def check_sdi_qps(ip, start_time, end_time):
    prometheus_ip = get_prometheus_ip(ip)
    url = f"http://{prometheus_ip}:8310/api/v1/query_range"
    query = "sum (irate(integrator_scheduler_chain_generic_processed_entries_total[5m]))"
    interval = '15s'
    data = {
        "query": query,
        "start": start_time,
        "end": end_time,
        "step": interval
    }
    print(f"exec_query_prometheus. [url={url}, data={data}]")
    result = requests.post(url, data=data)
    if result.status_code != 200:
        raise Exception(f"fail to query. [status={result.status_code}]")
    metric_result_json = result.json()
    if metric_result_json.get("status") != "success":
        raise Exception(f"fail to query. [status={metric_result_json.get('status')}]")
        # 获取 json 内的 data 节点下的 result 数组
    metric_list = metric_result_json.get('data').get('result')
    qps_list = []
    for item in metric_list:
         for value in item.get("values"):
             # values 是一个list, 其中包含两个元素，第一个是时间戳，第二个是时间戳对应的指标值
             if value[1] != '0':
                 qps_list.append(int(float(value[1])))

    # 获取最大的 8个点计算平均值作为 QPS
    avg_qps = get_average_of_max_n_elements(qps_list, 8)
    max_qps = get_average_of_max_n_elements(qps_list, 1)
    print(f"qps_list={qps_list}, avg_qps={avg_qps}, max_qps={max_qps}")
    qps_detail = {}
    qps_detail.update({"avg_qps": str(avg_qps)})
    qps_detail.update({"max_qps": str(max_qps)})
    return qps_detail


def get_average_of_max_n_elements(lst, n):
    # 确保n不会超过列表的长度
    n = min(n, len(lst))
    if n == 0:
        return 0
    # 获取最大的n个元素
    max_n_elements = sorted(lst, reverse=True)[:n]
    # 计算平均值
    average = int(sum(max_n_elements) / n)
    return average


def get_prometheus_ip(ip):
    global prometheus_ip
    if prometheus_ip is not None:
        return prometheus_ip
    result = exec_command(ip, "su - sa_cluster -c 'aradmin status -p sm -m prometheus' ")
    host_pattern_group = re.search(r'[hybrid|meta]\d+', result)
    if host_pattern_group:
        prometheus_host = host_pattern_group.group(0)
    else:
        raise RuntimeError("fail to get prometheus host!")
    host_list = exec_command(ip, "su - sa_cluster -c 'cat /etc/hosts'")
    lines = host_list.split('\n')
    for line in lines:
        if line.__contains__(prometheus_host):
            prometheus_ip = extract_ip_from_line(line)
            return prometheus_ip
    raise RuntimeError("fail to get prometheus ip, can't match ip from hosts")


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


if __name__ == '__main__':
    completeIdentityConfigForMultiId('10.129.29.103', 'production')
