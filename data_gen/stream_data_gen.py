import base64
import gzip
import json
import random
import time
import urllib
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy

import requests
from impala.dbapi import connect

profile_set_v2_json = {
    "distinct_id": "",
    "properties": "",
    "type": "profile_set"
}

track_v2_json = {
    "distinct_id": "",
    "properties": "",
    "event": "",
    "type": "track",
    "time": int(time.time() * 1000)
}

profile_set_v3_json = {
    "distinct_id": "",
    "identities": "",
    "properties": "",
    "type": "profile_set"
}

track_v3_json = {
    "distinct_id": "",
    "identities": "",
    "properties": "",
    "event": "",
    "type": "track",
    "time": int(time.time() * 1000)
}


def get_impala_client():
    return connect(
        host='hybrid01.classic-tx-beijing-01.org-sep-9544.deploy.sensorsdata.cloud',
        port=21050,
        user='sa_cluster'
    )


def gen_data(login_user_count, not_login_user_count, login_event_count, not_login_event_count, idm_version, servers):
    start_time = int(time.time())
    all_user_data = fetch_all_user_data(login_user_count, not_login_user_count, idm_version)
    event_data_sql = get_event_data_sql(login_event_count, not_login_event_count, idm_version)
    print("event_data_sql = {}".format(event_data_sql))
    # event(track) : user(profile) 比例, 每 N 条 event 数据中夹 1 条 user 数据
    eu_ratio = int((login_event_count + not_login_event_count) / (login_user_count + not_login_user_count))
    user_data_cursor = 0
    futures = []
    user_datas = []
    event_datas = []
    # 计算每次流式读取 event_data 的数据量
    if len(all_user_data) > 10:
        event_data_fetch_count = eu_ratio * 10
        user_data_cursor_step = 10
    else:
        event_data_fetch_count = eu_ratio * len(all_user_data)
        user_data_cursor_step = len(all_user_data)
    event_data_scan_total_count = 0

    with ThreadPoolExecutor(max_workers=20) as executor:
        connector = get_impala_client()
        cursor = connector.cursor()
        try:
            # 流式查询 event 数据
            cursor.execute("SET MEM_LIMIT=15g;")
            cursor.execute(event_data_sql)
            while True:
                results = cursor.fetchmany(event_data_fetch_count)
                if not results:
                    break
                else:
                    event_datas += results
                    user_datas += all_user_data[user_data_cursor:
                                                min(len(all_user_data), user_data_cursor + user_data_cursor_step)]
                    user_data_cursor += user_data_cursor_step
                    if len(event_datas) > 10000:
                        # 攒一批提交线程池处理
                        future = executor.submit(
                            send_data,
                            list(event_datas),
                            list(user_datas),
                            idm_version,
                            servers)
                        futures.append(future)
                        event_data_scan_total_count += len(event_datas)
                        print("event_data 已扫描数据量 = {}".format(event_data_scan_total_count))
                        event_datas.clear()
                        user_datas.clear()

            # 将最后一批提交
            if len(event_datas) > 0:
                future = executor.submit(
                    send_data,
                    list(event_datas),
                    list(user_datas),
                    idm_version,
                    servers)
                futures.append(future)
                event_data_scan_total_count += len(event_datas)
                print("event_data 已扫描数据量 = {}".format(event_data_scan_total_count))
                event_datas.clear()
                user_datas.clear()

            # 等待所有异步任务完成
            import_count = 0
            for future in as_completed(futures):
                import_count += future.result()
            end_time = int(time.time())
            total_seconds = end_time - start_time
            print("混合数据上报完成, 上报总数据量={}, 上报总耗时={}s. 平均qps={}"
                  .format(import_count, total_seconds, import_count / total_seconds))
        finally:
            cursor.close()
            connector.close()


def fetch_all_user_data(login_user_count, not_login_user_count, idm_version):
    start_time = int(time.time())
    login_user_start_num = random.randint(0, 20000000 - login_user_count)
    not_login_user_start_num = random.randint(20000000, 22000000 - not_login_user_count)
    if idm_version == 'id2':
        sql = '''
        SELECT t.anonymous_id, t.login_id, t.properties FROM (
         SELECT RAND() as rand_num, * FROM user_data WHERE login_id IS NOT NULL AND id >= {} AND id <= 20000000 limit {}
         UNION 
         SELECT RAND() as rand_num, * FROM user_data WHERE login_id IS NULL AND id > {} limit {}
        ) t ORDER BY t.rand_num
        '''.format(login_user_start_num, login_user_count, not_login_user_start_num, not_login_user_count)
    else:
        sql = '''
        SELECT t.anonymous_id, t.login_id, t.properties, t.identities FROM (
         SELECT RAND() as rand_num, * FROM user_data WHERE login_id IS NOT NULL AND id >= {} AND id <= 20000000 limit {}
         UNION 
         SELECT RAND() as rand_num, * FROM user_data WHERE login_id IS NULL AND id > {} limit {}
        ) t ORDER BY t.rand_num
        '''.format(login_user_start_num, login_user_count, not_login_user_start_num, not_login_user_count)

    connector = get_impala_client()
    cursor = connector.cursor()
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        print("user_data_sql={}, 数据量={}".format(sql, len(results)))
        return results
    finally:
        cursor.close()
        connector.close()
        print("user_data 查询完成, 耗时 = {}s".format(int(time.time() - start_time)))


def get_event_data_sql(login_event_count, not_login_event_count, idm_version):
    login_event_start_num = random.randint(0, 19800000 - login_event_count)
    not_login_event_start_num = random.randint(19800000, 22000000 - not_login_event_count)
    if idm_version == 'id2':
        return '''
            SELECT t.event, t.time, t.anonymous_id, t.login_id, t.properties FROM (
            SELECT RAND() as rand_num, event, time, anonymous_id, login_id, properties FROM event_mixed_data WHERE id>= {} AND id < 19800000 limit {} 
            UNION 
            SELECT RAND() as rand_num, event, time, anonymous_id, login_id, properties FROM event_mixed_data WHERE id >= {} limit {}
            ) t
            ORDER BY t.rand_num;
        '''.format(login_event_start_num, login_event_count, not_login_event_start_num, not_login_event_count)
    else:
        return '''
            SELECT t.event, t.time, t.anonymous_id, t.login_id, t.properties, t.identities FROM (
            SELECT RAND() as rand_num, event, time, anonymous_id, login_id, properties, identities FROM event_mixed_data WHERE id>= {} AND id < 19800000 limit {} 
            UNION 
            SELECT RAND() as rand_num, event, time, anonymous_id, login_id, properties, identities FROM event_mixed_data WHERE id >= {} limit {}
            ) t
            ORDER BY t.rand_num
        '''.format(login_event_start_num, login_event_count, not_login_event_start_num, not_login_event_count)


def send_data(event_datas, user_datas, idm_version, servers):
    # 遍历 event_data, 从中插入 user_data
    eu_ratio = int(len(event_datas) / len(user_datas))
    json_string_list = []
    index = 0
    for event_data in event_datas:
        track_json = make_track_json(event_data, idm_version)
        json_string_list.append(track_json)
        if len(json_string_list) % eu_ratio == 0:
            json_string_list.append(make_profile_set_json(user_datas[index], idm_version))
            import_api(1, 1, json_string_list, servers[random.randint(0, len(servers) - 1)])
            index += 1
            json_string_list.clear()
    return len(event_datas) + len(user_datas)


def make_profile_set_json(user_data, idm_version):
    if idm_version == 'id2':
        profile_set_json = deepcopy(profile_set_v2_json)
        profile_set_json.update({"anonymous_id": user_data[0]})
        if user_data[1] is not None:
            profile_set_json.update({"login_id": user_data[1]})
            profile_set_json.update({"distinct_id": user_data[1]})
        else:
            profile_set_json.update({"distinct_id": user_data[0]})
        profile_set_json.update({"properties": json.loads(user_data[2])})
    else:
        profile_set_json = deepcopy(profile_set_v3_json)
        profile_set_json.update({"anonymous_id": user_data[0]})
        if user_data[1] is not None:
            profile_set_json.update({"login_id": user_data[1]})
            profile_set_json.update({"distinct_id": user_data[1]})
        else:
            profile_set_json.update({"distinct_id": user_data[0]})
        profile_set_json.update({"properties": json.loads(user_data[2])})
        profile_set_json.update({"identities": json.loads(user_data[3])})
    return profile_set_json


def make_track_json(event_data, idm_version):
    if idm_version == 'id2':
        track_json = deepcopy(track_v2_json)
        track_json.update({"event": event_data[0]})
        track_json.update({"time": event_data[1]})
        track_json.update({"anonymous_id": event_data[2]})
        if event_data[3] is not None:
            track_json.update({"login_id": event_data[3]})
            track_json.update({"distinct_id": event_data[3]})
        else:
            track_json.update({"distinct_id": event_data[2]})
        track_json.update({"properties": json.loads(event_data[4])})

    else:
        track_json = deepcopy(track_v3_json)
        track_json.update({"event": event_data[0]})
        track_json.update({"time": event_data[1]})
        track_json.update({"anonymous_id": event_data[2]})
        if event_data[3] is not None:
            track_json.update({"login_id": event_data[3]})
            track_json.update({"distinct_id": event_data[3]})
        else:
            track_json.update({"distinct_id": event_data[2]})
        track_json.update({"properties": json.loads(event_data[4])})
        track_json.update({"identities": json.loads(event_data[5])})
    return track_json


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
        return
    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'Connection': "close"
    }
    s = requests.session()
    s.keep_alive = False
    requests.post(server, data=payload, headers=headers)


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


if __name__ == '__main__':
    ip_list = ['10.129.23.220', '10.129.25.225', '10.129.25.79']
    project_name = 'hj_test_id2_2'
    current_time = int(time.time())
    servers = []
    for ip in ip_list:
        server = "http://{}:8106/sa?project={}".format(ip, project_name)
        servers.append(server)
    gen_data(200000, 0, 7200000, 800000, 'id2', servers)
