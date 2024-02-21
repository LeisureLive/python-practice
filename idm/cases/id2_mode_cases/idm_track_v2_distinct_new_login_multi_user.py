import gc
import json
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

sys.path.append('../../..')
from idm.cases.test_case import TestCase
from idm.tools.common_tools import import_api, collect_sdi_qps, exec_importer

false = False
true = True


class IdmTrackV2DistinctNewLoginMultiUserCase(TestCase):

    def __init__(self, build_user, identification):
        super().__init__()
        self.file_name = "profile_set_v2_more_anonymous_{}_{}.json".format(build_user, identification)
        self.output_file_name = "track_v2_login_multi_user_{}_{}.json".format(build_user, identification)
        self.import_file_name = "IdmTrackV2DistinctNewLoginMultiUserCase_{}_{}_importer.json".format(build_user,
                                                                                                     identification)
        self.cost = 0
        self.track_v2 = {"event": "$pageview", "time": int(time.time() * 1000),
                         "lib": {"$lib_version": "2.6.4-id", "$lib": "iOS", "$app_version": "1.9.0",
                                 "$lib_method": "code"},
                         "properties": {"$device_id": "", "$os_version": "13.4", "$lib_method": "code", "$os": "iOS",
                                        "$screen_height": 896, "$is_first_day": false, "$app_name": "Example_yywang",
                                        "$model": "x86_64", "$screen_width": 414,
                                        "$app_id": "cn.sensorsdata.SensorsData",
                                        "$app_version": "1.9.0", "$manufacturer": "Apple", "$lib": "iOS", "$wifi": true,
                                        "$network_type": "WIFI", "$timezone_offset": -480, "$lib_version": "2.6.4-id"},
                         "distinct_id": "", "login_id": "", "anonymous_id": "", "type": "track"}

        self.track_v2_identities = {
            "distinct_id": "",
            "login_id": "",
            "anonymous_id": ""
        }

    def do_test(self, servers, count, list_count, proportion=0):
        print("开始导入 匿名用户关联同一登录id track 数据(version=2.0), 数据量={}".format(count))
        with open(self.file_name, 'r') as f:
            json_data = f.readlines()
        already_identities = [json.loads(line.strip()) for line in json_data]
        count = len(already_identities)
        # 单个并发最多导 100w 数据
        if count % 1000000 == 0:
            concurrent_num = int(count / 1000000)
        else:
            concurrent_num = int(count / 1000000) + 1
        avg_count = int(count / concurrent_num)
        futures = []
        with ThreadPoolExecutor(max_workers=concurrent_num) as executor:
            for i in range(concurrent_num):
                future = executor.submit(self.run_make_records, servers, already_identities, avg_count, list_count, i)
                futures.append(future)
        total_count = 0
        for future in futures:
            total_count += future.result()
        print("导入 匿名用户关联同一登录id track 数据完成, 数据量={}".format(total_count))
        del already_identities
        gc.collect()

    def run_make_records(self, servers, already_identities, count, list_count, concurrent_index):
        print("导入 匿名用户关联同一登录id 数据, 并发序号={}, 导入量={}".format(concurrent_index, count))
        cnt = int(count / list_count)
        base_index = concurrent_index * count
        for i in range(cnt):
            test_data = self.make_track_login_multi_user(already_identities, base_index, i, list_count)
            import_api(1, 1, test_data, servers[random.randint(0, len(servers) - 1)])
        print("导入 匿名用户关联同一登录id track 数据(version=2.0) 完成")
        return count

    def make_track_login_multi_user(self, already_identities, base_index, batch_index, list_count):
        track_v2_list = []
        identity_list = []
        # 一次挑选两个匿名用户, 并将两个匿名用户关联同一个 login_id
        for num in range(int(list_count / 2)):
            track_json1 = deepcopy(self.track_v2)
            track_json2 = deepcopy(self.track_v2)
            identity_json1 = deepcopy(self.track_v2_identities)
            identity_json2 = deepcopy(self.track_v2_identities)

            index = base_index + batch_index * list_count + num * 2
            device_id1 = already_identities[index]['distinct_id']
            device_id2 = already_identities[index + 1]['distinct_id']
            login_id = 'user_' + str(int(time.time() * 1000000)) + '_' + str(random.randint(1000000, 9999999))

            identity_json1['distinct_id'] = login_id
            identity_json1['login_id'] = login_id
            identity_json1['anonymous_id'] = device_id1
            identity_list.append(identity_json1)

            identity_json2['distinct_id'] = login_id
            identity_json2['login_id'] = login_id
            identity_json2['anonymous_id'] = device_id2
            identity_list.append(identity_json2)

            track_json1.update({"distinct_id": login_id})
            track_json1.update({"login_id": login_id})
            track_json1.update({"anonymous_id": device_id1})
            track_json1["properties"].update({"$device_id": device_id1})
            track_json1["properties"].update({"num": str(num)})
            _flush_time = str(random.randint(1000000, 9999999)) + str(num)
            track_json1["properties"].update({"case_id": _flush_time})
            track_json1["properties"].update({"case_text": "一二三四五" + str(num)})
            track_json1["properties"].update({"order": str(num)})
            track_json1.update({"_track_id": random.randint(1000000, 9999999999)})
            track_v2_list.append(track_json1)

            track_json2.update({"distinct_id": login_id})
            track_json2.update({"login_id": login_id})
            track_json2.update({"anonymous_id": device_id2})
            track_json2["properties"].update({"$device_id": device_id2})
            track_json2["properties"].update({"num": str(num)})
            _flush_time = str(random.randint(1000000, 9999999)) + str(num)
            track_json2["properties"].update({"case_id": _flush_time})
            track_json2["properties"].update({"case_text": "一二三四五" + str(num)})
            track_json2["properties"].update({"order": str(num)})
            track_json2.update({"_track_id": random.randint(1000000, 9999999999)})
            track_v2_list.append(track_json2)
        with open(self.output_file_name, 'a') as f:
            for item in identity_list:
                f.write(json.dumps(item) + '\n')
        return track_v2_list

    def collect_qps(self, exec_ip, data_count):
        qps_detail = collect_sdi_qps(exec_ip, data_count)
        qps_detail['title'] = " track (匿名用户关联同一登录id)"
        return qps_detail

    def do_import_test(self, exec_ip, project_name, count, import_mode):
        self.clean_path(self.import_file_name)
        with open(self.file_name, 'r') as f:
            json_data = f.readlines()
        already_identities = [json.loads(line.strip()) for line in json_data]
        with open(self.import_file_name, "w") as f:
            for i in range(0, count):
                ret = self.make_track_login_multi_user(already_identities, i, 1)
                f.write(json.dumps(ret[0]) + '\n')

        self.cost = exec_importer(exec_ip, project_name, self.import_file_name, import_mode)
        print(self.cost)

    def collect_import_qps(self, count):
        qps_detail = {}
        qps_detail['title'] = "匿名用户关联同一登录id -importer"
        qps_detail['avg_qps'] = count / self.cost
        return qps_detail
