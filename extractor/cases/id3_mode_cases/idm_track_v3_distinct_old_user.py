import gc
import json
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

sys.path.append('../../..')
from extractor.cases.test_case import TestCase
from extractor.tools.common_tools import collect_extractor_qps, import_api, exec_importer

false = False
true = True


class IdmTrack3DistinctOldUserCase(TestCase):

    def __init__(self, build_user, identification):
        super().__init__()
        self.import_file_name = "IdmTrack3DistinctOldUserCase_{}_{}_importer.json".format(build_user, identification)
        self.cost = 0
        self.file_name = "profile_set_v3_more_{}_{}.json".format(build_user, identification)
        self.track_v3 = {"event": "$AppStart", "time": int(time.time() * 1000),
                         "identities": {"$identity_idfv": ""},
                         "lib": {"$lib_version": "2.6.4-id", "$lib": "iOS", "$app_version": "1.9.0",
                                 "$lib_method": "code"},
                         "properties": {"$ip": "10.129.29.1", "$device_id": "", "$os_version": "13.4", "$lib_method": "code", "$os": "iOS",
                                        "$screen_height": 896, "$is_first_day": false, "$app_name": "Example_yywang",
                                        "$model": "x86_64", "$screen_width": 414,
                                        "$app_id": "cn.sensorsdata.SensorsData",
                                        "$app_version": "1.9.0", "$manufacturer": "Apple", "$lib": "iOS", "$wifi": true,
                                        "$network_type": "WIFI", "$timezone_offset": -480, "$lib_version": "2.6.4-id",
                                        "string_field": "1234567890",
                                        "bool_field": true,
                                        "number_field": 1234567890,
                                        "list_field": ["12345678901", "12345678902", "12345678903"],
                                        "datetime_field": "2020-02-02 22:22:22"},
                         "distinct_id": "", "type": "track"}

    def do_test(self, servers, count, list_count, proportion=0):
        print("开始导入老用户 track(version=3.0) 数据, 数据量={}".format(count))
        with open(self.file_name, 'r') as f:
            json_data = f.readlines()
        already_identities = [json.loads(line.strip()) for line in json_data]

        # 单个并发最多导 100w 数据
        if count % 1000000 == 0:
            concurrent_num = int(count / 1000000)
        else:
            concurrent_num = int(count / 1000000) + 1
        avg_count = int(count / concurrent_num)
        futures = []
        with ThreadPoolExecutor(max_workers=min(concurrent_num, 10)) as executor:
            for i in range(concurrent_num):
                future = executor.submit(self.run_make_records, servers, avg_count, list_count, already_identities, i)
                futures.append(future)
        total_count = 0
        for future in futures:
            total_count += future.result()
        del already_identities
        gc.collect()
        print("导入老用户 track(version=3.0) 数据完成")

    def run_make_records(self, servers, count, list_count, already_identities, concurrent_index):
        print("导入 老用户 track(version=3.0) 数据, 并发序号={}, 导入量={}".format(concurrent_index, count))
        cnt = int(count / list_count)
        for i in range(cnt):
            test_data = self.make_track_v3_old_user(list_count, already_identities)
            import_api(1, 1, test_data, servers[random.randint(0, len(servers) - 1)])
        return count

    def make_track_v3_old_user(self, count, already_identities):
        track_v3_list = []
        for num in range(count):
            track_json = deepcopy(self.track_v3)
            index = random.randint(0, len(already_identities) - 1)
            distinct_id = already_identities[index]['distinct_id']
            login_id = already_identities[index]['identities']['$identity_login_id']
            idfv = already_identities[index]['identities']['$identity_idfv']
            mobile = already_identities[index]['identities']['$identity_mobile']
            cookie = already_identities[index]['identities']['$identity_cookie_id']
            email = already_identities[index]['identities']['$identity_email']
            taobao = already_identities[index]['identities']['$identity_taobao_ouid']
            track_json.update({"distinct_id": distinct_id})
            track_json.update({"time": int(time.time() * 1000) + num})
            track_json["properties"].update({"$device_id": distinct_id})
            track_json["identities"].update({"$identity_login_id": login_id})
            track_json["identities"].update({"$identity_idfv": idfv})
            track_json["identities"].update({"$identity_mobile": mobile})
            track_json["identities"].update({"$identity_cookie_id": cookie})
            track_json["identities"].update({"$identity_email": email})
            track_json["identities"].update({"$identity_taobao_ouid": taobao})
            _flush_time = str(random.randint(1000000, 9999999)) + str(num)
            track_json['properties'].update({"$ip": "10.129.29." + str(random.randint(1, 255))})
            track_json["properties"].update({"case_id": _flush_time})
            track_json["properties"].update(
                {"case_text": "一二三四五" + str(time.time() * 1000)})
            track_json["properties"].update(
                {"string_field": "一二三四五六七八九十" + str(time.time() * 1000)})
            track_v3_list.append(track_json)
        return track_v3_list

    def collect_qps(self, exec_ip, data_count):
        qps_detail = collect_extractor_qps(exec_ip, data_count)
        qps_detail['title'] = "track (匿名老用户)"
        return qps_detail

    def do_import_test(self, exec_ip, project_name, count, import_mode):
        with open(self.file_name, 'r') as f:
            json_data = f.readlines()
        already_identities = [json.loads(line.strip()) for line in json_data]

        self.clean_path(self.import_file_name)
        with open(self.import_file_name, "w") as f:
            for i in range(0, count):
                ret = self.make_track_v3_old_user(1, already_identities)
                f.write(json.dumps(ret[0]) + '\n')

        self.cost = exec_importer(exec_ip, project_name, self.import_file_name, import_mode)
        print(self.cost)

    def collect_import_qps(self, count):
        qps_detail = {}
        qps_detail['title'] = "老用户 track 事件-importer"
        qps_detail['avg_qps'] = count / self.cost
        return qps_detail