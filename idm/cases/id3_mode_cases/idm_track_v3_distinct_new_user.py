import random
import sys
import time
import json
import uuid
from copy import deepcopy

sys.path.append('../../..')
from idm.cases.test_case import TestCase
from idm.common_tools import collect_sdi_qps, import_api, exec_importer

false = False
true = True


class IdmTrack3DistinctNewUserCase(TestCase):

    def __init__(self, build_user, identification):
        self.import_file_name = "IdmTrack3DistinctNewUserCase_{}_{}_importer.json".format(build_user, identification)
        self.cost = 0
        super().__init__()
        self.track_v3 = {"event": "$AppStart", "time": int(time.time() * 1000),
                         "identities": {"$identity_idfv": ""},
                         "lib": {"$lib_version": "2.6.4-id", "$lib": "iOS", "$app_version": "1.9.0",
                                 "$lib_method": "code"},
                         "properties": {"$device_id": "", "$os_version": "13.4", "$lib_method": "code", "$os": "iOS",
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
        print("开始导入新用户 track(version=3.0) 数据, 数据量={}".format(count))
        # 单个并发最多导 100w 数据
        if count % 1000000 == 0:
            concurrent_num = int(count / 1000000)
        else:
            concurrent_num = int(count / 1000000) + 1
        avg_count = int(count / concurrent_num)
        futures = []
        with ThreadPoolExecutor(max_workers=min(concurrent_num, 10)) as executor:
            for i in range(concurrent_num):
                future = executor.submit(self.run_make_records, servers, avg_count, list_count, i)
                futures.append(future)
        total_count = 0
        for future in futures:
            total_count += future.result()
        print("导入新用户 track(version=3.0) 数据完成")

    def run_make_records(self, servers, count, list_count, concurrent_index):
        print("导入 新用户 track(version=3.0) 数据, 并发序号={}, 导入量={}".format(concurrent_index, count))
        cnt = int(count / list_count)
        for i in range(cnt):
            test_data = self.make_track_v3_new_user(list_count, concurrent_index)
            import_api(1, 1, test_data, servers[random.randint(0, len(servers) - 1)])
        return count

    def make_track_v3_new_user(self, count, concurrent_index):
        track_v3_list = []
        for num in range(count):
            track_json = deepcopy(self.track_v3)
            nm_device_id = str(uuid.uuid4()) + str(int(time.time() * 1000000)) + '_' + str(
                random.randint(1000000, 9999999)) + str(concurrent_index)
            login_id = 'user_' + str(int(time.time() * 1000000)) + '_' + str(random.randint(1000000, 9999999)) + str(
                concurrent_index)
            idfv = 'idfv_' + str(int(time.time() * 1000000)) + '_' + str(random.randint(1000000, 9999999)) + str(
                concurrent_index)
            mobile = 'mobile_' + str(int(time.time() * 1000000)) + '_' + str(random.randint(1000000, 9999999)) + str(
                concurrent_index)
            cookie = 'cookie' + str(int(time.time() * 1000000)) + '_' + str(random.randint(1000000, 9999999)) + str(
                concurrent_index)
            email = 'email_' + str(int(time.time() * 1000000)) + '_' + str(random.randint(1000000, 9999999)) + str(
                concurrent_index)
            taobao = 'taobao_' + str(int(time.time() * 1000000)) + '_' + str(random.randint(1000000, 9999999)) + str(
                concurrent_index)
            track_json.update({"distinct_id": nm_device_id})
            track_json.update({"time": int(time.time() * 1000) + num})
            track_json["properties"].update({"$device_id": nm_device_id})
            track_json["identities"].update({"$identity_login_id": login_id})
            track_json["identities"].update({"$identity_idfv": idfv})
            track_json["identities"].update({"$identity_mobile": mobile})
            track_json["identities"].update({"$identity_cookie_id": cookie})
            track_json["identities"].update({"$identity_email": email})
            track_json["identities"].update({"$identity_taobao_ouid": taobao})
            _flush_time = str(random.randint(1000000, 9999999)) + str(num)
            track_json["properties"].update({"case_id": _flush_time})
            track_json["properties"].update({"case_text": "一二三四五" + str(num)})
            track_json["properties"].update({"string_field": "一二三四五六七八九十" + str(num)})
            track_v3_list.append(track_json)
        return track_v3_list

    def collect_qps(self, exec_ip, data_count):
        qps_detail = collect_sdi_qps(exec_ip, data_count)
        qps_detail['title'] = "新用户 track 事件"
        return qps_detail

    def do_import_test(self, exec_ip, project_name, count, import_mode):
        self.clean_path(self.import_file_name)
        with open(self.import_file_name, "w") as f:
            for i in range(0, count):
                ret = self.make_track_v3_new_user(1, 1)
                f.write(json.dumps(ret[0]) + '\n')

        self.cost = exec_importer(exec_ip, project_name, self.import_file_name, import_mode)
        print(self.cost)

    def collect_import_qps(self, count):
        qps_detail = {}
        qps_detail['title'] = "新用户 track 事件-importer"
        qps_detail['avg_qps'] = count / self.cost
        return qps_detail