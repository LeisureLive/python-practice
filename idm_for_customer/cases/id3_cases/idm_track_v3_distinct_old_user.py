import gc
import json
import random
import sys
import time
from copy import deepcopy

sys.path.append('../../..')
from idm_for_customer.cases.test_case import TestCase
from idm_for_customer.common_tools import collect_sdi_qps, import_api

false = False
true = True


class IdmTrack3DistinctOldUserCase(TestCase):

    def __init__(self, build_user, identification):
        super().__init__()
        self.file_name = "profile_set_v3_more_{}_{}.json".format(build_user, identification)
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
        cnt = int(count * 4 / list_count)
        print("开始导入老用户 track(version=3.0) 数据, 数据量={}".format(count * 4))
        with open(self.file_name, 'r') as f:
            json_data = f.readlines()
        already_identities = [json.loads(line.strip()) for line in json_data]
        for i in range(cnt):
            test_data = self.make_track_v3_old_user(list_count, already_identities)
            import_api(1, 1, test_data, servers[random.randint(0, len(servers) - 1)])
        del already_identities
        gc.collect()
        print("导入老用户 track(version=3.0) 数据完成")

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
            track_json["properties"].update({"case_id": _flush_time})
            track_json["properties"].update(
                {"case_text": "一二三四五" + str(time.time() * 1000)})
            track_json["properties"].update(
                {"string_field": "一二三四五六七八九十" + str(time.time() * 1000)})
            track_v3_list.append(track_json)
        return track_v3_list

    def collect_qps(self, data_count):
        qps_detail = collect_sdi_qps(data_count)
        qps_detail['title'] = "老用户 track 事件"
        return qps_detail
