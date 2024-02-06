import random
import sys
import time
import uuid
from copy import deepcopy

sys.path.append('../../..')
from idm_for_customer.cases.test_case import TestCase
from idm_for_customer.common_tools import collect_sdi_qps, import_api

false = False
true = True


class IdmTrack3DistinctNewUserCase(TestCase):

    def __init__(self):
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
        cnt = int(count / list_count)
        print("开始导入新用户 track(version=3.0) 数据, 数据量={}".format(count))
        for i in range(cnt):
            test_data = self.make_track_v3_new_user(list_count)
            import_api(1, 1, test_data, servers[random.randint(0, len(servers) - 1)])
        print("导入新用户 track(version=3.0) 数据完成")

    def make_track_v3_new_user(self, count):
        track_v3_list = []
        for num in range(count):
            track_json = deepcopy(self.track_v3)
            nm_device_id = str(uuid.uuid4()) + str(int(time.time() * 1000000)) + '_' + str(
                random.randint(1000000, 9999999))
            login_id = 'user_' + str(int(time.time() * 1000000)) + '_' + str(random.randint(1000000, 9999999))
            idfv = 'idfv_' + str(int(time.time() * 1000000)) + '_' + str(random.randint(1000000, 9999999))
            mobile = 'mobile_' + str(int(time.time() * 1000000)) + '_' + str(random.randint(1000000, 9999999))
            cookie = 'cookie' + str(int(time.time() * 1000000)) + '_' + str(random.randint(1000000, 9999999))
            email = 'email_' + str(int(time.time() * 1000000)) + '_' + str(random.randint(1000000, 9999999))
            taobao = 'taobao_' + str(int(time.time() * 1000000)) + '_' + str(random.randint(1000000, 9999999))
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

    def collect_qps(self, data_count):
        qps_detail = collect_sdi_qps(data_count)
        qps_detail['title'] = "新用户 track 事件"
        return qps_detail
