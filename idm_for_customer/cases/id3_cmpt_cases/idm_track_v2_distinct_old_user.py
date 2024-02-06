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


class IdmTrackV2DistinctOldUserCase(TestCase):

    def __init__(self, build_user, identification):
        super().__init__()
        self.file_name = "profile_set_v2_more_{}_{}.json".format(build_user, identification)
        self.track_v2 = {
            "event": "$submitOrder",
            "time": int(time.time() * 1000),
            "lib": {
                "$lib_version": "2.6.4-id",
                "$lib": "iOS",
                "$app_version": "1.9.0",
                "$lib_method": "code"
            },
            "properties": {
                "$device_id": "",
                "$os_version": "13.4",
                "$lib_method": "code",
                "$os": "iOS",
                "$screen_height": 896,
                "$is_first_day": false,
                "$app_name": "Example_yywang",
                "$model": "x86_64",
                "$screen_width": 414,
                "$app_id": "cn.sensorsdata.SensorsData",
                "$app_version": "1.9.0",
                "$manufacturer": "Apple",
                "$lib": "iOS",
                "$wifi": true,
                "$network_type": "WIFI",
                "$timezone_offset": -480,
                "$lib_version": "2.6.4-id",
                "benefits": [
                    "9折",
                    "100优惠券"
                ],
                "original_price": 2000,
                "receiver_city": "广州",
                "discount_amount": 100,
                "if_use_discount": false,
                "order_time": "1973-11-24",
                "is_success": true,
                "platform_type": "mobile",
                "order_amount": 10000,
                "cash_back_amount": 0,
                "discount_name": "优惠券B",
                "if_use_points": true,
                "receiver_name": "用户D",
                "commodity_name": "单反相机",
                "receiver_area": "地区B",
                "receiver_province": "广东",
                "transportation_costs": 0,
                "number_of_points": 0,
                "is_cash_back": true,
                "cash_back_channel": "团长A返利",
                "receiver_address": "地址A",
                "points_discount_amount": 100,
                "order_id": 6789
            },
            "distinct_id": "",
            "type": "track"
        }

    def do_test(self, servers, count, list_count, proportion=0):
        cnt = int(count * 10 / list_count)
        print("开始导入老用户 track(version=2.0) 数据, 数据量={}".format(count * 10))
        with open(self.file_name, 'r') as f:
            json_data = f.readlines()
        already_identities = [json.loads(line.strip()) for line in json_data]
        for i in range(cnt):
            test_data = self.make_track_v2_old_user(list_count, already_identities)
            import_api(1, 1, test_data, servers[random.randint(0, len(servers) - 1)])
        del already_identities
        gc.collect()
        print("导入老用户 track(version=2.0) 数据完成")

    def make_track_v2_old_user(self, count, already_identities):
        track_v2_list = []
        for num in range(count):
            track_json = deepcopy(self.track_v2)
            index = random.randint(0, len(already_identities) - 1)
            distinct_id = already_identities[index]['distinct_id']
            track_json.update({"distinct_id": distinct_id})
            track_json.update({"time": int(time.time() * 1000) + num})
            track_json["properties"].update({"$device_id": distinct_id})
            track_json["properties"].update({"num": str(num)})
            _flush_time = str(random.randint(1000000, 9999999)) + str(num)
            track_json["properties"].update({"case_id": _flush_time})
            track_json["properties"].update({"case_text": "一二三四五" + str(num)})
            track_json["properties"].update({"order_id": str(random.randint(1000000, 9999999)) + str(num)})
            track_json.update({"_track_id": random.randint(1000000, 9999999999)})
            track_v2_list.append(track_json)
        return track_v2_list

    def collect_qps(self, data_count):
        qps_detail = collect_sdi_qps(data_count)
        qps_detail['title'] = "老用户 track 事件"
        return qps_detail
