import random
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

sys.path.append('../../..')
from idm.cases.test_case import TestCase
from idm.common_tools import collect_sdi_qps, import_api

false = False
true = True


class IdmTrackV2DistinctNewUserCase(TestCase):

    def __init__(self):
        super().__init__()
        self.track_v2 = {"event": "$pageview", "time": int(time.time() * 1000),
                         "lib": {"$lib_version": "2.6.4-id", "$lib": "iOS", "$app_version": "1.9.0",
                                 "$lib_method": "code"},
                         "properties": {"$device_id": "", "$os_version": "13.4", "$lib_method": "code", "$os": "iOS",
                                        "$screen_height": 896, "$is_first_day": false, "$app_name": "Example_yywang",
                                        "$model": "x86_64", "$screen_width": 414,
                                        "$app_id": "cn.sensorsdata.SensorsData",
                                        "$app_version": "1.9.0", "$manufacturer": "Apple", "$lib": "iOS", "$wifi": true,
                                        "$network_type": "WIFI", "$timezone_offset": -480, "$lib_version": "2.6.4-id"},
                         "distinct_id": "", "type": "track"}

    def do_test(self, servers, count, list_count, proportion=0):
        print("开始导入 track (Mock IDM, 匿名新用户, version=2.0) 数据, 数据量={}".format(count))
        # 单个并发最多导 200w 数据
        if count % 2000000 == 0:
            concurrent_num = int(count / 2000000)
        else:
            concurrent_num = int(count / 2000000) + 1
        avg_count = int(count / concurrent_num)
        futures = []
        with ThreadPoolExecutor(max_workers=concurrent_num) as executor:
            for i in range(concurrent_num):
                future = executor.submit(self.run_make_records, servers, avg_count, list_count, i)
                futures.append(future)
        total_count = 0
        for future in futures:
            total_count += future.result()
        print("导入 track (Mock IDM, 匿名新用户, version=2.0)  数据完成, 数据量={}".format(total_count))

    def run_make_records(self, servers, count, list_count, concurrent_index):
        print("导入 track (Mock IDM, 匿名新用户, version=2.0) 数据, 并发序号={}, 导入量={}".format(concurrent_index, count))
        cnt = int(count / list_count)
        for i in range(cnt):
            test_data = self.make_track_v2_new_user(list_count, concurrent_index)
            import_api(1, 1, test_data, servers[random.randint(0, len(servers) - 1)])
        print("导入 track (Mock IDM, 匿名新用户, version=2.0) 数据完成")
        return count

    def make_track_v2_new_user(self, count, concurrent_index):
        track_v2_list = []
        for num in range(count):
            track_json = deepcopy(self.track_v2)
            nm_device_id = str(uuid.uuid4()) + str(int(time.time() * 1000000)) + '_' + str(
                random.randint(1000000, 9999999)) + str(concurrent_index)
            track_json.update({"distinct_id": nm_device_id})
            track_json.update({"time": int(time.time() * 1000) + num})
            track_json.update({"_track_id": random.randint(1000000, 9999999999)})
            track_json["properties"].update({"$device_id": nm_device_id})
            track_json["properties"].update({"num": str(num)})
            _flush_time = str(random.randint(1000000, 9999999)) + str(num)
            track_json["properties"].update({"case_id": _flush_time})
            track_json["properties"].update({"case_text": "一二三四五" + str(num)})
            track_json["properties"].update({"order": str(num)})
            track_v2_list.append(track_json)
        return track_v2_list

    def collect_qps(self, exec_ip, data_count):
        qps_detail = collect_sdi_qps(exec_ip, data_count)
        qps_detail['title'] = "匿名新用户 track (Mock IDM)"
        return qps_detail
