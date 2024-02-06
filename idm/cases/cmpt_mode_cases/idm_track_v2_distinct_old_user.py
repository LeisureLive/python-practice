import gc
import json
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

sys.path.append('../../..')
from idm.cases.test_case import TestCase
from idm.common_tools import collect_sdi_qps, import_api,exec_importer

false = False
true = True


class IdmTrackV2DistinctOldUserCase(TestCase):

    def __init__(self, build_user, identification):
        super().__init__()
        self.import_file_name = "IdmTrackV2DistinctOldUserCase_{}_{}_importer.json".format(build_user, identification)
        self.cost = 0
        self.file_name = "profile_set_v2_more_anonymous_{}_{}.json".format(build_user, identification)
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
        print("开始导入 track(匿名老用户, version=2.0) 数据, 数据量={}".format(count))
        with open(self.file_name, 'r') as f:
            json_data = f.readlines()
        already_identities = [json.loads(line.strip()) for line in json_data]
        # 单个并发最多导 200w 数据
        if count % 2000000 == 0:
            concurrent_num = int(count / 2000000)
        else:
            concurrent_num = int(count / 2000000) + 1
        avg_count = int(count / concurrent_num)
        futures = []
        with ThreadPoolExecutor(max_workers=concurrent_num) as executor:
            for i in range(concurrent_num):
                future = executor.submit(self.run_make_records, servers, already_identities, avg_count, list_count, i)
                futures.append(future)
        total_count = 0
        for future in futures:
            total_count += future.result()
        print("导入 track(匿名老用户, version=2.0) 数据完成, 数据量={}".format(total_count))
        del already_identities
        gc.collect()

    def run_make_records(self, servers, already_identities, count, list_count, concurrent_index):
        print("导入 track(匿名老用户, version=2.0) 数据, 并发序号={}, 导入量={}".format(concurrent_index, count))
        cnt = int(count / list_count)
        for i in range(cnt):
            test_data = self.make_track_v2_old_user(list_count, already_identities)
            import_api(1, 1, test_data, servers[random.randint(0, len(servers) - 1)])
        print("导入 track(匿名老用户, version=2.0) 数据完成")
        return count

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
            track_json["properties"].update({"order": str(num)})
            track_json.update({"_track_id": random.randint(1000000, 9999999999)})
            track_v2_list.append(track_json)
        return track_v2_list

    def collect_qps(self, exec_ip, data_count):
        qps_detail = collect_sdi_qps(exec_ip, data_count)
        qps_detail['title'] = "匿名老用户 track 事件"
        return qps_detail

    def do_import_test(self, exec_ip, project_name, count, import_mode):
        with open(self.file_name, 'r') as f:
            json_data = f.readlines()
        already_identities = [json.loads(line.strip()) for line in json_data]

        self.clean_path(self.import_file_name)
        with open(self.import_file_name, "w") as f:
            for i in range(0, count):
                ret = self.make_track_v2_old_user(1, already_identities)
                f.write(json.dumps(ret[0]) + '\n')

        self.cost = exec_importer(exec_ip, project_name, self.import_file_name, import_mode)
        print(self.cost)

    def collect_import_qps(self, count):
        qps_detail = {}
        qps_detail['title'] = "匿名老用户 track 事件-importer"
        qps_detail['avg_qps'] = count / self.cost
        return qps_detail