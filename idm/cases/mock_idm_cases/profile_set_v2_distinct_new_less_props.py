import json
import os
import random
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

sys.path.append('../../..')
from idm.cases.test_case import TestCase
from idm.tools.common_tools import collect_sdi_qps, import_api

false = False
true = True


class IdmProfileSetV2DistinctNewUserLessPropsCase(TestCase):

    def __init__(self, build_user, identification):
        super().__init__()
        self.profile_set_v2_less = {
            "distinct_id": "",
            "lib": {
                "$lib_version": "2.6.4-id",
                "$lib": "iOS",
                "$app_version": "1.9.0",
                "$lib_method": "code"
            },
            "properties": {
                "$ip": "10.129.29.1",
                "client_id": "12312312",
                "client_name": "sdasdasd",
                "gender": "男",
                "first_visit_source": "小红书",
                "city": "深圳",
                "hobbies": [
                    "护肤",
                    "童装"
                ]
            },
            "type": "profile_set"
        }

    def do_test(self, servers, count, list_count, proportion=0):
        print("开始导入 profile_set(Mock IDM, 匿名新用户, 单个属性 version=2.0) 数据, 数据量={}".format(count))
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
        print("导入 profile_set(Mock IDM, 匿名新用户, 单个属性 version=2.0) 数据完成")

    def run_make_records(self, servers, count, list_count, concurrent_index):
        print("导入 profile_set(Mock IDM, 匿名新用户, 单个属性 version=2.0) 数据, 并发序号={}, 导入量={}".format(concurrent_index, count))
        cnt = int(count / list_count)
        for i in range(cnt):
            test_data = self.make_profile_set_v2_less(list_count, concurrent_index)
            import_api(1, 1, test_data, servers[random.randint(0, len(servers) - 1)])
        print("导入 profile_set(Mock IDM, 匿名新用户, 单个属性 version=2.0) 数据完成")
        return cnt * list_count

    def make_profile_set_v2_less(self, count, concurrent_index):
        profile_set_list = []
        genders = ['男', '女', '未填写']
        first_visit_source_list = ['微信', 'QQ', '微博', '小红书']
        citys = ['上海', '深圳', '成都', '武汉', '杭州', '北京', '广州', '福州', '天津']
        for i in range(count):
            profile_set_json = deepcopy(self.profile_set_v2_less)
            device_id = str(uuid.uuid4()) + str(int(time.time() * 1000000)) + '_' + \
                        str(random.randint(1000000, 9999999)) + str(concurrent_index)
            profile_set_json['distinct_id'] = device_id
            profile_set_json['properties']['$ip'] = "10.129.29." + str(random.randint(1, 255))
            profile_set_json['gender'] = genders[random.randint(0, len(genders) - 1)]
            profile_set_json['first_visit_source'] = first_visit_source_list[
                random.randint(0, len(first_visit_source_list) - 1)]
            profile_set_json['city'] = citys[random.randint(0, len(citys) - 1)]
            profile_set_list.append(profile_set_json)
        return profile_set_list

    def collect_qps(self, exec_ip, data_count):
        qps_detail = collect_sdi_qps(exec_ip, data_count)
        qps_detail['title'] = "profile_set (匿名新用户, 单个属性)"
        return qps_detail
