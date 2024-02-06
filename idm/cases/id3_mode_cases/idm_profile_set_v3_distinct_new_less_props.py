import json
import os
import random
import sys
import time
import uuid
from copy import deepcopy

sys.path.append('../../..')
from idm.cases.test_case import TestCase
from idm.common_tools import collect_sdi_qps, import_api

false = False
true = True


class IdmProfileSetV3DistinctNewUserLessPropsCase(TestCase):

    def __init__(self, build_user, identification):
        super().__init__()
        self.file_name = "profile_set_v3_less_{}_{}.json".format(build_user, identification)
        self.profile_set_v3_less = {
            "distinct_id": "",
            "identities": {
                "$identity_idfv": ""
            },
            "lib": {
                "$lib_version": "2.6.4-id",
                "$lib": "iOS",
                "$app_version": "1.9.0",
                "$lib_method": "code"
            },
            "properties": {
                "myprofile": false
            },
            "type": "profile_set"
        }

    def do_test(self, servers, count, list_count, proportion=0):
        print("开始导入 profile_set(单个属性 version=3.0) 数据, 数据量={}".format(count))
        if os.path.exists(self.file_name):
            print("文件 {} 存在，删除历史记录的用户信息".format(self.file_name))
            os.remove(self.file_name)
        else:
            print("文件 {} 不存在".format(self.file_name))

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
        print("导入 profile_set(单个属性 version=3.0) 数据完成")

    def run_make_records(self, servers, count, list_count, concurrent_index):
        print("导入 profile_set(单个属性 version=3.0) 数据, 并发序号={}, 导入量={}".format(concurrent_index, count))
        cnt = int(count / list_count)
        for i in range(cnt):
            test_data = self.make_profile_set_v3_less(list_count, concurrent_index)
            import_api(1, 1, test_data, servers[random.randint(0, len(servers) - 1)])
        return cnt * list_count

    def make_profile_set_v3_less(self, count, concurrent_index):
        profile_set_list = []
        genders = ['男', '女', '未填写']
        first_visit_source_list = ['微信', 'QQ', '微博', '小红书']
        citys = ['上海', '深圳', '成都', '武汉', '杭州', '北京', '广州', '福州', '天津']
        for i in range(count):
            profile_set_json = deepcopy(self.profile_set_v3_less)
            device_id = str(uuid.uuid4()) + str(int(time.time() * 1000000)) + '_' + str(
                random.randint(1000000, 9999999)) + str(concurrent_index)
            login_id = 'user_' + str(int(time.time() * 1000000)) + '_' + str(
                random.randint(1000000, 9999999)) + str(concurrent_index)
            idfv = 'idfv_' + str(int(time.time() * 1000000)) + '_' + str(
                random.randint(1000000, 9999999)) + str(concurrent_index)
            mobile = 'mobile_' + str(int(time.time() * 1000000)) + '_' + str(
                random.randint(1000000, 9999999)) + str(concurrent_index)
            cookie = 'cookie' + str(int(time.time() * 1000000)) + '_' + str(
                random.randint(1000000, 9999999)) + str(concurrent_index)
            email = 'email_' + str(int(time.time() * 1000000)) + '_' + str(
                random.randint(1000000, 9999999)) + str(concurrent_index)
            taobao = 'taobao_' + str(int(time.time() * 1000000)) + '_' + str(
                random.randint(1000000, 9999999)) + str(concurrent_index)
            profile_set_json['distinct_id'] = device_id
            profile_set_json['identities']['$identity_login_id'] = login_id
            profile_set_json['identities']['$identity_idfv'] = idfv
            profile_set_json['identities']['$identity_mobile'] = mobile
            profile_set_json['identities']['$identity_cookie_id'] = cookie
            profile_set_json['identities']['$identity_email'] = email
            profile_set_json['identities']['$identity_taobao_ouid'] = taobao
            profile_set_json['properties']['gender'] = genders[random.randint(0, len(genders) - 1)]
            profile_set_json['properties']['first_visit_source'] = first_visit_source_list[
                random.randint(0, len(first_visit_source_list) - 1)]
            profile_set_json['properties']['city'] = citys[random.randint(0, len(citys) - 1)]
            profile_set_list.append(profile_set_json)
        with open(self.file_name, 'a') as f:
            for item in profile_set_list:
                f.write(json.dumps(item) + '\n')
        return profile_set_list

    def collect_qps(self, exec_ip, data_count):
        qps_detail = collect_sdi_qps(exec_ip, data_count)
        qps_detail['title'] = "profile_set 新用户(单个属性)"
        return qps_detail
