import datetime
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


class IdmProfileSetV2DistinctOldUserMorePropsCase(TestCase):

    def __init__(self, build_user, identification):
        super().__init__()
        self.file_name = "profile_set_v2_more_{}_{}.json".format(build_user, identification)
        self.profile_set_v2_more = {
            "distinct_id": "",
            "properties": {
                "account": "123123123",
                "client_id": "12312312",
                "client_name": "sdasdasd",
                "gender": "男",
                "first_visit_source": "小红书",
                "city": "深圳",
                "hobbies": [
                    "护肤",
                    "童装"
                ],
                "balance": 163,
                "is_vip": false,
                "name": "邬翔旭",
                "birthday": "1979-05-24",
                "phone_number": "13794242717",
                "career": "老师",
                "quota": 4000,
                "number_int": 2971,
                "number_float": 2000.11,
                "number_negative": -5000,
                "datetime1": "1974-03-18",
                "string1": "18216782405",
                "bool1": false
            },
            "type": "profile_set"
        }

    def do_test(self, servers, count, list_count, proportion=0):
        cnt = int(count * 4 / list_count)
        print("开始导入老用户 profile_set(20个用户属性 version=2.0) 数据, 数据量={}".format(count * 4))
        with open(self.file_name, 'r') as f:
            json_data = f.readlines()
        already_identities = [json.loads(line.strip()) for line in json_data]
        for i in range(cnt):
            test_data = self.make_profile_set_v2_more_old_user(list_count, already_identities)
            import_api(1, 1, test_data, servers[random.randint(0, len(servers) - 1)])
        del already_identities
        gc.collect()
        print("导入老用户 profile_set(20个用户属性 version=2.0) 数据完成")

    def make_profile_set_v2_more_old_user(self, count, already_identities):
        profile_set_list = []
        genders = ['男', '女', '未填写']
        first_visit_source_list = ['微信', 'QQ', '微博', '小红书']
        citys = ['上海', '深圳', '成都', '武汉', '杭州', '北京', '广州', '福州', '天津']
        careers = ['司机', '学生', '白领', '教师', '外卖员', '公务员', '无业']
        for i in range(count):
            profile_set_json = deepcopy(self.profile_set_v2_more)
            index = random.randint(0, len(already_identities) - 1)
            distinct_id = already_identities[index]['distinct_id']
            profile_set_json['distinct_id'] = distinct_id
            profile_set_json['properties']['account'] = 'account_' + str(int(time.time() * 1000000)) + str(
                random.randint(1000000, 9999999))
            profile_set_json['properties']['gender'] = genders[random.randint(0, len(genders) - 1)]
            profile_set_json['first_visit_source'] = first_visit_source_list[
                random.randint(0, len(first_visit_source_list) - 1)]
            profile_set_json['properties']['city'] = citys[random.randint(0, len(citys) - 1)]
            profile_set_json['properties']['birthday'] = datetime.date(random.randint(1900, 2021),
                                                                       random.randint(1, 12),
                                                                       random.randint(1, 28)).strftime('%Y-%m-%d')
            phone_prefix = random.choice(['133', '149', '153', '173', '177', '180', '181', '189', '191', '199'])
            phone_suffix = ''.join(random.choice('0123456789') for _ in range(8))
            profile_set_json['properties']['phone_number'] = phone_prefix + phone_suffix
            profile_set_json['properties']['career'] = careers[random.randint(0, len(careers) - 1)]

            profile_set_list.append(profile_set_json)
        return profile_set_list

    def collect_qps(self, data_count):
        qps_detail = collect_sdi_qps(data_count)
        qps_detail['title'] = "profile_set 老用户(20个用户属性, 属性产生变更)"
        return qps_detail
