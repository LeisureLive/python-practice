import datetime
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
from idm.tools.common_tools import collect_sdi_qps, import_api, exec_importer

false = False
true = True


class IdmProfileSetV2DistinctNewUserMorePropsCase(TestCase):

    def __init__(self, build_user, identification):
        super().__init__()
        self.file_name = "profile_set_v2_more_anonymous_{}_{}.json".format(build_user, identification)
        self.import_file_name = "IdmProfileSetV2DistinctNewUserMorePropsCase_{}_{}_importer.json".format(build_user, identification)
        self.cost = 0
        self.profile_set_v2_more = {
            "distinct_id": "",
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
                "bool1": false,
                "number_int1": 1130,
                "number_float1": 4000.222,
                "number_negative1": -1000,
                "datetime2": "1984-10-07",
                "string2": "15159682272",
                "bool2": false,
                "number_int2": 6668,
                "number_float2": 5000.3333,
                "number_negative2": -5000,
                "datetime3": "1978-01-18",
                "string3": "15778596480",
                "bool3": true,
                "number_int3": 4162,
                "number_float3": 4000.222,
                "number_negative3": -2000,
                "datetime4": "1986-03-12",
                "string4": "14752420921",
                "bool4": true,
                "number_int4": 5024,
                "number_float4": 3000.111,
                "number_negative4": -3000.1,
                "datetime5": "2007-02-13",
                "string5": "18321290947",
                "bool5": true,
                "number_int5": 9244,
                "number_float5": 2000.11,
                "number_negative5": -4000,
                "datetime6": "2022-07-18",
                "string6": "15293881573",
                "bool6": true,
                "number_int6": 5352,
                "number_float6": 4000.222,
                "number_negative6": -4000,
                "datetime7": "2021-09-12",
                "string7": "15049019846",
                "bool7": true,
                "number_int7": 4076,
                "number_float7": 3000.111,
                "number_negative7": -3000.1,
                "datetime8": "1980-04-08",
                "string8": "18472549637",
                "bool8": true,
                "number_int8": 3307,
                "number_float8": 5000.3333,
                "number_negative8": -5000,
                "datetime9": "2022-04-04",
                "string9": "18398768766",
                "bool9": true,
                "number_int9": 7309,
                "number_float9": 1000.11,
                "number_negative9": -5000,
                "datetime10": "1984-06-27",
                "string10": "17204453037",
                "bool10": true,
                "number_int10": 2902,
                "number_float10": 3000.111,
                "number_negative10": -3000.1,
                "datetime12": "2001-02-04",
                "string12": "15039437400",
                "bool12": false,
                "number_int12": 8588,
                "number_float12": 4000.222,
                "number_negative12": -4000,
                "datetime13": "2015-06-24",
                "string13": "13727280369",
                "bool13": false,
                "number_int13": 4518,
                "number_float13": 2000.11,
                "number_negative13": -5000,
                "datetime14": "1974-05-15",
                "string14": "17828452917",
                "bool14": false,
                "number_int14": 9021,
                "number_float14": 3000.111,
                "number_negative14": -2000,
                "datetime15": "2007-04-13",
                "string15": "15943246925",
                "bool15": true,
                "number_int15": 5549,
                "number_float15": 3000.111,
                "number_negative15": -2000,
                "datetime16": "1983-11-29",
                "string16": "19828614739",
                "bool16": false,
                "number_int16": 9202,
                "number_float16": 5000.3333,
                "number_negative16": -1000,
                "datetime17": "1981-08-24",
                "string17": "13403321669",
                "bool17": false,
                "number_int17": 7647,
                "number_float17": 4000.222,
                "number_negative17": -2000,
                "datetime18": "2016-08-03",
                "string18": "13997402565",
                "bool18": false,
                "number_int18": 7184,
                "number_float18": 3000.111,
                "number_negative18": -5000,
                "datetime19": "2007-04-25",
                "string19": "13824988884",
                "bool19": false,
                "number_int19": 8782,
                "number_float19": 3000.111,
                "number_negative19": -1000
            },
            "type": "profile_set"
        }

        self.profile_set_v2_identities = {
            "distinct_id": ""
        }

    def do_test(self, servers, count, list_count, proportion=0):
        print("开始导入 profile_set(匿名新用户 125 个属性 version=2.0) 数据, 数据量={}".format(count))
        if os.path.exists(self.file_name):
            print("文件 {} 存在，删除历史记录的用户信息".format(self.file_name))
            os.remove(self.file_name)
        else:
            print("文件 {} 不存在, 记录用户信息到此文件".format(self.file_name))
        # 单个并发最多导 100w 数据
        if count % 1000000 == 0:
            concurrent_num = int(count / 1000000)
        else:
            concurrent_num = int(count / 1000000) + 1
        avg_count = int(count / concurrent_num)
        futures = []
        with ThreadPoolExecutor(max_workers=concurrent_num) as executor:
            for i in range(concurrent_num):
                future = executor.submit(self.run_make_records, servers, avg_count, list_count, i)
                futures.append(future)
        total_count = 0
        for future in futures:
            total_count += future.result()
        print("导入 profile_set(匿名新用户, 125 个属性 version=2.0) 数据完成, 数据量={}".format(total_count))

    def run_make_records(self, servers, count, list_count, concurrent_index):
        print("导入 profile_set(匿名新用户, 125 个属性 version=2.0) 数据, 并发序号={}, 导入量={}".format(concurrent_index, count))
        cnt = int(count / list_count)
        for i in range(cnt):
            test_data = self.make_profile_set_v2_more(list_count, concurrent_index)
            import_api(1, 1, test_data, servers[random.randint(0, len(servers) - 1)])
        return count

    def make_profile_set_v2_more(self, count, concurrent_index):
        profile_set_list = []
        profile_set_identity_list = []
        genders = ['男', '女', '未填写']
        first_visit_source_list = ['微信', 'QQ', '微博', '小红书']
        citys = ['上海', '深圳', '成都', '武汉', '杭州', '北京', '广州', '福州', '天津']
        careers = ['司机', '学生', '白领', '教师', '外卖员', '公务员', '无业']
        for i in range(count):
            profile_set_json = deepcopy(self.profile_set_v2_more)
            profile_set_identity_json = deepcopy(self.profile_set_v2_identities)
            device_id = str(uuid.uuid4()) + str(int(time.time() * 1000000)) + '_' + str(
                random.randint(1000000, 9999999)) + str(concurrent_index)
            profile_set_identity_json['distinct_id'] = device_id
            profile_set_identity_list.append(profile_set_identity_json)

            profile_set_json['distinct_id'] = device_id
            profile_set_json['properties']['$ip'] = "10.129.29." + str(random.randint(1, 255))
            profile_set_json['properties']['gender'] = genders[random.randint(0, len(genders) - 1)]
            profile_set_json['properties']['first_visit_source'] = first_visit_source_list[
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
        with open(self.file_name, 'a') as f:
            for item in profile_set_identity_list:
                f.write(json.dumps(item) + '\n')
        return profile_set_list

    def collect_qps(self, exec_ip, data_count):
        qps_detail = collect_sdi_qps(exec_ip, data_count)
        qps_detail['title'] = "profile_set (匿名新用户, 125个属性)"
        return qps_detail

    def do_import_test(self, exec_ip, project_name, count, import_mode):
        self.clean_path(self.file_name)
        self.clean_path(self.import_file_name)
        with open(self.import_file_name, "w") as f:
            for i in range(0, count):
                ret = self.make_profile_set_v2_more(1, i)
                f.write(json.dumps(ret[0]) + '\n')

        self.cost = exec_importer(exec_ip, project_name, self.import_file_name, import_mode)
        print(self.cost)

    def collect_import_qps(self, count):
        qps_detail = {}
        qps_detail['title'] = "profile_set (匿名新用户, 125个属性)-importer"
        qps_detail['avg_qps'] = count / self.cost
        return qps_detail