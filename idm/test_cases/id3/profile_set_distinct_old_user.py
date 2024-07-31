import sys

from idm.test_cases.test_case import TestCase
from idm.tools.common_tools import collect_sdi_qps
from idm.tools.spark_job import start_spark_job

sys.path.append("../..")

false = False
true = True


class IdmProfileSetV3DistinctOldUserCase(TestCase):

    def __init__(self):
        super().__init__()
        self.work_path = "/home/sa_cluster/import_data_benchmark"
        self.script_path = "daily_build_data_gen"
        self.basic_data_path = f"hdfs:///sa/runtime/daily_benchmark_basic_data/id3/anonymous_old_profile_set"
        self.data_type = "profile_set"
        self.cost = 0

    def do_test(self, basic_data_ip, target_ips, project, count):
        count = count * 3
        print("开始导入 profile_set(老用户 125 个属性 version=3.0) 数据, 数据量={}".format(count))
        start_spark_job(basic_data_ip, self.work_path, self.script_path, "IdmProfileSetV3DistinctOldUserCase", 'id3',
                        target_ips, project, self.basic_data_path, self.data_type)
        print("导入 profile_set(老用户, 125 个属性 version=3.0) 数据完成, 数据量={}".format(count))

    def collect_qps(self, exec_ip, data_count):
        qps_detail = collect_sdi_qps(exec_ip, data_count)
        qps_detail['title'] = "profile_set (老用户, 6个identity_id, 125个属性)"
        return qps_detail
