import sys

from idm.cases.test_case import TestCase
from idm.tools.common_tools import collect_sdi_qps
from idm.tools.spark_job import start_spark_job

sys.path.append("../..")

false = False
true = True


class IdmProfileSetV2DistinctNewUserCase(TestCase):

    def __init__(self, build_user, identification):
        super().__init__()
        self.work_path = "/home/sa_cluster/import_data_benchmark"
        self.script_path = "daily_build_data_gen"
        self.output_file_dir = "profile_set_v2_anonymous_user_{}_{}".format(build_user, identification)
        self.import_file_name = "IdmProfileSetV2DistinctNewUserMorePropsCase_{}_{}_importer.json".format(build_user,
                                                                                                         identification)
        self.cost = 0

    def do_test(self, exec_ip, ips, project, count):
        print("开始导入 profile_set(匿名新用户 100 个属性 version=2.0) 数据, 数据量={}".format(count))
        start_spark_job(exec_ip, self.work_path, self.script_path, "IdmProfileSetV2DistinctNewUserCase", 'id2', ips,
                        project, count, 0, 0.0, 1.0, "", self.output_file_dir)
        print("导入 profile_set(匿名新用户, 100 个属性 version=2.0) 数据完成, 数据量={}".format(count))

    def collect_qps(self, exec_ip, data_count):
        qps_detail = collect_sdi_qps(exec_ip, data_count)
        qps_detail['title'] = "profile_set (匿名新用户, 100个属性)"
        return qps_detail
