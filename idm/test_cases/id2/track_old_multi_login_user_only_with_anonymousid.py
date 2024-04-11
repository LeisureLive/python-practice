import sys

from idm.test_cases.test_case import TestCase
from idm.tools.common_tools import collect_sdi_qps
from idm.tools.spark_job import start_spark_job

sys.path.append("../..")

false = False
true = True


class IdmTrackMultiLoginUserOnlyWithAnonymousIdCase(TestCase):

    def __init__(self, build_user, identification):
        super().__init__()
        self.work_path = "/home/sa_cluster/import_data_benchmark"
        self.script_path = "daily_build_data_gen"
        # 多对一绑定的设备id个数
        self.device_id_list_size = 1
        self.login_percent = 0.0
        self.new_user_percent = 0.0
        self.input_file_dir = "track_v2_login_multi_user_{}_{}.json".format(build_user, identification)
        self.output_file_dir = ""
        self.cost = 0

    def do_test(self, exec_ip, ips, project, count):
        print("开始导入 track(登录老用户匿名上报 version=2.0) 数据, 数据量={}".format(count))
        start_spark_job(exec_ip, self.work_path, self.script_path, "IdmTrackMultiLoginUserOnlyWithAnonymousIdCase",
                        'id2', ips, project, 0, count, self.login_percent, self.new_user_percent,
                        self.device_id_list_size, self.input_file_dir, self.output_file_dir)
        print("导入 track(登录老用户匿名上报 version=2.0)  数据完成, 数据量={}".format(count))

    def collect_qps(self, exec_ip, data_count):
        qps_detail = collect_sdi_qps(exec_ip, data_count)
        qps_detail['title'] = "track (登录老用户匿名上报)"
        return qps_detail
