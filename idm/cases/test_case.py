import os
class TestCase:

    def __init__(self):
        pass

    def do_test(self, servers, count, list_count, proportion):
        pass

    def collect_qps(self, exec_ip, data_count):
        pass

    def do_import_test(self, exec_ip, project_name, count, import_mode):
        pass

    def collect_import_qps(self, count):
        pass

    def clean_path(self, file_name):
        if os.path.exists(file_name):
            print("文件 {} 存在，删除历史记录的用户信息".format(file_name))
            os.remove(file_name)
        else:
            print("文件 {} 不存在, 记录用户信息到此文件".format(file_name))