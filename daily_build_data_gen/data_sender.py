import base64
import gzip
import json
import random
import urllib

import requests


class DataSender:

    def __init__(self, server_list):
        self.server_list = server_list

    def batch_send_to_import_api(self, partition):
        batch_size = 100
        batch = []
        for item in partition:
            batch.append(item)
            if len(batch) == batch_size:
                # 攒满一批进行上报
                self.import_api(1, 1, batch, self.server_list[random.randint(0, len(self.server_list) - 1)])
                batch = []
        # 处理剩余的
        if batch:
            self.import_api(1, 1, batch, self.server_list[random.randint(0, len(self.server_list) - 1)])
        return partition

    def import_api(self, gzipType, dataType, jsonString, server):
        Udata = self.dealwith(gzipType, jsonString)
        if gzipType == 1 and dataType == 1:
            payload = "gzip=1&data_list=%s" % Udata
        elif gzipType == 1 and dataType == 0:
            payload = "gzip=1&data=%s" % Udata
        elif gzipType == 0 and dataType == 1:
            payload = "data_list=%s" % Udata
        elif gzipType == 0 and dataType == 0:
            payload = "data=%s" % Udata
        else:
            print("非法参数！！")
        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'Connection': "close"
        }
        s = requests.session()
        s.keep_alive = False
        response = requests.post(server, data=payload, headers=headers)
        return len(jsonString)

    def dealwith(self, gzipType, jsonString):
        data = json.dumps(jsonString, ensure_ascii=False)
        # print("json: "+data)
        data = data.encode('utf-8')
        if gzipType == 1:
            # gzip压缩
            data = gzip.compress(data)
        Bdata = base64.b64encode(data)
        Udata = urllib.parse.quote(Bdata)
        return Udata
