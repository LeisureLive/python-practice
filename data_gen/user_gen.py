import json
import random
import uuid

from pyspark import Row

import data_gen.prop_gen
import data_gen.data_utils
from data_gen.data_type import DataType


class UserGen:
    PROP_NAME_PREFIX = "property_user_"
    PROP_COUNT_MAP = {
        DataType.STRING: 50,
        DataType.NUMBER: 30,
        DataType.NUMBER_WITH_DOUBLE: 10,
        DataType.BOOL: 4,
        DataType.DATETIME: 4,
        DataType.LIST: 2
    }

    def __init__(self, user_count, login_percent, cookie_percent, mobile_percent, idfv_percent) -> None:
        super().__init__()
        self.has_login_max_id = int(user_count) * login_percent
        self.cookie_percent = cookie_percent
        self.mobile_percent = mobile_percent
        self.idfv_percent = idfv_percent

        self.schema = {}
        self.schema["id"] = DataType.NUMBER
        # 必传
        self.schema["anonymous_id"] = DataType.STRING
        self.schema["login_id"] = DataType.STRING
        # id3
        self.schema["identities"] = {
            "$identity_anonymous_id": DataType.STRING,
            "$identity_login_id": DataType.STRING,
            "$identity_cookie_id": DataType.STRING,
            "$identity_mobile": DataType.STRING,
            "$identity_idfv": DataType.STRING
        }
        # properties
        prop_schema={}
        for data_type, count in self.PROP_COUNT_MAP.items():
            for i in range(1, count+1):
                prop_schema[self.PROP_NAME_PREFIX + str(data_type.name).lower() + str(i)] = data_type
        self.schema["properties"] = prop_schema


    def get_id3_spark_schema(self):
        return data_gen.data_utils.convert_eu_schema_to_spark_schema(self.schema)

    def get_id3_spark_csv_schema(self):
        return data_gen.data_utils.convert_eu_schema_to_spark_csv_schema(self.schema)

    def convert_to_csv(self, row):
        row['identities'] = json.dumps(row['identities'], ensure_ascii=False)
        row['properties'] = json.dumps(row['properties'], ensure_ascii=False)
        return row

    def coalesce(self, *args):
        for arg in args:
            if arg is not None and len(arg) > 0:
                return arg
        return "None"

    def gen_data(self, row):
        ret = {}
        # 固有字段
        ret['id'] = row['id']
        random_uuid = str(uuid.uuid4())
        # 填充 login_id
        if ret['id'] < self.has_login_max_id:
            ret['login_id'] = "login_" + random_uuid
        # 填充 identity
        ret['identities'] = {}
        if random.random() < self.mobile_percent:
            ret['identities']['$identity_mobile'] = "mobile_" + random_uuid
        if random.random() < self.idfv_percent:
            ret['identities']['$identity_idfv'] = "device_" + random_uuid
        if random.random() < self.cookie_percent:
            ret['identities']['$identity_cookie_id'] = "cookie_" + random_uuid
        # 填充 $identity_anonymous_id
        ret['identities']['$identity_anonymous_id'] = self.coalesce(ret['identities'].get('$identity_mobile'),
                                                                    ret['identities'].get('$identity_idfv'),
                                                                    ret['identities'].get('$identity_cookie_id'),
                                                                    "anonymous_" + random_uuid)
        # 填充 distinct_id
        ret['anonymous_id'] = ret['identities']['$identity_anonymous_id']

        properties = {}
        for key, value in self.schema.items():
            if key == "properties":
                for name, data_type in value.items():
                    properties[name] = data_gen.prop_gen.gen_value(name, data_type)
        ret['properties'] = properties

        return ret


if __name__ == '__main__':
    generator = UserGen(100, 1,1,1,1)
    for i in range(0, 100):
        row = Row(id=i)
        data1 = generator.gen_data(row)
        print(data1)
        print(json.dumps(data1))
        # print(generator.convert_to_csv(data1))
