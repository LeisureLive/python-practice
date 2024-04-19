import json
import sys
import uuid

from daily_build_data_gen import data_utils
from daily_build_data_gen.data_type import DataType
from daily_build_data_gen.prop_gen import gen_value

sys.path.append("..")


class UserGen:
    PROP_NAME_PREFIX = "property_user_"
    PROP_COUNT_MAP = {
        DataType.STRING: 60,
        DataType.NUMBER: 30,
        DataType.NUMBER_WITH_DOUBLE: 20,
        DataType.BOOL: 5,
        DataType.DATETIME: 5,
        DataType.LIST: 5
    }

    def __init__(self, idm_version, count, login_percent) -> None:
        super().__init__()
        self.user_count = count
        self.login_user_max_id = int(int(count) * login_percent)
        self.idm_version = idm_version
        # 必传
        self.schema = {
            "type": DataType.STRING,
            "distinct_id": DataType.STRING,
            "anonymous_id": DataType.STRING
        }
        if idm_version == 'id2':
            self.schema["login_id"] = DataType.STRING
        else:
            self.schema["identities"] = {
                "$identity_login_id": DataType.STRING,
                "$identity_cookie_id": DataType.STRING,
                "$identity_mobile": DataType.STRING,
                "$identity_idfv": DataType.STRING,
                "$identity_email": DataType.STRING,
                "$identity_taobao_ouid": DataType.STRING
            }
        # properties
        prop_schema = {}
        for data_type, count in self.PROP_COUNT_MAP.items():
            for i in range(1, count + 1):
                prop_schema[self.PROP_NAME_PREFIX + str(data_type.name).lower() + str(i)] = data_type
        self.schema["properties"] = prop_schema

    def append_identities(self, row):
        random_uuid = str(uuid.uuid4())
        row['identities'] = {}
        row['identities']['$identity_login_id'] = 'login_id_' + random_uuid
        row['identities']['$identity_cookie_id'] = 'cookie_id_' + random_uuid
        row['identities']['$identity_mobile'] = 'mobile_' + random_uuid
        row['identities']['$identity_idfv'] = 'device_' + random_uuid
        row['identities']['$identity_email'] = 'email_' + random_uuid
        row['identities']['$identity_taobao_ouid'] = 'taobao_ouid_' + random_uuid
        return row

    def gen_data(self, row):
        id = row['id']
        random_uuid = str(uuid.uuid4())
        ret = {}
        ret['id'] = row['id']
        # 填充 distinct_id
        if id < self.login_user_max_id:
            # 登录用户
            ret['login_id'] = 'login_id_' + random_uuid
            ret['distinct_id'] = 'login_id_' + random_uuid
            ret['anonymous_id'] = 'device_' + random_uuid
        else:
            # 匿名新用户
            ret['distinct_id'] = 'device_' + random_uuid
            ret['anonymous_id'] = 'device_' + random_uuid

        # 填充 identity
        if self.idm_version == 'id3':
            ret['identities'] = {}
            ret['identities']['$identity_login_id'] = 'login_id_' + random_uuid
            ret['identities']['$identity_cookie_id'] = 'cookie_id_' + random_uuid
            ret['identities']['$identity_mobile'] = 'mobile_' + random_uuid
            ret['identities']['$identity_idfv'] = 'device_' + random_uuid
            ret['identities']['$identity_email'] = 'email_' + random_uuid
            ret['identities']['$identity_taobao_ouid'] = 'taobao_ouid_' + random_uuid

        # 填充 properties
        properties = {}
        for key, value in self.schema.items():
            if key == "properties":
                for name, data_type in value.items():
                    properties[name] = gen_value(name, data_type)
        ret['properties'] = properties
        ret['type'] = "profile_set"
        return ret

    def gen_old_data(self, row, data_times):
        ret = []
        user_id = row['id']
        if user_id > self.user_count:
            return ret
        for i in range(0, data_times):
            element = {
                "id": i * self.user_count + user_id,
                "anonymous_id": row['anonymous_id'],
                "distinct_id": row['distinct_id'],
                "type": "profile_set"
            }

            self._put_prop_if_exists(element, "login_id", row.get('login_id'))
            if 'identities' in row:
                identites = row['identities']
                element['identities'] = {}
                self._put_prop_if_exists(element['identities'], "$identity_anonymous_id",
                                         identites.get('$identity_anonymous_id'))
                self._put_prop_if_exists(element['identities'], "$identity_login_id",
                                         identites.get('$identity_login_id'))
                self._put_prop_if_exists(element['identities'], "$identity_cookie_id",
                                         identites.get('$identity_cookie_id'))
                self._put_prop_if_exists(element['identities'], "$identity_mobile", identites.get('$identity_mobile'))
                self._put_prop_if_exists(element['identities'], "$identity_idfv", identites.get('$identity_idfv'))
                self._put_prop_if_exists(element['identities'], "$identity_email", identites.get('$identity_email'))
                self._put_prop_if_exists(element['identities'], "$identity_taobao_ouid",
                                         identites.get('$identity_taobao_ouid'))
            # 填充 properties
            properties = {}
            for key, value in self.schema.items():
                if key == "properties":
                    for name, data_type in value.items():
                        properties[name] = gen_value(name, data_type)
            element['properties'] = properties
            ret.append(element)
        return ret

    def gen_bind_login_data(self, row, data_times):
        user_id = row['id']
        base_count = int(self.user_count / data_times)
        suffix = str(int(user_id % base_count))
        row['login_id'] = 'login_id_muti_case_' + suffix
        row['distinct_id'] = 'login_id_muti_case_' + suffix
        return row

    def _put_prop_if_exists(self, dict, key, value):
        if value is not None:
            dict[key] = value

    def get_id3_spark_csv_schema(self):
        return data_utils.convert_eu_schema_to_spark_csv_schema(self.schema)

    def convert_to_csv(self, row):
        row['properties'] = json.dumps(row['properties'], ensure_ascii=False)
        if 'identities' in row:
            row['identities'] = json.dumps(row['identities'], ensure_ascii=False)
        return row

    def filter_identity_info(self, row):
        del row['properties']
        del row['type']
        return row
