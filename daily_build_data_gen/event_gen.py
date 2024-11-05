import json
import random
import sys
import time
import uuid

sys.path.append("..")
from daily_build_data_gen import data_utils, prop_gen
from daily_build_data_gen.data_type import DataType
from daily_build_data_gen.prop_gen import gen_value


class EventGen:
    PROP_NAME_PREFIX = "property_event_"
    PROP_COUNT_MAP = {
        DataType.STRING: 10,
        DataType.NUMBER: 5,
        DataType.NUMBER_WITH_DOUBLE: 3,
        DataType.BOOL: 3,
        DataType.DATETIME: 3,
        DataType.LIST: 1,
    }
    EVENT_PROP_INDEX_MAP = {
        "WMemberPayResult": 1,
        "CouponDistribution": 2,
        "CouponVerification": 3,
        "OrderStatusChange": 4,
        "OrderStatusChangeDetail": 5
    }

    def __init__(self, idm_version, count, login_percent) -> None:
        super().__init__()
        self.event_count = count
        self.login_user_max_id = int(count) * login_percent
        self.idm_version = idm_version
        # 必传
        self.schema = {
            "type": DataType.STRING,
            "event": DataType.STRING,
            "time": DataType.NUMBER,
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
                for event_name, index in self.EVENT_PROP_INDEX_MAP.items():
                    prop_schema[self.PROP_NAME_PREFIX + str(data_type.name).lower() + "_" + str(i) + "_" + str(
                        index)] = data_type
        self.schema["properties"] = prop_schema

    def get_id3_spark_csv_schema(self):
        return data_utils.convert_eu_schema_to_spark_csv_schema(self.schema)

    def convert_to_csv(self, row):
        row['properties'] = json.dumps(row['properties'], ensure_ascii=False)
        if 'identities' in row:
            row['identities'] = json.dumps(row['identities'], ensure_ascii=False)
        return row

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
        current_timestamp = str(time.time() * 1000)
        ret = {}
        ret['id'] = row['id']
        # 填充 distinct_id
        if id < self.login_user_max_id:
            # 登录新用户
            ret['login_id'] = 'login_id_' + random_uuid + current_timestamp
            ret['distinct_id'] = 'login_id_' + random_uuid + current_timestamp
            ret['anonymous_id'] = 'device_' + random_uuid + current_timestamp
        else:
            # 匿名新用户
            ret['distinct_id'] = 'device_' + random_uuid + current_timestamp
            ret['anonymous_id'] = 'device_' + random_uuid + current_timestamp

        event = random.choice(list(self.EVENT_PROP_INDEX_MAP.keys()))
        prop_index = self.EVENT_PROP_INDEX_MAP[event]
        ret['event'] = event
        ret['time'] = int(time.time() * 1000)
        ret['type'] = "track"

        # 填充 identity
        if self.idm_version == 'id3':
            ret['identities'] = {}
            ret['identities']['$identity_login_id'] = 'login_id_' + random_uuid + current_timestamp
            ret['identities']['$identity_cookie_id'] = 'cookie_id_' + random_uuid + current_timestamp
            ret['identities']['$identity_mobile'] = 'mobile_' + random_uuid + current_timestamp
            ret['identities']['$identity_idfv'] = 'device_' + random_uuid + current_timestamp
            ret['identities']['$identity_email'] = 'email_' + random_uuid + current_timestamp
            ret['identities']['$identity_taobao_ouid'] = 'taobao_ouid_' + random_uuid + current_timestamp
        # 填充 properties
        properties = {}
        for key, value in self.schema.items():
            if key == "properties":
                for name, data_type in value.items():
                    if self._check_property_in_event(prop_index, name):
                        properties[name] = gen_value(name, data_type)
        ret['properties'] = properties
        return ret

    def gen_old_data(self, row):
        event = random.choice(list(self.EVENT_PROP_INDEX_MAP.keys()))
        row['event'] = event
        row['time'] = int(time.time() * 1000)
        prop_index = self.EVENT_PROP_INDEX_MAP[event]

        properties = {}
        for key, value in self.schema.items():
            if key == "properties":
                for name, data_type in value.items():
                    if self._check_property_in_event(prop_index, name):
                        properties[name] = prop_gen.gen_value(name, data_type)
        row['properties'] = properties
        return row

    def gen_bind_login_data(self, row, data_times):
        id = row['id']
        base_count = int(self.event_count / data_times)
        suffix = str(int(id % base_count))

        row['login_id'] = 'login_id_muti_case_' + suffix
        row['distinct_id'] = 'login_id_muti_case_' + suffix
        event = random.choice(list(self.EVENT_PROP_INDEX_MAP.keys()))
        prop_index = self.EVENT_PROP_INDEX_MAP[event]
        row['event'] = event
        row['time'] = int(time.time() * 1000)

        # 填充 properties
        properties = {}
        for key, value in self.schema.items():
            if key == "properties":
                for name, data_type in value.items():
                    if self._check_property_in_event(prop_index, name):
                        properties[name] = gen_value(name, data_type)
        row['properties'] = properties
        return row

    def _put_prop_if_exists(self, dict, key, value):
        if value is not None:
            dict[key] = value

    def _check_property_in_event(self, prop_index, property_name):
        return property_name.endswith("_" + str(prop_index))
