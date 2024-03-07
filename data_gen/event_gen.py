import json
import random
import time
import uuid

from pyspark import Row

import data_gen.prop_gen
import data_gen.data_utils
from data_gen.data_type import DataType

class EventGen:
    PROP_NAME_PREFIX = "property_event_"
    PROP_COUNT_MAP = {
        DataType.STRING: 12,
        DataType.NUMBER: 5,
        DataType.NUMBER_WITH_DOUBLE: 5,
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

    def __init__(self, user_count, event_count) -> None:
        super().__init__()
        # 每条 user 产出几条 event
        self.user_count = user_count
        self.event_count = event_count
        self.data_times = int(event_count / user_count)
        if self.data_times < 1:
            self.data_times = 1
        self.schema = {}
        self.schema["id"] = DataType.NUMBER
        # 必传
        self.schema["event"] = DataType.STRING
        self.schema["time"] = DataType.NUMBER
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
                for event_name, index in self.EVENT_PROP_INDEX_MAP.items():
                    prop_schema[self.PROP_NAME_PREFIX + str(data_type.name).lower() + "_" + str(i) + "_" + str(index)] = data_type
        self.schema["properties"] = prop_schema

    def get_id3_spark_schema(self):
        return data_gen.data_utils.convert_eu_schema_to_spark_schema(self.schema)

    def get_id3_spark_csv_schema(self):
        return data_gen.data_utils.convert_eu_schema_to_spark_csv_schema(self.schema)

    def convert_to_csv(self, row):
        row['identities'] = json.dumps(row['identities'], ensure_ascii=False)
        row['properties'] = json.dumps(row['properties'], ensure_ascii=False)
        return row

    def gen_data(self, row):
        ret = []
        user_id = row['id']
        if user_id > self.event_count:
            return ret
        for i in range(0, self.data_times):
            event = random.choice(list(self.EVENT_PROP_INDEX_MAP.keys()))
            prop_index = self.EVENT_PROP_INDEX_MAP[event]
            element = {
                "id": i * self.user_count + user_id,
                "event": event,
                "time": int(time.time() * 1000),
                "anonymous_id": row['anonymous_id'],
            }
            identites = row['identities']
            self._put_prop_if_exists(element, "login_id", row.get('login_id'))
            element['identities'] = {}
            self._put_prop_if_exists(element['identities'], "$identity_anonymous_id", identites.get('$identity_anonymous_id'))
            self._put_prop_if_exists(element['identities'], "$identity_login_id", identites.get('$identity_login_id'))
            self._put_prop_if_exists(element['identities'], "$identity_cookie_id", identites.get('$identity_cookie_id'))
            self._put_prop_if_exists(element['identities'], "$identity_mobile", identites.get('$identity_mobile'))
            self._put_prop_if_exists(element['identities'], "$identity_idfv", identites.get('$identity_idfv'))

            properties = {}
            for key, value in self.schema.items():
                if key == "properties":
                    for name, data_type in value.items():
                        if self._check_property_in_event(prop_index, name):
                            properties[name] = data_gen.prop_gen.gen_value(name, data_type)
            element['properties'] = properties
            ret.append(element)
        return ret

    def _put_prop_if_exists(self, dict, key, value):
        if value is not None:
            dict[key] = value

    def _check_property_in_event(self, prop_index, property_name):
        return property_name.endswith("_" + str(prop_index))


if __name__ == '__main__':
    row1 = {'id': 89, 'login_id': 'login_cec9dfc7-44df-4f2c-8302-1c456e7f159a',
     'identities': {'$identity_idfv': 'email_cec9dfc7-44df-4f2c-8302-1c456e7f159a',
                    '$identity_cookie_id': 'cookie_cec9dfc7-44df-4f2c-8302-1c456e7f159a',
                    '$identity_anonymous_id': 'email_cec9dfc7-44df-4f2c-8302-1c456e7f159a'},
     'anonymous_id': 'login_cec9dfc7-44df-4f2c-8302-1c456e7f159a',
     'properties': {'property_user_string1': 'M 端', 'property_user_string2': '自助', 'property_user_string3': '百安居'}}
    row2 = {'id': 90, 'identities': {'$identity_mobile': 'mobile_a8bdff69-a828-4f91-9ae6-be5f61865c04',
                              '$identity_idfv': 'email_a8bdff69-a828-4f91-9ae6-be5f61865c04',
                              '$identity_anonymous_id': 'mobile_a8bdff69-a828-4f91-9ae6-be5f61865c04'},
     'anonymous_id': 'mobile_a8bdff69-a828-4f91-9ae6-be5f61865c04',
     'properties': {'property_user_list1': ['9ad40b5f-1762-4d17-9666-8707848a7f27']}}
    generator = EventGen(100, 89)
    print(generator.gen_data(row1))
    print(generator.gen_data(row2))