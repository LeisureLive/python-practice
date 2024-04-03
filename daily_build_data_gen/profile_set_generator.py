import sys
import time
import uuid

from daily_build_data_gen.data_type import DataType
from daily_build_data_gen.prop_gen import gen_value

sys.path.append("..")


class ProfileSetGenerator:
    PROP_NAME_PREFIX = "property_event_"
    PROP_COUNT_MAP = {
        DataType.STRING: 10,
        DataType.NUMBER: 5,
        DataType.NUMBER_WITH_DOUBLE: 5,
        DataType.BOOL: 2,
        DataType.DATETIME: 1,
        DataType.LIST: 1
    }

    def __init__(self, idm_version, count, login_percent, new_user_percent, exist_identities) -> None:
        super().__init__()
        self.login_user_max_id = int(count) * login_percent
        self.new_login_user_max_id = self.login_user_max_id * new_user_percent
        self.new_not_login_user_max_id = self.login_user_max_id + (count - self.login_user_max_id) * new_user_percent
        self.exist_identities = exist_identities
        self.idm_version = idm_version
        # 必传
        self.schema = {"distinct_id": DataType.STRING}
        # properties
        prop_schema = {}
        for data_type, count in self.PROP_COUNT_MAP.items():
            for i in range(1, count + 1):
                prop_schema[self.PROP_NAME_PREFIX + str(data_type.name).lower() + str(i)] = data_type
        self.schema["properties"] = prop_schema

    def gen_data(self, row):
        ret = {}
        id = row['id']
        random_uuid = str(uuid.uuid4())
        current_timestamp = str(int(time.time() * 1000))
        is_old_user = False
        # 填充 distinct_id
        if id < self.new_login_user_max_id:
            # 登录新用户
            ret['login_id'] = 'login_id_' + random_uuid + current_timestamp
            ret['distinct_id'] = 'login_id_' + random_uuid + current_timestamp
            ret['anonymous_id'] = 'device_' + random_uuid + current_timestamp
        elif id < self.login_user_max_id:
            # 登录老用户
            ret['login_id'] = self.exist_identities[id]['login_id']
            ret['distinct_id'] = self.exist_identities[id]['distinct_id']
            ret['anonymous_id'] = self.exist_identities[id]['anonymous_id']
            is_old_user = True
        elif id < self.new_not_login_user_max_id:
            # 匿名新用户
            ret['distinct_id'] = 'device_' + random_uuid + current_timestamp
        else:
            # 匿名老用户
            ret['distinct_id'] = self.exist_identities[id]['distinct_id']
            is_old_user = True

        # 填充 identity
        if self.idm_version == 'id3' and is_old_user:
            ret['identities'] = {}
            ret['identities']['$identity_login_id'] = self.exist_identities[id]['identities']['$identity_login_id']
            ret['identities']['$identity_cookie_id'] = self.exist_identities[id]['identities']['$identity_cookie_id']
            ret['identities']['$identity_mobile'] = self.exist_identities[id]['identities']['$identity_mobile']
            ret['identities']['$identity_idfv'] = self.exist_identities[id]['identities']['$identity_idfv']
            ret['identities']['$identity_email'] = self.exist_identities[id]['identities']['$identity_email']
            ret['identities']['$identity_taobao_ouid'] = self.exist_identities[id]['identities'][
                '$identity_taobao_ouid']
        elif self.idm_version == 'id3' and is_old_user is False:
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
                    properties[name] = gen_value(name, data_type)
        ret['properties'] = properties
        ret['type'] = "profile_set"
        return ret

    def filter_identiy_info(self, row):
        print(f"row {row}")
        del row['properties']
        return row
