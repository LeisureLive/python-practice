import sys
import uuid

from daily_build_data_gen.data_type import DataType
from daily_build_data_gen.prop_gen import gen_value

sys.path.append("..")


class ProfileSetGenerator:
    PROP_NAME_PREFIX = "property_user_"
    PROP_COUNT_MAP = {
        DataType.STRING: 60,
        DataType.NUMBER: 30,
        DataType.NUMBER_WITH_DOUBLE: 20,
        DataType.BOOL: 5,
        DataType.DATETIME: 5,
        DataType.LIST: 5
    }

    def __init__(self, idm_version, count, login_percent, new_user_percent, device_id_list_size,
                 exist_identities) -> None:
        super().__init__()
        self.login_user_max_id = int(int(count) * login_percent)
        self.login_new_user_max_id = int(self.login_user_max_id * new_user_percent)
        self.not_login_new_user_max_id = \
            self.login_user_max_id + int((count - self.login_user_max_id) * new_user_percent)
        self.device_id_list_size = device_id_list_size
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
        index = 0
        if len(self.exist_identities) > 0:
            index = id % len(self.exist_identities)
        random_uuid = str(uuid.uuid4())
        is_old_user = False
        # 填充 distinct_id
        if id < self.login_new_user_max_id:
            # 登录新用户
            ret['login_id'] = 'login_id_' + random_uuid
            ret['distinct_id'] = 'login_id_' + random_uuid
            ret['anonymous_id'] = 'device_' + random_uuid
        elif id < self.login_user_max_id:
            # 登录老用户
            exist_identity = self.exist_identities[index]
            if 'login_id' not in exist_identity:
                # 如果输入的用户是匿名用户, 那生成新 login_id 给已存在的匿名用户，属于建立多对一关联
                suffix = int(id / self.device_id_list_size)
                login_id = 'login_id_' + str(suffix) + '_' + str(self.device_id_list_size)
                ret['login_id'] = login_id
                ret['distinct_id'] = login_id
                ret['anonymous_id'] = self.exist_identities[index]['distinct_id']
            else:
                ret['login_id'] = exist_identity['login_id']
                ret['distinct_id'] = exist_identity['distinct_id']
                ret['anonymous_id'] = exist_identity['anonymous_id']
            is_old_user = True
        elif id < self.not_login_new_user_max_id:
            # 匿名新用户
            ret['distinct_id'] = 'device_' + random_uuid
            ret['anonymous_id'] = 'device_' + random_uuid
        else:
            # 匿名老用户
            ret['distinct_id'] = self.exist_identities[index]['distinct_id']
            ret['anonymous_id'] = self.exist_identities[index]['anonymous_id']
            is_old_user = True

        # 填充 identity
        if self.idm_version == 'id3' and is_old_user:
            exist_identity = self.exist_identities[index]
            ret['identities'] = {}
            ret['identities']['$identity_login_id'] = exist_identity['identities']['$identity_login_id']
            ret['identities']['$identity_cookie_id'] = exist_identity['identities']['$identity_cookie_id']
            ret['identities']['$identity_mobile'] = exist_identity['identities']['$identity_mobile']
            ret['identities']['$identity_idfv'] = exist_identity['identities']['$identity_idfv']
            ret['identities']['$identity_email'] = exist_identity['identities']['$identity_email']
            ret['identities']['$identity_taobao_ouid'] = exist_identity['identities']['$identity_taobao_ouid']
        elif self.idm_version == 'id3' and is_old_user is False:
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

    def filter_identity_info(self, row):
        del row['properties']
        del row['type']
        return row
