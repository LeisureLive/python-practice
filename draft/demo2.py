import ast
import re

from horizon_scout.common.precheck_step import PreCheckStep
from hyperion_client.deploy_topo import DeployTopo


class SubscriptionStep(PreCheckStep):

    def __init__(self):
        super().__init__()
        self.__project_ids = None

        # 是否需要迁移，有些环境没有装 infinity
        self.need_check = 'infinity' in DeployTopo().get_product_name_list()
        # 旧版本 inf 使用单表存储，新版本使用两张表
        self.is_new_version = self.is_table_exists('infinity_db', 'infinity_subscription_rule_define')

        # 匹配虚拟属性的 join 的表名和属性名
        self.virtual_field_pattern = re.compile(r'([$a-zA-Z_][$a-zA-Z#\d_]*)\.([$a-zA-Z_][$a-zA-Z#\d_]*)', re.I)

        # 匹配虚拟属性不支持订阅的 sql 表达式, 格式为 $.a.b
        self.not_subscribeable_virtual_field_pattern = re.compile(r'\$\.([a-zA-Z][a-zA-Z0-9]*)\.([a-zA-Z][a-zA-Z0-9]*)')

        # 匹配事件属性 event.event_name.field_name 结构
        self.event_field_pattern = r'event\.([a-zA-Z][a-zA-Z0-9]*)\.([a-zA-Z][a-zA-Z0-9]*)'

        # 老数据模型里，订阅的 filter 里包含该属性，元数据没有该属性，但又实际存在的列。SDH 元数据有。
        self.event_property_white_list = ['time', 'month_id', 'week_id', 'user_id', 'distinct_id', 'day']

        # project_id:subscription_type:list[subscription]
        self.all_subscription = self.__get_all_subscription()
        # project_id:table_type:list[name]
        self.all_field = self.__get_all_field()
        # project_id:table_type:dict{name:property}
        self.all_virtual_field = self.__get_all_virtual_field()

    def __get_check_item(self):
        if self.is_new_version:
            return {
                'profile_stream': {
                    'name': 'subscription_PROFILE_STREAM_check', 'description': '订阅用户数据存在跨表的虚拟属性',
                    'check_sql': '''
                select subscription.id,subscription.project_id,subscription.subscription_config,subscription.property,virtual.expression
                from (
                  select a.id,a.project_id,b.subscription_type,b.subscription_config,
                        replace(JSON_EXTRACT(b.subscription_config, '$.name'),'"','') as property
                  from ${infinity_db}.infinity_subscription_define a left join ${infinity_db}.infinity_subscription_rule_define b
                  on a.subscription_rule_id=b.id where a.is_in_use=1 and b.subscription_type="PROFILE_STREAM"
                  ) subscription
                left join ${sca_db}.external_property_define virtual
                on subscription.project_id=virtual.project_id and subscription.property=virtual.name
                where virtual.expression is not NULL
                ''',
                    'error_tip': '系统中以下用户订阅存在跨表属性。如果与商务和客户协商可以继续升级，将忽略本检查，并不迁移以下数据'
                },
                'event_stream': {
                    'name': 'subscription_EVENT_STREAM_check', 'description': '订阅事件数据存在不支持订阅的虚拟属性',
                    'check_sql': '''
                  select a.id,a.project_id,b.subscription_type,b.subscription_config,
                         replace(JSON_EXTRACT(b.subscription_config, '$.retain_property_names'),'"','') as retain_property_names,
                         replace(JSON_EXTRACT(b.subscription_config, '$**.field'),'"','') as fields
                  from ${infinity_db}.infinity_subscription_define a left join ${infinity_db}.infinity_subscription_rule_define b
                  on a.subscription_rule_id=b.id where a.is_in_use=1 and b.subscription_type="EVENT_STREAM"
                ''',
                    'error_tip': '系统中以下事件订阅包含不支持订阅的虚拟属性。如果与商务和客户协商可以继续升级，将忽略本检查，并不迁移以下数据'
                },
                'table_metadata_stream': {
                    'name': 'subscription_TABLE_METADATA_STREAM_check',
                    'description': 'table metatadata 订阅是否有不存在的表',
                    'check_sql': '''
                select a.id,a.project_id,b.subscription_config, replace(JSON_EXTRACT(b.subscription_config, '$.table_name'),'"','') as table_name
                  from ${infinity_db}.infinity_subscription_define a
                  left join ${infinity_db}.infinity_subscription_rule_define b
                  on a.subscription_rule_id=b.id
                  where a.is_in_use=1 and b.subscription_type="TABLE_METADATA_STREAM"
                ''',
                    'error_tip': '系统中以下 TABLE_METADATA 订阅了不存在的表。如果与商务和客户协商可以继续升级，将忽略本检查，并不迁移以下数据'
                }
            }
        else:
            return {
                'profile_stream': {
                    'name': 'subscription_PROFILE_STREAM_check',
                    'description': '订阅用户数据存在跨表的虚拟属性',
                    'check_sql': '''
                select subscription.id,subscription.project_id,subscription.subscription_config,replace(JSON_EXTRACT(subscription_config, '$.name'),'"','') as property,virtual.expression
                from ${infinity_db}.infinity_subscription_define subscription
                left join ${sca_db}.external_property_define virtual
                on subscription.project_id=virtual.project_id and replace(JSON_EXTRACT(subscription.subscription_config, '$.name'),'"','')=virtual.name
                where virtual.expression is not NULL and subscription.is_in_use=1
                ''',
                    'error_tip': '系统中以下用户订阅存在跨表属性。如果与商务和客户协商可以继续升级，将忽略本检查，并不迁移以下数据'
                },
                'event_stream': {
                    'name': 'subscription_EVENT_STREAM_check',
                    'description': '订阅事件数据存在包含不支持订阅的虚拟属性',
                    'check_sql': '''
                select id,project_id,subscription_config,
                replace(JSON_EXTRACT(subscription_config, '$.retain_property_names'),'"','') as retain_property_names,
                replace(JSON_EXTRACT(subscription_config, '$**.field'),'"','') as fields
                from ${infinity_db}.infinity_subscription_define
                where is_in_use=1
                ''',
                    'error_tip': '系统中以下事件订阅存在不支持订阅的虚拟属性。如果与商务和客户协商可以继续升级，将忽略本检查，并不迁移以下数据'
                },
                'table_metadata_stream': {
                    'name': 'subscription_TABLE_METADATA_STREAM_check',
                    'description': 'table metatadata 订阅是否有不存在的表',
                    'check_sql': '''
                select id,project_id,subscription_config, replace(JSON_EXTRACT(subscription_config, '$.table_name'),'"','') as table_name
                  from ${infinity_db}.infinity_subscription_define
                  where is_in_use=1 and subscription_type="TABLE_METADATA_STREAM"
                ''',
                    'error_tip': '系统中以下 TABLE_METADATA 订阅了不存在的表。如果与商务和客户协商可以继续升级，将忽略本检查，并不迁移以下数据'
                }
            }

    def do_precheck(self):
        self.logger.info('执行 subscription 前置检查')
        self.__project_ids = [p['id'] for p in self.list_all_enabled_projects()]

        if not self.need_check:
            return

        check_items = self.__get_check_item()

        self.__check_profile_stream_subscription(check_items.get('profile_stream'))
        self.__check_table_metadata_stream_subscription(check_items.get('table_metadata_stream'))
        self.__check_event_stream_subscription(check_items.get('event_stream'))
        self.__check_event_stream_filter()

    # 校验跨表 join 的订阅
    def __check_profile_stream_subscription(self, item: dict):
        results = self.__query_sql(item['check_sql'])
        if len(results) == 0:
            return self.check_tips_succeed('subscription', item['name'], item['description'])

        joined_subscription = []
        for subscription in results:
            if self.__check_joined_virtual_property(subscription['expression']):
                joined_subscription.append(subscription)

        if len(joined_subscription) > 0:
            self.check_tips_failed('subscription', item['name'], item['description'],
                                   self.__format_check_fail_message(item['error_tip'], joined_subscription))
        else:
            self.check_tips_succeed('subscription', item['name'], item['description'])

    # 是否是跨表 join 的属性
    def __check_joined_virtual_property(self, virtual_expression: str):
        result = self.virtual_field_pattern.findall(virtual_expression)
        if result is None:
            return False
        for match in result:
            if match[0] != 'events' and match[0] != 'users':
                return True
        return False

    def __check_event_stream_subscription(self, item: dict):
        results = self.__query_sql(item['check_sql'])
        if len(results) == 0:
            return self.check_tips_succeed('subscription', item['name'], item['description'])

        invalid_subscription = []
        for subscription in results:
            project_id = subscription['project_id']
            retain_property_names = ast.literal_eval(subscription['retain_property_names'])
            if subscription['fields'] is None:
                field_names = retain_property_names
            else:
                field_names = retain_property_names | ast.literal_eval(subscription['fields'])
            expressions = self.__list_expression_by_fields(project_id, field_names)
            if len(expressions) == 0:
                continue
            for expression in expressions:
                matchs = self.not_subscribeable_virtual_field_pattern.findall(expression)
                if matchs is not None:
                    invalid_subscription.append(subscription)

        if len(invalid_subscription) > 0:
            self.check_tips_failed('subscription', item['name'], item['description'],
                                   self.__format_check_fail_message(item['error_tip'], invalid_subscription))
        else:
            self.check_tips_succeed('subscription', item['name'], item['description'])

    def __list_expression_by_fields(self, project_id, fields: list):
        field_names = []
        for field in fields:
            match = re.match(self.event_field_pattern, field)
            if match:
                field_names.append(match.group(2))
            else:
                field_names.append(field)
        sql = "SELECT DISTINCT expression FROM ${sca_db}.external_property_define WHERE `name` IN (%s) AND project_id = %s"
        results = self.__query_sql(sql % (','.join(field_names), project_id))
        expressions = []
        if len(results) != 0:
            for result in results:
                expressions.append(result['expression'])
        return expressions

    # 检查 tag 和 segment 订阅，非阻塞项
    def __check_table_metadata_stream_subscription(self, item: dict):
        results = self.__query_sql(item['check_sql'])
        if len(results) == 0:
            return self.check_tips_succeed('subscription', item['name'], item['description'])

        # get exist tag
        exist_tag_dict = self.__get_exist_tag()

        not_exist_table_subscription = []
        for subscription in results:
            tag = subscription['table_name']
            if tag.startswith('user_tag'):
                tag = tag.replace('user_tag_', '', 1)
            else:
                tag = tag.replace('user_group_', '', 1)
            if tag not in exist_tag_dict.get(subscription['project_id'], []):
                not_exist_table_subscription.append(subscription)

        if len(not_exist_table_subscription) > 0:
            self.check_tips_failed('subscription', item['name'], item['description'],
                                   self.__format_check_fail_message(item['error_tip'], not_exist_table_subscription))
        else:
            self.check_tips_succeed('subscription', item['name'], item['description'])

    # 获取存在的 tag
    def __get_exist_tag(self):
        all_dict = {}
        results = self.query_sql('select project_id,name from ${sps_db}.sp_user_tag')
        for item in results:
            if item['project_id'] not in all_dict:
                all_dict[item['project_id']] = []
            all_dict[item['project_id']].append(item['name'])
        return all_dict

    def __query_sql(self, sql: str):
        sql = '\n'.join(['select * from (', sql,
                         ') table_001 where project_id in (%s)' % ','.join(str(i) for i in self.__project_ids)])
        return self.query_sql(sql)

    def __check_event_stream_filter(self):
        pattern = re.compile(r'\{"param_type":"FIELD","field":"(.*?)"}', re.I)

        table_type = 0
        filter_contain_not_exist_subscription = []
        event_subscription = self.all_subscription.get('EVENT_STREAM')

        if not event_subscription:
            return

        for sub in event_subscription:
            # 匹配所有 field，inf 的格式是 event.{event_name}.{field_name}，需要 split
            field_list = pattern.findall(sub['subscription_config'])

            for field in field_list:
                field_name = field.split('.')[2]
                if field_name not in self.all_field.get(
                        sub['project_id'],
                        {}).get(
                    table_type, []) and field_name not in self.event_property_white_list:
                    filter_contain_not_exist_subscription.append(sub)
                    break

        if len(filter_contain_not_exist_subscription) > 0:
            self.check_tips_failed('subscription', 'subscription_EVENT_STREAM_filter_check',
                                   '订阅事件数据筛选条件是否包含不存在属性',
                                   self.__format_check_fail_message(
                                       '订阅事件数据筛选条件包含不存在属性。如果与商务和客户协商可以继续升级，将忽略本检查，并不迁移以下数据',
                                       filter_contain_not_exist_subscription))
        else:
            self.check_tips_succeed('subscription', 'subscription_EVENT_STREAM_filter_check',
                                    '订阅事件数据筛选条件是否包含不存在属性')

    @staticmethod
    def __format_check_fail_message(tip: str, results: list) -> str:
        msg = [tip]
        if not results:
            raise RuntimeError('results must not be empty')
        for sub in results:
            msg.append('项目Id:{}, 订阅 ID：{}，订阅配置：{}'.format(sub['project_id'], sub['id'], sub['subscription_config']))
        return '\n'.join(msg)

    # 查询所有订阅配置
    def __get_all_subscription(self):
        # 旧版本
        sql = '''
        select id,project_id,subscription_type,subscription_config from ${infinity_db}.infinity_subscription_define
        where is_in_use=1
        '''
        if self.is_new_version:
            # 新版本
            sql = '''
            select a.id,a.project_id,b.subscription_type,b.subscription_config
            from ${infinity_db}.infinity_subscription_define a
            left join ${infinity_db}.infinity_subscription_rule_define b
            on a.subscription_rule_id=b.id
            where a.is_in_use=1
            '''

        results = self.query_sql(sql)
        all_dict = {}
        for item in results:
            if item['subscription_type'] not in all_dict:
                all_dict[item['subscription_type']] = []
            all_dict[item['subscription_type']].append(item)
        return all_dict

    # 查询所有旧模型的属性，并分类
    def __get_all_field(self):
        all_dict = {}
        sql = '''
        select project_id, table_type, `name` from ${sca_db}.property_define
        union select project_id, table_type, `name` from ${sca_db}.sp_pre_property_define where tracked = 0
        union select project_id, table_type, `name` from ${sca_db}.external_property_define where overwrite = 0
        '''
        results = self.query_sql(sql)
        for item in results:
            if item['project_id'] not in all_dict:
                all_dict[item['project_id']] = {}
            table_dict = all_dict[item['project_id']]
            if item['table_type'] not in table_dict:
                all_dict[item['project_id']][item['table_type']] = []
            all_dict[item['project_id']][item['table_type']].append(item['name'])

        return all_dict

    def __get_all_virtual_field(self):
        all_dict = {}
        sql = 'select project_id, table_type, `name`, expression from ${sca_db}.external_property_define where overwrite = 0'
        results = self.query_sql(sql)
        for item in results:
            if item['project_id'] not in all_dict:
                all_dict[item['project_id']] = {}
            table_dict = all_dict[item['project_id']]
            if item['table_type'] not in table_dict:
                all_dict[item['project_id']][item['table_type']] = {}
            all_dict[item['project_id']][item['table_type']][item['name']] = item

        return all_dict
