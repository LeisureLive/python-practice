import re

import pymysql

# 匹配虚拟属性不支持订阅的 sql 表达式, 格式为 $.a.b
not_subscribeable_virtual_field_pattern = re.compile(r'\$\.([a-zA-Z][a-zA-Z0-9_]*)\.([a-zA-Z][a-zA-Z0-9_]*)')

# 匹配事件属性 event.event_name.field_name 结构
event_field_pattern = r'event\.([a-zA-Z][a-zA-Z0-9_]*)\.([a-zA-Z][a-zA-Z0-9_]*)'


def check_event_stream_subscription():
    sql = '''
    select a.id,a.project_id,b.subscription_type,b.subscription_config,
     replace(JSON_EXTRACT(b.subscription_config, '$.retain_property_names'),'"','') as retain_property_names,
     replace(JSON_EXTRACT(b.subscription_config, '$**.field'),'"','') as fields
from metadata.infinity_subscription_define a left join metadata.infinity_subscription_rule_define b
on a.subscription_rule_id=b.id where a.is_in_use=1 and b.subscription_type="EVENT_STREAM"
    '''
    results = query_sql(sql)
    if len(results) == 0:
        return

    invalid_subscription = []
    for subscription in results:
        project_id = subscription['project_id']
        field_names = []
        if subscription['retain_property_names'] is not None:
            retain_property_names = subscription['retain_property_names'].strip('[]').split(',')
            retain_property_names = [element.strip() for element in retain_property_names]
            field_names = retain_property_names
        if subscription['fields'] is not None:
            fields = subscription['fields'].strip('[]').split(',')
            fields = [element.strip() for element in fields]
            field_names = retain_property_names + fields
        expressions = list_expression_by_fields(project_id, field_names)
        if len(expressions) == 0:
            continue
        for expression in expressions:
            matchs = not_subscribeable_virtual_field_pattern.findall(expression)
            if len(matchs) > 0:
                invalid_subscription.append(subscription)
    print(invalid_subscription)


def list_expression_by_fields(project_id, fields: list):
    field_names = []
    for field in fields:
        match = re.match(event_field_pattern, field)
        if match:
            field_names.append(match.group(2))
        else:
            field_names.append(field)
    sql = "SELECT DISTINCT expression FROM metadata.external_property_define WHERE `name` IN (%s) AND project_id = %s" % (
        ', '.join([f"'{value}'" for value in field_names]), project_id)
    results = query_sql(sql)
    expressions = []
    if len(results) != 0:
        for result in results:
            expressions.append(result['expression'])
    return expressions


def query_sql(sql_query):
    # 配置MySQL连接参数
    db_config = {
        'host': '10.129.24.120',
        'port': 3305,
        'user': 'sc_dba',
        'password': 'L/O3zhqGGw',
        'database': 'metadata',
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }

    # 连接到MySQL数据库
    connection = pymysql.connect(**db_config)

    # 创建游标对象
    with connection.cursor() as cursor:
        try:
            # 执行SQL语句
            cursor.execute(sql_query)

            # 获取执行结果
            result = cursor.fetchall()

            return result
        except pymysql.Error as err:
            # 处理执行SQL语句过程中的异常
            return f"Error: {err}"
        finally:
            # 关闭连接
            connection.close()


if __name__ == '__main__':
    check_event_stream_subscription()
