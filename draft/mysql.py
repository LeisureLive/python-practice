from __future__ import annotations
import copy
from urllib.parse import urlparse

import pymysql
from hyperion_client.config_manager import ConfigManager
from pyguidance.mysql_config import GuidanceMysqlConfig

from . import env

__READONLY_USER = 'horizon_scout_readonly'
__READONLY_USER_PASSWORD = 'U&ibVkF+6i'


def get_db_name(product, module, resource_name):
    if env.is_sp_20():
        # sp2.0 没有分库
        return 'metadata'
    else:
        return GuidanceMysqlConfig.get_access_conf(
            product, resource_name, current_product=product, current_module=module
        )['access_conf']['db_name']


def __parse_conf_from_jdbc_url(jdbc_url: str) -> dict:
    url = urlparse(jdbc_url.replace('jdbc:mysql:', 'mysql:'))
    return {
        'host': url.hostname,
        'port': url.port,
        'db': url.path.split('/')[1],
    }


def __get_readonly_connection_conf() -> dict:
    if env.is_sp_20():
        # 2.0 环境需要创建只读账号
        return __init_readonly_account_in_sp20()
    else:
        from hyperion_guidance.metadb_account_access import MetaDBAccountAccess

        account = MetaDBAccountAccess().get_metadb_root_account('', 'mariadb')
        conf = __parse_conf_from_jdbc_url(
            GuidanceMysqlConfig.get_mysql_conf_by_specified('sp', 'scheduler', 'sp', 'metadata')['jdbc_url']
        )
        conf['user'] = account['user']
        conf['password'] = account['password']
        return conf


def __init_readonly_account_in_sp20() -> dict:
    # 先获取 root 账号
    root_connection_conf = __get_root_connection_conf_in_sp_20()
    root_connection = __connect_to_mysql(root_connection_conf)

    try:
        if not __readonly_user_exists_in_sp_20(root_connection):
            __create_readonly_user_in_sp_20(root_connection)

        readonly_conf = copy.deepcopy(root_connection_conf)
        readonly_conf['user'] = __READONLY_USER
        readonly_conf['password'] = __READONLY_USER_PASSWORD
        return readonly_conf
    finally:
        root_connection.close()


def __readonly_user_exists_in_sp_20(conn: Connection) -> bool:
    sql = "select count(1) as num from mysql.user u where u.User='%s'" % __READONLY_USER
    ret = conn.query(sql)
    return ret[0]['num'] > 0


def __create_readonly_user_in_sp_20(conn: Connection):
    sql = "GRANT SELECT ON metadata.* TO '%s'@'%%' IDENTIFIED BY '%s'" % (
        __READONLY_USER, __READONLY_USER_PASSWORD)
    conn.execute(sql)
    conn.execute('FLUSH PRIVILEGES')


def __get_root_connection_conf_in_sp_20() -> dict:
    mysql_conf = ConfigManager().get_client_conf("sp", "mysql")
    conf = __parse_conf_from_jdbc_url(mysql_conf['jdbc_url_list'][mysql_conf['master_index']])
    conf['user'] = 'root'
    conf['password'] = mysql_conf['password']
    return conf


class Connection:

    # sp2.1 使用 connect 方法进行连接，不要自己生成实例
    def __init__(self, db_connection):
        self.__mysql_con = db_connection

    def close(self):
        self.__mysql_con.close()

    def query(self, sql, args=None) -> list[dict]:
        self.__mysql_con.ping(reconnect=True)

        ret = []
        with self.__mysql_con.cursor() as cursor:
            cursor.execute(sql, args)
            columns = [x[0] for x in cursor.description]
            for row in cursor.fetchall():
                d = dict(zip(columns, row))
                ret.append(d)
        return ret

    def execute(self, sql, args=None):
        self.__mysql_con.ping(reconnect=True)

        with self.__mysql_con.cursor() as cursor:
            cursor.execute(sql, args)
        self.__mysql_con.commit()

    __TABLE_SQL = "SELECT count(*) AS count FROM INFORMATION_SCHEMA.TABLES " \
                  "WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = %s AND TABLE_NAME = %s"

    def is_table_exists(self, db_name: str, table_name: str) -> bool:
        result = self.query(self.__TABLE_SQL, [db_name, table_name])
        return result[0]['count'] > 0

    __COLUMN_SQL = "SELECT count(*) AS count FROM INFORMATION_SCHEMA.COLUMNS " \
                   "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s"

    def is_column_exists(self, db_name: str, table_name: str, column_name: str) -> bool:
        result = self.query(self.__COLUMN_SQL, [db_name, table_name, column_name])
        return result[0]['count'] > 0


def __connect_to_mysql(conf: dict) -> Connection:
    param = copy.deepcopy(conf)
    param['client_flag'] = pymysql.constants.CLIENT.MULTI_STATEMENTS
    return Connection(pymysql.connect(**param))


class __InstanceHolder:
    instance: Connection | None = None


# 连接数据库并缓存实例
def connect() -> Connection:
    if not __InstanceHolder.instance:
        __InstanceHolder.instance = __connect_to_mysql(__get_readonly_connection_conf())
    return __InstanceHolder.instance


def disconnect():
    if __InstanceHolder.instance:
        __InstanceHolder.instance.close()
        __InstanceHolder.instance = None
