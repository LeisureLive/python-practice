import random
import time
import uuid

from pyguidance.impala_client.GuidanceImpala import get_impala_client


def insert_impala(sql):
    connector = get_impala_client('sp', 'rawdata', current_product='sp', current_module='scheduler')
    cursor = connector.cursor()
    try:
        cursor.execute(sql)
    finally:
        cursor.close()
        connector.close()


def batch_insert_impala(sql, data):
    connector = get_impala_client('sp', 'rawdata', current_product='sp', current_module='scheduler')
    cursor = connector.cursor()
    try:
        cursor.executemany(sql, data)
    finally:
        cursor.close()
        connector.close()


if __name__ == '__main__':

    sql = '''
    insert into rawdata.profile_wos_p4(id,first_id,second_id,p__device_id_list,p__is_deleted,p__update_time,p_property_user_string1) 
    values (%s,'%s','%s','%s',%s,%s,'%s');
    '''
    data = []
    for i in range(30000):
        id = random.randint(1000000000000000000, 1999999999999999999)
        second_id = 'second_id' + str(uuid.uuid4())
        first_id = 'first_id' + str(uuid.uuid4())
        p__device_id_list = ''
        for j in range(499):
            p__device_id_list += 'device_id_' + str(uuid.uuid4()) + '\n'
        p__device_id_list += 'device_id_' + str(uuid.uuid4())
        p__is_deleted = 0
        p__update_time = int(time.time() * 1000 * 1000)
        p_property_user_string1 = str(uuid.uuid4())
        data.append(
            (id, first_id, second_id, p__device_id_list, p__is_deleted, p__update_time, p_property_user_string1))
        if i % 50 == 0:
            batch_insert_impala(sql, data)
            data = []
    batch_insert_impala(sql, data)
