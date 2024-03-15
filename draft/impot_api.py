import base64
import gzip
import json
import urllib

import requests


def import_api(gzipType, dataType, jsonString, server):
    Udata = dealwith(gzipType, jsonString)
    if gzipType == 1 and dataType == 1:
        payload = "gzip=1&data_list=%s" % Udata
    elif gzipType == 1 and dataType == 0:
        payload = "gzip=1&data=%s" % Udata
    elif gzipType == 0 and dataType == 1:
        payload = "data_list=%s" % Udata
    elif gzipType == 0 and dataType == 0:
        payload = "data=%s" % Udata
    else:
        print("非法参数！！")
    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'Connection': "close"
    }
    s = requests.session()
    s.keep_alive = False
    response = requests.post(server, data=payload, headers=headers)
    print(response)


def dealwith(gzipType, jsonString):
    data = json.dumps(jsonString, ensure_ascii=False)
    # print("json: "+data)
    data = data.encode('utf-8')
    if gzipType == 1:
        # gzip压缩
        data = gzip.compress(data)
    Bdata = base64.b64encode(data)
    Udata = urllib.parse.quote(Bdata)
    return Udata



if __name__ == '__main__':
    # json_datas = [
    # {"distinct_id":"anony1","anonymous_id":"anony1","event":"$AppStart","type":"track", "identities":{"$identity_idfv":"123123123"},"properties":{"case_id":"anony1","lib":{"$lib_version":"2.6.4-id","$lib":"iOS","$app_version":"1.9.0","$lib_method":"code"}}},
    # {"distinct_id": "anony2", "anonymous_id": "anony2", "event": "$AppStart", "identities":{"$identity_mobile":"15111111111"}, "type": "track","properties": {"case_id": "anony2", "lib": {"$lib_version": "2.6.4-id", "$lib": "iOS", "$app_version": "1.9.0","$lib_method": "code"}}}
    #
    #     {"distinct_id": "123123123", "anonymous_id": "123123123", "event": "$AppStart", "type": "track",
    #      "identities": {"$identity_idfv": "123123123", "$identity_mobile":"15111111111"}, "properties": {"case_id": "123123123",
    #                                                                    "lib": {"$lib_version": "2.6.4-id",
    #                                                                            "$lib": "iOS", "$app_version": "1.9.0",
    #                                                                            "$lib_method": "code"}}},
    # ]

    json_datas = [
            {"distinct_id": "123123123",
             "type": "profile_unset",
             "properties": {"$identity_idfv":"456456", "sgx_idm_string": true}}

        ]
    #
    # json_datas = [
    #     {"distinct_id": "123123123",
    #      "type": "profile_set",
    #      "properties": {"case_id": "one2more_merge_auto_uuid_5", "case_title": "已绑定的匿名id，先删除匿名用户，再重新上报+未绑定过的登录上报，合并成功",
    #                     "sgx_idm_string": "sgx-idm-test", "sgx_idm_int": 100, "sgx_idm_bool": false,
    #                     "sgx_idm_list": ["a", "b", "c"], "sgx_idm_datetime": "2021-03-15"}}

    # ]

    #     json_datas = [
    # {"distinct_id":"one2more_merge_auto_55_2_login_id","login_id":"one2more_merge_auto_55_2_login_id","type":"profile_set_once","properties":{"case_id":"one2more_merge_auto_55_2","case_title":"已绑定的登录用户（登录用户包括匿名id和登录id），分别上报匿名id和登录id，再次调用绑定","sgx_idm_int":100,"sgx_idm_list":["d","e"],"sgx_idm_bool":None,"sgx_idm_datetime":"2021-03-15"}},
    # {"distinct_id":"one2more_merge_auto_55_2_login_id","login_id":"one2more_merge_auto_55_2_login_id","type":"profile_set_once","properties":{"case_id":"one2more_merge_auto_55_2","case_title":"已绑定的登录用户（登录用户包括匿名id和登录id），分别上报匿名id和登录id，再次调用绑定","sgx_idm_string":"sgx idm test profile_set_once","sgx_idm_int":200,"sgx_idm_list":["a","b"],"sgx_idm_bool":true}}
    # ]

    # json_datas = [
    # {"distinct_id":"one2more_merge_auto_xxxx_5_login_id","login_id":"one2more_merge_auto_xxxx_5_login_id","anonymous_id":"one2more_merge_auto_xxxx_5_anony_id","event":"$SubmitOrder","type":"track","properties":{"case_id":"one2more_merge_auto_xxxx_5","case_title":"已绑定的匿名id，先删除匿名用户，再重新上报+未绑定过的登录上报，合并成功"},"lib":{"$lib_version":"2.6.4-id","$lib":"iOS","$app_version":"1.9.0","$lib_method":"code"}}
    # ]
        #
        # json_datas = [
        # {"distinct_id":"one2more_merge_auto_uuid_5_login_id","login_id":"one2more_merge_auto_uuid_5_login_id","type":"profile_delete","properties":{"case_id":"one2more_merge_auto_uuid_5","case_title":"已绑定的匿名id，先删除匿名用户，再重新上报+未绑定过的登录上报，合并成功","sgx_idm_string":"sgx-idm-test","sgx_idm_int":100,"sgx_idm_bool":false,"sgx_idm_list":["a","b","c"],"sgx_idm_datetime":"2021-03-15"}}
        # ]
        #
        # json_datas = [
        # {"distinct_id":"one2more_merge_auto_uuid_5_login_id_2","login_id":"one2more_merge_auto_uuid_5_login_id_2","anonymous_id":"one2more_merge_auto_uuid_5_anony_id","original_id":"one2more_merge_auto_uuid_5_anony_id","event":"$AppStart","type":"track_signup","properties":{"case_id":"one2more_merge_auto_uuid_5","case_title":"已绑定的匿名id，先删除匿名用户，再重新上报+未绑定过的登录上报，合并成功"},"lib":{"$lib_version":"2.6.4-id","$lib":"iOS","$app_version":"1.9.0","$lib_method":"code"}}
        # ]


    import_api(1, 1, json_datas, 'http://10.129.26.92:8106/sa?project=hj1')

