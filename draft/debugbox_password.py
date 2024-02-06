import requests


def get_pwd():
    url = 'http://security.sensorsdata.cn/qa_auth'
    header = {
        'x-qa-tools': 'Oj4QluoQ6ZT6VzACWY9Bqr5gzVFPFhoUPJIqzmZTRxc'
    }
    resp = requests.get(url, headers=header, params=None)
    if resp.status_code == 404:
        return None
    elif resp.status_code != 200:
        raise RuntimeError('get %s error %s, %s' % (url, resp.status_code, resp.text))
    # print(resp.text)
    return resp.text


if __name__ == '__main__':
    print(get_pwd())