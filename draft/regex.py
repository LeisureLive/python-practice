import re


def sum_lag():
    result = '''
    {
    "hybrid01.classic-envm-local.org-sep-12120.deploy.sensorsdata.cloud": {
        "current_access_log": "access_log.2024072216",
        "current_access_log_offset": 0,
        "error_code": 1,
        "error_msg": "KeyError('0')",
        "latency_hour": 0,
        "latency_size": 8328,
        "latest_access_log": "access_log.2024072216",
        "send_speed": 0,
        "update_time": "2024-07-22 16:29:51"
    },
    "hybrid02.classic-envm-local.org-sep-12120.deploy.sensorsdata.cloud": {
        "current_access_log": "access_log.2024072216",
        "current_access_log_offset": 0,
        "error_code": 1,
        "error_msg": "KeyError('1')",
        "latency_hour": 0,
        "latency_size": 8328,
        "latest_access_log": "access_log.2024072216",
        "send_speed": 0,
        "update_time": "2024-07-22 16:29:51"
    },
    "hybrid03.classic-envm-local.org-sep-12120.deploy.sensorsdata.cloud": {
        "current_access_log": "access_log.2024072216",
        "current_access_log_offset": 0,
        "error_code": 1,
        "error_msg": "KeyError('2')",
        "latency_hour": 0,
        "latency_size": 8328,
        "latest_access_log": "access_log.2024072216",
        "send_speed": 0,
        "update_time": "2024-07-22 16:29:52"
    },
    "max_latency_hour": 0,
    "total_latency_file_count": 0,
    "total_latency_size": 24984,
    "total_send_speed": 0
}
    '''
    max_latency_hour_pattern = r'"max_latency_hour": (\d+)'
    total_latency_file_count_pattern = r'"total_latency_file_count": (\d+)'
    match_max_latency_hour_group = re.search(max_latency_hour_pattern, result)
    match_total_latency_file_count_group = re.search(total_latency_file_count_pattern, result)
    max_latency_hour = -1
    total_latency_file_count = -1
    if match_max_latency_hour_group:
        max_latency_hour = int(match_max_latency_hour_group.group(1))
    if match_total_latency_file_count_group:
        total_latency_file_count = int(match_total_latency_file_count_group.group(1))

    print(f'max_latency_hour={max_latency_hour}, total_latency_file_count={total_latency_file_count}')

if __name__ == '__main__':
    sum_lag()