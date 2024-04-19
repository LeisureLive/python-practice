import datetime
# 定义一个函数，将日期字符串转换为自1970-01-01以来的天数
def date_to_dayid(date_str):
    # 获取当前日期和时间
    input_date = datetime.strptime(date_str, "%Y-%m-%d")
    # 定义epoch时间，即1970年1月1日
    epoch = datetime(1970, 1, 1)
    # 计算当前日期和epoch时间之间相差的天数
    days_difference = (input_date - epoch).days
    return days_difference

if __name__ == '__main__':
    # # 示例使用
    # date_str = "2024-03-13"
    # dayid = date_to_dayid(date_str)
    # print(f"The dayId for {date_str} is {dayid}")
    print(datetime.datetime.now().strftime("%Y_%m_%d"))