from datetime import datetime
# 定义一个函数，将日期字符串转换为自1970-01-01以来的天数
def date_to_dayid(date_str):
    # 解析日期字符串
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    # 计算从1970-01-01到指定日期的天数
    dayid = date_obj.toordinal()
    return dayid
# 示例使用
date_str = "2024-03-13"
dayid = date_to_dayid(date_str)
print(f"The dayId for {date_str} is {dayid}")