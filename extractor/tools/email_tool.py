import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from prettytable import PrettyTable

from extractor.tools.common_tools import get_sdf_version


def send_email(sender_addr, password, receiver_addr, title, content):
    msg = MIMEMultipart()
    textApart = MIMEText(content)
    msg.attach(textApart)
    msg['Subject'] = title
    msg['From'] = sender_addr
    msg['To'] = receiver_addr
    try:
        # smtp服务器,端口
        server = smtplib.SMTP_SSL('smtp.163.com', 465)
        # 打印debug日志
        server.set_debuglevel(1)
        server.login(sender_addr, password)
        server.sendmail(sender_addr, receiver_addr, msg.as_string())
        print('success')
        server.quit()
    except smtplib.SMTPException as e:
        print('error:', e)  # 打印错误


def send_benchmark_result(ip_list, id2_project_qps_list, id3_project_qps_list,
                          sender_addr, password, receiver_addrs):
    if receiver_addrs is None:
        receiver_addrs = 'hejie@sensorsdata.cn'

    cluster_size = len(ip_list)
    # 测试结果
    table = PrettyTable(['数据集', 'avg_qps', 'min_qps', 'max_qps'])
    for case_qps in id2_project_qps_list:
        avg_qps = int(case_qps['avg_qps']) * cluster_size
        max_qps = int(case_qps['max_qps']) * cluster_size
        min_qps = int(case_qps['min_qps']) * cluster_size
        table.add_row(['[id2 多对一] ' + case_qps['title'], avg_qps, min_qps, max_qps])
    for case_qps in id3_project_qps_list:
        avg_qps = int(case_qps['avg_qps']) * cluster_size
        max_qps = int(case_qps['max_qps']) * cluster_size
        min_qps = int(case_qps['min_qps']) * cluster_size
        table.add_row(['[id3] ' + case_qps['title'], avg_qps, min_qps, max_qps])

    data_html = table.get_html_string()

    # 正文
    sdf_version = get_sdf_version(ip_list[0])
    content = ''
    content += "<p>机器 IP: " + f'{ip_list} </p>'
    content += "<p>SDF 版本: " + f'{sdf_version} </p>'

    # 拼装 html
    title = '导入流性能 daily benchmark'
    head = \
        """
             <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <style>
                    body {
                        font-family: 'Arial', sans-serif;
                        background-color: #f4f4f4;
                        margin: 0;
                        padding: 20px;
                    }
                    h1 {
                        color: #333;
                        text-align: center;
                    }
                    table {
                        width: 100%;
                        max-width: 4096px;
                        margin: 20px auto;
                        border-collapse: collapse;
                    }
                    th, td {
                        border: 2px solid #ddd;
                        padding: 10px;
                        text-align: left;
                    }
                    th {
                        background-color: #5cb85c;
                        color: white;
                    }
                    td {
                        background-color: #f0f0f0;
                    }
                    tr:nth-child(even) td {
                        background-color: #f9f9f9;
                    }
                    .table-description {
                        margin-top: 10px;
                        color: #666;
                        text-align: center;
                    }
                    .hover-row:hover {
                        background-color: #e9e9e9;
                    }
                    .content-description {
                        margin-top: 20px;
                        color: #666;
                        text-align: left;
                        font-weight: bold;
                    }
                </style>
            </head>
        """
    body = \
        """
            <body>
                <h1>{title}</h1>
                <div>
                    {data_html}
                </div>
                <div class="content-description">
                  <h4>{content}</h4>
                </div>
            </body>
        """.format(title=title, content=content, data_html=data_html)

    html_msg = "<html>" + head + body + "</html>"

    msg = MIMEMultipart()
    subject = 'SDF 架构导入流性能测试结果(ID2 多对一 + ID3) - ' + datetime.now().strftime("%Y-%m-%d")
    msg['Subject'] = subject
    msg['From'] = sender_addr
    msg['To'] = receiver_addrs
    msg.attach(MIMEText(html_msg.encode("utf-8"), 'html', 'utf-8'))

    try:
        # smtp服务器,端口
        server = smtplib.SMTP_SSL('smtp.163.com', 465)
        # 打印debug日志
        server.set_debuglevel(1)
        server.login(sender_addr, password)
        server.sendmail(sender_addr, receiver_addrs.split(";"), msg.as_string())
        print('success')
        server.quit()
    except smtplib.SMTPException as e:
        print('error:', e)  # 打印错误


if __name__ == '__main__':
    # send_email('enjoyleisure8027@163.com', 'HSUJWIYVQGDMFXDH', 'hejie@sensorsdata.com', '测试邮件', '这是一封测试邮件!')
    ip_list = ['10.129.24.143', '10.129.25.195', '10.129.26.59']
    id2_project_qps_list = [{'title': 'profile_set', 'min_qps': 100, 'max_qps': 200, 'avg_qps': 150}]
    id3_project_qps_list = [{'title': 'profile_set (version=3.0)', 'min_qps': 100, 'max_qps': 200, 'avg_qps': 150}]
    send_benchmark_result(ip_list, id2_project_qps_list, id3_project_qps_list,
                          'enjoyleisure8027@163.com', 'HSUJWIYVQGDMFXDH', 'hejie@sensorsdata.com')
