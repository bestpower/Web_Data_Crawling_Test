import pymysql
import datetime
import time
from bs4 import BeautifulSoup
import urllib.request
import urllib.request
import urllib.parse
import urllib.request, urllib.parse, http.cookiejar

# 初始化爬取数据存储数据库信息
host = '127.0.0.1'  # 主机地址（本地）
user = 'root'  # 用户名
password = 'asd'  # 密码
port = 3306  # 接口
db = 'lottery'  # 数据库名
db_table = 'lottery_table'  # 数据表名
# 待爬取数据网址（广东快乐十分开奖记录静态网址）
url = 'https://kj.13322.com/kl10_dkl10_history_dtoday.html'

# 连接数据库方法
def connect_db():
    return pymysql.connect(host=host, user=user, password=password, port=port, db=db)

# 查看数据方法
def select_db(issue, db_table):
    data_base = connect_db()
    cursor = data_base.cursor()
    try:
        sql = "SELECT '%s' FROM %s " % (issue, db_table)
        cursor.execute(sql)
        data_base.commit()
    except ValueError as e:
        print(e)
        data_base.rollback()
    finally:
        return issue

# 添加新数据方法
def insert_db(db_table, issue, time_str, num_code):
    data_base = connect_db()
    cursor = data_base.cursor()
    try:
        sql = "INSERT INTO %s VALUES ('%s','%s','%s')" % (db_table, issue, time_str, num_code)
        cursor.execute(sql)
        data_base.commit()
    except ValueError as e:
        print(e)
        data_base.rollback()
    finally:
        cursor.close()
        data_base.close()

# 获取网页信息（解析网址信息并以utf-8编码输出）
def getHtml(url):
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    opener.addheaders = [('User-Agent',
                          'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36'),
                         ('Cookie', '4564564564564564565646540')]
    urllib.request.install_opener(opener)
    html_bytes = urllib.request.urlopen(url).read()
    html_string = html_bytes.decode('utf-8')
    return html_string

# 爬虫方法
def crawl_test():
    # 解析待爬取html网页
    html = getHtml(url)
    # print (html)
    soup = BeautifulSoup(html, fromEncoding='utf8')
    # print (soup)
    # 获取字符表数据
    c_t = soup.select('#trend_table')[0]
    trs = c_t.contents[4:]
    # 获取字符表数据条目
    for tr in trs:
        if tr == '\n':
            continue
        tds = tr.select('td')
        issue = tds[1].text
        time_str = tds[0].text
        num_code = tr.table.text.replace('\n0', ',').replace('\n', ',').strip(',')
        # 输出爬取数据条目
        print('期号：%s\t时间：%s\t号码:%s' % (str(issue), str(time_str), str(num_code)))
        issue_db = select_db(issue, db_table)
        # 判断该条目是否已添加
        if issue_db == issue:
            print('%s 已经存在！' % issue_db)
        else:
            try:
                insert_db(db_table, issue_db, time_str, num_code)
                print('添加%s到%s成功' % (issue_db, db_table))
            except Exception as e:
                print(e)

if __name__ == '__main__':
    flag = 0   # 是否进行增量更新标志位
    now = datetime.datetime.now()  # 当前时间
    # 计划爬取时间
    sched_time = datetime.datetime(now.year, now.month, now.day, now.hour, now.minute, now.second) + \
                 datetime.timedelta(seconds=3)
    # 定时增量更新方法（每隔两分钟更新一次）
    while True:
        now = datetime.datetime.now()
        # 在计划时间内每3秒爬取一次
        if sched_time < now:
            time.sleep(3)
            print(now)
            crawl_test()
            flag = 1
        # 达到计划时间后等待两分钟
        else:
            if flag == 1:
                sched_time = sched_time + datetime.timedelta(minutes=2)
                flag = 0
