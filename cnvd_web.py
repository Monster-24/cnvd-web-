from gevent import monkey
monkey.patch_all()
import os, gevent
from gevent.queue import Queue
import requests
import sqlite3
import hashlib
import math
from bs4 import BeautifulSoup


def request(url):
    headers = {
        "authority": "www.cnvd.org.cn",
        "method": "GET",
        "scheme": "https",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "zh-CN,zh;q=0.9",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers)
        return response
    except Exception as e:
        print(e)
        request(url)

def html_analysis(response):
    data = []
    soup = BeautifulSoup(response.text, 'lxml')
    tr = soup.findAll('tr')
    for record in tr:
        url = record.a['href']
        name = record.a['title'].replace('"', '')
        level = record.findAll('td')[1].text.split()[0]
        click = record.findAll('td')[2].text.split()[0]
        comment = record.findAll('td')[3].text.split()[0].replace('"', '')
        follow = record.findAll('td')[4].text.split()[0]
        datetime = record.findAll('td')[5].text.split()[0]
        data.append([url, name, level, click, comment, follow, datetime])
        
    return data

def save_data(dbConn, data):
    hash = lambda s:hashlib.new('sha1', s.encode('utf-8')).hexdigest()
    cursor = dbConn.cursor()
    for row in data:
        sql = """insert into WEB values(
                                        null, 
                                        "{url}", 
                                        "{name}", 
                                        "{level}", 
                                        {click}, 
                                        {comment},
                                        {follow},
                                        "{datetime}",
                                        "{hash}")""".format(
                                                            url   = row[0],
                                                            name  = row[1],
                                                            level = row[2],
                                                            click = row[3],
                                                            comment = row[4],
                                                            follow = row[5],
                                                            datetime = row[6],
                                                            hash   = hash(''.join(row))
                                                        )
        try:
            cursor.execute(sql)
        except Exception as e:
            print(e)
        dbConn.commit()

def create_db(dbConn):
    cursor = dbConn.cursor()
    sql = """CREATE TABLE WEB
        (id               INTEGER  PRIMARY KEY AUTOINCREMENT,
        web_url           VARCHAR(100)     NOT NULL,
        web_name          VARCHAR(100)     NOT NULL,
        web_level         CHAR(10)         NOT NULL,
        web_click         INT              NOT NULL,
        web_comment       INT              NOT NULL,
        web_follow        INT              NOT NULL,
        web_datetime      CHAR(30)         NOT NULL, 
        web_hash          CHAR(30)         NOT NULL);"""
    cursor.execute(sql)
    dbConn.commit()

def run():
    while not q.empty():
        url = q.get_nowait()
        print(url)
        data = html_analysis(request(url))
        save_data(conn, data)



if __name__ == "__main__":
    if not os.path.exists("test.db"):
        conn = sqlite3.connect('test.db')
        create_db(conn)
    else:
        conn = sqlite3.connect('test.db')

    q = Queue()

    #生产数据
    for i in range(0, 22253, 100):
        url = "https://www.cnvd.org.cn/flaw/typeResult?typeId=29&max=100&offset={}".format(i)
        q.put_nowait(url)
    
    tasks = [gevent.spawn(run) for i in range(10)]
    gevent.joinall(tasks)


    