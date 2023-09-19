import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime,timedelta
import pandas as pd

import pymysql
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import monpa
from monpa import utils
from sqlalchemy import create_engine

default_args = {
    'owner':'Angus su',
    'retires': 2,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id="ppt_stock_web_crawling_taskflow_api",
    start_date= days_ago(1),
    schedule_interval = timedelta(days=1),
    default_args = default_args
)

def ppt_stock_web_crawling_taskflow_api():

    @task
    def crawling():
        result = {}
        url = "https://www.ptt.cc/bbs/Stock/index.html"

        for i in range(10): 
            r = requests.get(url)
            soup = bs(r.text,"html.parser")
            data = soup.select("div.r-ent")
            u = soup.select("div.btn-group.btn-group-paging a")
            url = "https://www.ptt.cc"+ u[1]["href"]
            for item in data:
                try:
                    title = item.select("div.title")[0].text.strip()
                    
                    raw_link = item.select("div.title a")[0]["href"]
                    domain_name = "https://www.ptt.cc"
                    link = domain_name + raw_link
                    
                    r_content = requests.get(link)
                    soup_content = bs(r_content.text,"html.parser")

                    header = soup_content.find_all('span','article-meta-value')
                    # 作者
                    author = header[0].text
                    # 看版
                    board = header[1].text

                    main_container = soup_content.find(id='main-container')
                    # 把所有文字都抓出來
                    all_text = main_container.text
                    # 把整個內容切割透過 "-- " 切割成2個陣列
                    pre_text = all_text.split('--')[0]
                    # 把每段文字 根據 '\n' 切開
                    texts = pre_text.split('\n')
                    # 如果你爬多篇你會發現 
                    contents = texts[2:]
                    # 內容
                    content = '\n'.join(contents)
                    
                    ## 找出時間
                    date_and_time = soup_content.select("span.article-meta-value")[-1].text
                    date_and_time = datetime.strptime(date_and_time, "%a %b %d %H:%M:%S %Y")
                    date= date_and_time.strftime("%Y/%-m/%-d")
                    time = date_and_time.strftime("%H:%M:%S")
                    # 處理評論
                    comment = soup_content.find_all('span','push-content')
                    comment = [j.text.strip().replace(': ','') for j in comment]

                    result[title] = {'author': author,'board': board,'date': date, 'time': time, 'link': link, 'content': content, 'comment': comment}
                except:
                    continue
                
        df = pd.DataFrame(result).T.reset_index().rename(columns={'index':'title'})
        return df

    @task
    def processing_data(df):
        today = datetime.now().strftime('%Y/%-m/%-d')
        
        df = df[df['date'] == today].reset_index()
        print('資料處理\n', df)
        df['words'] = df['title'] + df['content']
        df['chunk'] = df['words'].map(utils.short_sentence)

        return df

    @task
    def write_table(df):
        db = pymysql.connect(host='140.118.1.26',
                        user='root',
                        password='simslab7111',
                        database='airflow_web_crawling',
                        port = 3306)
        
        cursor = db.cursor()
        print('DB連線成功')

        table_name = 'airflow_web_crawling_data'
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")

        if cursor.fetchone() is None:
            # 如果表不存在，创建新表格
            print('建立表格')
            create_table_sql = f"""
            CREATE TABLE {table_name} (
                title TEXT(65535),
                author CHAR(100),
                board CHAR(10),
                date DATE,
                time TIME
            )
            """
            cursor.execute(create_table_sql)
            db.commit()
        try:
            conn = create_engine('mysql+pymysql://root:simslab7111@140.118.1.26:3306/airflow_web_crawling')
            ## 這邊因為SQL會誤判字元報錯,因此只上傳該幾個欄位
            df = df[['title', 'author', 'board', 'date', 'time']]
            df.to_sql(table_name, con=conn, if_exists='append', index=False)
            print('資料上傳成功')
        except:
            print('資料上傳失敗')
            pass
        db.close()


    data = crawling()
    data = processing_data(data)
    write_table(data)

dag = ppt_stock_web_crawling_taskflow_api()