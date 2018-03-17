# -*- coding: utf-8 -*-
"""
Created on Tue Mar 13 14:21:47 2018

@author: 红笺小字
"""
import json
import requests
import random
import csv
import mysql.connector
import logging; logging.basicConfig(level=logging.INFO)

def Get_json(city,kd,pageNo): 
    '''
    city:城市
    kd:搜索栏关键词
    pageNo:页码
    '''
    url = 'https://www.lagou.com/jobs/positionAjax.json?px=default&city={}&needAddtionalResult=false'.format(city)
    data = {
        'first':'ture',
        'pn':pageNo,
        'kd':kd
    }
    
    headers = {
        'Referer': 'https://www.lagou.com/jobs/list_%E6%95%B0%E6%8D%AE%E8%BF%90%E8%90%A5?labelWords=&fromSearch=true&suginput=',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0',
        
        'Cookie':'JSESSIONID=ABAAABAABEEAAJA79B1435F41D6756F4884FFED3961EDA0;'
        '_ga=GA1.2.1862152617.1519566014; user_trace_token=20180225214013-68395722-1a31-11e8-9007-525400f775ce;'
        'LGUID=20180225214013-68395ce7-1a31-11e8-9007-525400f775ce; index_location_city=%E5%8C%97%E4%BA%AC; '
        'X_HTTP_TOKEN=44852a63c46cb5b97b5ead023b15955c; _putrc=4DA4F880F514B9ED; login=true;'
        'unick=%E9%BB%84%E7%A9%97%E5%BC%BA; showExpriedIndex=1; showExpriedCompanyHome=1;' 
        'showExpriedMyPublish=1; hasDeliver=0; _gid=GA1.2.1130159603.1520842144; hideSliderBanner20180305WithTopBannerC=1;' 
        'TG-TRACK-CODE=search_code; Hm_lvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1520342506,1520842144,1520921396,1520922903;' 
        'gate_login_token=42a89501b58c434c83107a9a550a3ed31c1636ac802a269c; Hm_lpvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1520922921;'
        'LGRID=20180313143519-b31ab09b-2688-11e8-b1de-5254005c3644; SEARCH_ID=f4f82a1bb47d4e6494bc46804c248fbc'
                }
    
    response = requests.post(url,headers=headers,data=data,timeout=random.randrange(5,10))
    return response.text

def MaxNumber(city,kd):
    '''
    city:城市
    kd:搜索栏关键词    
    '''
    result = Get_json(city,kd,'1')
    MaxNumber = json.loads(result)['content']['positionResult']['totalCount']//15+1
    return MaxNumber

def Get_results(Json_Data):
    '''
    Json_Data:Json数据
    '''
    Data = json.loads(Json_Data)
    if Data['success'] == True:
        PRS = []
        positions = Data['content']['positionResult']['result']
        for item in positions:
            companyId = item['companyId']
            companyShortName = item['companyShortName']
            companyFullName = item['companyFullName']
            industryField = item['industryField']
            positionId = item['positionId']
            positionName = item['positionName']
            positionLables = item['positionLables']
            education = item['education']
            workYear = item['workYear']
            salary = item['salary']
            district = item['district']
            companySize = item['companySize']
            financeStage = item['financeStage']
            createTime = item['createTime']
            jobNature = item['jobNature']
            positionAdvantage = item['positionAdvantage']
            
            PRS.append([companyId,companyShortName,companyFullName,industryField,
                        positionId,positionName,positionLables,education,workYear,
                        salary,district,companySize,financeStage,createTime,
                        jobNature,positionAdvantage])       
        return PRS
    else:
        logging.info ('post访问错误，请检查请求后重试')   

def main(city,kd):
    '''
    city:城市
    kd:关键词    
    '''
    pageNumber = MaxNumber(city,kd)
    position_info = []
    for i in range(1,pageNumber + 1):
        logging.info ('爬取第{}页...'.format(i))
        Page_Data =Get_json(city,kd,str(i))
        Page_Result = Get_results(Page_Data)
        position_info.append(Page_Result)
    return position_info
        
def fwrite(file,Positions,Label):
    '''
    file:文件名
    Positions:职位列表
    '''
    with open(file,'w',newline='') as f:
        writer = csv.writer(f)        
        writer.writerow(Label)
        for Data in Positions:
            for row in Data:
                try:
                    writer.writerow(row)
                except UnicodeEncodeError as e1:  
                    writer.writerow(['UnicodeEncodeError']) 
            
def Mysql(city,kd,Positions):
    '''
    city:城市
    kd:关键词
    Label:标签
    '''
    conn = mysql.connector.connect(user='root', password='123456')
    cursor = conn.cursor()
    cursor.execute('create database if not exists Positions')
    cursor.execute('use Positions')
    cursor.execute('drop table if exists '+city+kd+'') 
    cursor.execute('create table '+city+kd+' (公司id varchar(255),公司简称 varchar(255), 公司全称 varchar(255), 领域 varchar(255), 职位id varchar(255), 职位名称 varchar(255), 职位标签 varchar(255),学历要求 varchar(255), 工作年限 varchar(255), 薪水 varchar(255), 区域 varchar(255), 公司大小 varchar(255), 融资情况 varchar(255), 发布时间 varchar(255), 职位类型 varchar(255), 职位优势 varchar(255))')   
    for Data in Positions:
        for row in Data:
            #这句怎么优化？
            cursor.execute('insert into '+city+kd+'(公司id,公司简称,公司全称,领域,职位id,职位名称,职位标签,学历要求,工作年限,薪水,区域,公司大小,融资情况,发布时间,职位类型,职位优势) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',[row[0],row[1],row[2],row[3],row[4],row[5],",".join(row[6]),row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15]])
    conn.commit()
    cursor.close()

if __name__ == '__main__':
    city = '上海'
    kd = '数据运营'
    Label = ["公司id","公司简称","公司全称","领域",
             "职位id","职位名称","职位标签","学历要求",
             "工作年限", "薪水","区域","公司大小","融资情况",
             "发布时间","职位类型","职位优势"]
    file = city+'+'+kd+'+'+'Position.csv'  #写入文件的文件名 
    position_results = main(city,kd)       #职位信息
    Mysql(city,kd,position_results)        #写入数据库
#   fwrite(file,position_results,Label)    #写入文件
    



