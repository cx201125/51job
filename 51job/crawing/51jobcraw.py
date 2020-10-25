#-*- codeing = utf-8 -*-
import requests as rq #用于爬取网页的库
import re   #用于匹配正则
import pymysql as pm #操作数据库
"""
作者:程鑫
时间:2020-09-20
作用：爬取51job指定搜索内容的数据
"""
#爬取指定页数的数据
def getAssignUrlByPage(page,type):

    url='https://search.51job.com/list/060000,000000,0000,00,9,99,'+type+',2,'+str(page)+'.html?lang=c&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&ord_field=0&dibiaoid=0&line=&welfare='
    headers={
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36"
    }
    response=rq.get(url=url,headers=headers)
    content=response.content.decode('gbk')
    com=re.compile('"engine_search_result":(.*?),"jobid_count"')
    type=re.findall(com,content)

    return type[0]

#提取数据
def extractData(datalist):
    # 正则表达式

    # 工作详情链接正则表达式
    job_hrefcom = re.compile('"job_href":"(.*?)","job_name"')
    # 工作姓名的正则表达式
    job_namecom = re.compile('"job_name":"(.*?)","job_title"')
    # 公司链接的正则表达式
    company_hrefcom = re.compile('"company_href":"(.*?)","company_name"')
    # 公司姓名的正则表达式
    company_namecom = re.compile('"company_name":"(.*?)","providesalary_text"')
    # 薪资的正则表达式
    providesalary_textcom = re.compile('"providesalary_text":"(.*?)","workarea"')
    # 工作地点的正则表达式
    workarea_textcom = re.compile('"workarea_text":"(.*?)","updatedate"')
    # 更新时间
    updatatimecom = re.compile('updatedate":"(.*?)","isIntern')
    # 公司类型
    companytype_textcom = re.compile('"companytype_text":"(.*?)","degreefrom"')
    # 发布时间
    issuedatecom = re.compile('"issuedate":"(.*?)","isFromXyz"')
    # 工作福利
    jobwelf_listcom = re.compile('"jobwelf_list":."(.*?)"],"attribute_text"')
    # 工作要求
    attribute_textcom = re.compile('"attribute_text":."(.*?)"],"companysize_text"')
    # 公司人数
    companysize_textcom = re.compile('"companysize_text":"(.*?)人","companyind_text"')
    # 公司行业
    companyind_textcom = re.compile('"companyind_text":"(.*?)","adid"')
    datalist1=[]
    for job in datalist:
        joblist = []
        #获取工作详情链接
        job_href=re.findall(job_hrefcom,job)[0]
        job_href=str(job_href).replace("\\","")
        joblist.append(job_href)
        #获取工作名
        job_name=re.findall(job_namecom,job)[0]
        job_name=str(job_name).replace("\\","")
        joblist.append(job_name)
        # 获取公司链接
        company_href = re.findall(company_hrefcom, job)[0]
        company_href = str(company_href).replace("\\", "")
        joblist.append(company_href)
        # 获取公司姓名
        company_name = re.findall(company_namecom, job)[0]
        joblist.append(company_name)
        # 获取薪资
        providesalary_text = re.findall(providesalary_textcom, job)[0]
        providesalary_text = str(providesalary_text).replace("\\", "")
        joblist.append(providesalary_text)
        # 工作地点
        workarea_text = re.findall(workarea_textcom, job)[0]
        joblist.append(workarea_text)
        # 更新时间
        updatatime = re.findall(updatatimecom, job)[0]
        joblist.append(updatatime)
        # 公司类型
        companytype_text = re.findall(companytype_textcom, job)[0]
        joblist.append(companytype_text)
        # 发布时间
        issuedate = re.findall(issuedatecom, job)[0]
        joblist.append(issuedate)
        # 工作福利
        jobwelf_list = re.findall(jobwelf_listcom,job)[0]
        jobwelf_list=str(jobwelf_list).replace('"',"")
        joblist.append(jobwelf_list)
        # 公司要求
        attribute_text = re.findall(attribute_textcom, job)[0]
        attribute_text = str(attribute_text).replace('"', "")
        attribute_text = str(attribute_text).replace('\\', "")
        joblist.append(attribute_text)
        # 公司行业
        companysize_text = re.findall(companysize_textcom, job)
        if(len(companysize_text)==0):
            joblist.append(" ")
        else:
            joblist.append(companysize_text[0])

        # 公司要求
        companyind_text = re.findall(companyind_textcom, job)[0]
        companyind_text=str(companyind_text).replace("\\","")
        joblist.append(companyind_text)
        # print(joblist[12])
        # for job in joblist:
        #     print(job)
        print(joblist)
        datalist1.append(joblist)
    print("提取数据完毕!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    return datalist1

#爬取数据
def crawData(maxpage,search):
    datalist=[]
    jobcom = re.compile('{(.*?)}')
    for i in range(1,maxpage):
        type=getAssignUrlByPage(page=i,type=search)
        print("爬取完第{}页".format(i))
        joblist=re.findall(jobcom,type)
        datalist.extend(joblist)
    #返回的是1000条工作信息
    datalist1=extractData(datalist)
    print("爬取和提取数据完毕!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    return datalist1


def saveDataToMysql(datalist,search):
    # 连接数据库
    conn = pm.connect("localhost", "root", "201125", "python")
    # 获得游标对象
    cusor = conn.cursor()
    sql = "insert into 51job values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    i=0
    for job in datalist:
        job.insert(0,None)
        job.append(search)
        print(job)
        cusor.execute(sql,args=job)
        i+=1
        print("保存第{}条".format(i))
    # 提交
    conn.commit()
    # 关闭游标对象和connection
    cusor.close()
    conn.close()
    print("保存数据完毕!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")


def main():
    search='sql'
    maxpage=4
    #爬取数据
    datalist=crawData(maxpage=maxpage,search=search)
    # 保存数据到数据库
    saveDataToMysql(datalist,search=search)
    print("爬取51job完毕!!!!!!!!!!!!!!!!!!!!!!!!!")

main()
