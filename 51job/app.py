from flask import Flask,render_template,make_response
import pymysql as pm
import copy

app = Flask(__name__)


#获得每个程序语言的数量
def getSearchCount(cuscor):
    sql = "SELECT job_search ,COUNT(*) search_num from 51job GROUP BY job_search"
    cuscor.execute(sql)
    searchlist = cuscor.fetchall()
    #用来存储所有的search的名称
    search=[]
    #用来存储所有的search的数量
    searchcount=[]
    #把每个元素添加到相应的集合
    for i in searchlist:
        search.append(i[0])
        searchcount.append(i[1])
    #把所有的search名字和数量添加到集合中返回
    searchlist=[]
    searchlist.append(search)
    searchlist.append(searchcount)
    return searchlist
#计算一个语言的平均薪资
def getAvgByThousAndMonth(searchlist):
    moneysum = 0.0
    i=0
    for pro in searchlist:
        prostr = str(pro[0])
        # 过滤没有写薪资的工作
        if prostr != '':
            i+=1
            # 获取工作薪资是按什么计算的
            datestr = str(prostr.split('/')[-1])
            if datestr == '月':
                oneprostr = str(prostr.split('/')[0])
                if oneprostr[-1] == '万':
                    money = oneprostr.split("-")
                    # 计算最高工资和最低薪资的平均薪资
                    moneysum += (float(money[0]) * 10000 + float(money[1].replace("万", "")) * 10000) / 2
                elif oneprostr[-1] in '千':
                    money = oneprostr.split("-")
                    # 计算最高工资和最低薪资的平均薪资
                    moneysum += (float(money[0]) * 1000 + float(money[1].replace("千", "")) * 1000) / 2
                elif oneprostr[-1] == '百':
                    money = oneprostr.split("-")
                    # 计算最高工资和最低薪资的平均薪资
                    moneysum += (float(money[0]) * 100 + float(money[1].replace("百", "")) * 100) / 2
                elif oneprostr[-1] == '下':
                    moneysum += (float(oneprostr.replace("千以下", "")) * 1000)
            elif datestr == '年':
                oneprostr = str(prostr.split('/')[0])
                money = oneprostr.split("-")
                # 计算最高工资和最低薪资的平均薪资
                moneysum += ((float(money[0]) * 10000 + float(money[1].replace("万", "")) * 10000) / 2) / 12
            elif datestr == '天':
                oneprostr = str(prostr.split('/')[0])
                moneysum += (float(oneprostr.split("-")[0].replace("元", ""))) * 30
            elif datestr == '小时':
                oneprostr = str(prostr.split('/')[0])
                moneysum += ((float(oneprostr.replace("元", ""))) * 30) * 8
    return round(moneysum/i,2)

# 1159, 496, 474, 600, 142, 149
def getSearchAvgProvidesalary(cursor,searchlist):
    sql = "SELECT providesalary_text search_num from 51job where job_search=%s"

    moneyavglist=[]
    moneyavgAndsearchnamelist=[]
    #循环计算每一个程序语言的平均薪资
    for i in range(0,len(searchlist[0])):
        cursor.execute(sql,args=searchlist[0][i])
        avglist=cursor.fetchall()
        moneyavg=getAvgByThousAndMonth(avglist)
        moneyavglist.append(moneyavg)
    #计算所有程序语言的平均值，并且加入到集合中
    cursor.execute("SELECT providesalary_text from 51job")
    moneyavglist.insert(0,getAvgByThousAndMonth(cursor.fetchall()))
    searchlist[0].insert(0,'Sum')
    # 向工资平均值和搜索名称添加工资平均值集合，和搜索名称集合
    moneyavgAndsearchnamelist.append(searchlist[0])
    moneyavgAndsearchnamelist.append(moneyavglist)
    return moneyavgAndsearchnamelist

#获取公司的类别及其数量
def getCompanyTypeAndCount(cursor):
    datalist=[]
    sql="SELECT  companytype_text,count(companytype_text) from 51job GROUP BY companytype_text"
    cursor.execute(sql)
    typelist=[]
    countlist=[]
    #获取全部的公司及其类别
    alllist=cursor.fetchall()
    #把每一个类别的名字和数量分解添加到相应的集合
    for type in alllist:
        typelist.append(type[0])
        countlist.append(type[1])
    #把类别名字的集合添和数量的集合添加到数据的集合并返回
    datalist.append(typelist)
    datalist.append(countlist)
    return datalist


def getWorkAreaAndCount(cursor):
    datalist=[]
    sql='SELECT  workarea_text,count(workarea_text) cw from 51job GROUP BY workarea_text order BY cw desc'
    cursor.execute(sql)
    #存地区名称的集合
    worknamelist=[]
    # 对其他地区进行累加
    qitacount=0
    #存储数量的集合
    countlist=[]
    #获取工作地区和数量的集合
    worklist=cursor.fetchall()
    for i in range(0,len(worklist)):
        if i<11:
            worknamelist.append(str(worklist[i][0]).replace("重庆-",""))

            countlist.append(worklist[i][1])
        else:
            qitacount+=int(worklist[i][1])
    worknamelist.append("其他")
    countlist.append(qitacount)
    #把名称的数量的集合添加到返回的集合
    datalist.append(worknamelist)
    datalist.append(countlist)
    return datalist

#获取工作的福利及其数量
def getJobMelfList(cursor):
    sql='SELECT jobwelf_list FROM 51job '
    cursor.execute(sql)
    #初始化其他的数量
    qitacount=0
    # 返回的集合
    datalist=[]
    # 临时的map集合，用于计算melf的数量
    melfMap={}
    # melf的列表
    melfList=[]
    # 数量的列表
    count=[]
    melfNameList=cursor.fetchall()
    for melf  in melfNameList:
        if melf[0] != '':
            # 得到福利政策的集合
            melfStr=str(melf[0]).split(',')
            for i in melfStr:
                if i not in melfMap:
                    melfMap[i]=1
                else:
                    melfMap[i]=melfMap[i]+1
    #遍历每一个map，把集合变为两个列表并返回
    for key,value in melfMap.items():
        if int(value)>363:
            melfList.append(key)
            count.append(value)
        else:
            qitacount+=value
    melfList.append("其他")
    count.append(qitacount)
    datalist.append(melfList)
    datalist.append(count)
    return datalist

#获取经验和要求的方法
def getJobSufferEduactionList(cursor):
    datalist=[]
    sql="select attribute_text FROM 51job "
    cursor.execute(sql)
    sufferlist=[]
    eduactionlist=[]
    #对数据进行过滤清洗，把经验和学历要求放进相应的集合
    for claim in cursor.fetchall():
        claims=str(claim[0]).split(",")
        if len(claims)==4:
            sufferlist.append(str(claims[-3]).replace("经验",""))
            eduactionlist.append(claims[-2])
        elif len(claims)==3:
            if claims[-2].find("经验")==-1:
                eduactionlist.append(claims[-2])
            elif claims[-2].find("经验")!=-1:
                sufferlist.append(str(claims[-2]).replace("经验",""))
    datalist.append(listToMapToList(eduactionlist))
    datalist.append(listToMapToList(sufferlist))
    return datalist
#用来统计数量的方法
def listToMapToList(list):
    datalist=[]
    datalist.append([])
    datalist.append([])
    datamap={}
    for item in list:
        if item not in datamap:
            datamap[item]=1
        else:
            datamap[item]=datamap[item]+1
    for key,value in datamap.items():
        datalist[0].append(key)
        datalist[1].append(value)
    return datalist


def getAllJob(cursor):
    sql="select * from 51job"
    cursor.execute(sql)
    return cursor.fetchall()

@app.route('/list')
def list():
    # 连接数据库
    conn = pm.connect("localhost", "root", "201125", "python")
    # 获得游标对象
    cursor = conn.cursor()

    #获取所有的工作
    allJob=getAllJob(cursor)
    print(allJob)
    # 关闭游标对象和connection
    conn.close()
    return render_template("list.html",
                           allJob=allJob
                           )

@app.route('/')
def index():
    # 连接数据库
    conn = pm.connect("localhost", "root", "201125", "python")
    # 获得游标对象
    cursor = conn.cursor()
    #获取search的名字和数量
    searchlist=getSearchCount(cursor)
    # 把列表重新赋值，用于传递参数
    searchlist1=copy.deepcopy(searchlist)
    #获取每一个search的平均薪资
    searchAvgProvidesalarylist=getSearchAvgProvidesalary(cursor,searchlist1)
    #获取公司的类别及其数量
    companyTypeAndCount=getCompanyTypeAndCount(cursor)
    #获取工作的福利政策
    jobmeillist= getJobMelfList(cursor)
    #获取工作的的地点
    workarealist=getWorkAreaAndCount(cursor)
    # 获取工作的学历要求，和工作经验要求
    jobSufferEduactionList=getJobSufferEduactionList(cursor)

    # 关闭游标对象和connection
    conn.close()
    return render_template("index.html",
                           searchlist=searchlist,
                           searchAvgProvidesalarylist=searchAvgProvidesalarylist,
                           companyTypeAndCount=companyTypeAndCount,
                           workarealist=workarealist,
                           jobmeillist=jobmeillist,
                           jobSufferEduactionList=jobSufferEduactionList


                           )


if __name__ == '__main__':
    app.config['JSON_AS_ASCII'] = False
    app.run(debug=True)


