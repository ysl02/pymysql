#1.导入pymysql包
import pymysql
#2.创建连接对象
conn=pymysql.connect(host="192.168.56.80",
                port=4306,
                user="root",
                password="123456",
                database="db01",
                charset="utf8"
)
#3.获取游标，目的就是要执行SQL语句
cursor=conn.cursor()
#准备sql,之前在mysql客户端如何编写sql,在Python程序里面还怎么编写
sql="select * from tb_user limit 3;"

#4.执行sql语句
cursor.execute(sql)

#获取查询的结果
#row=cursor.fetchone()   #获取一行
#print(row)

result=cursor.fetchall()
for row in result:
    print(row)
#5.关闭游标
cursor.close()
#6.关闭连接
conn.close()
