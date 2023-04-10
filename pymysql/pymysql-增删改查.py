#1.导入pymysql包
import pymysql
#2.创建连接对象
conn=pymysql.connect(host="10.30.128.187",
                port=8306,
                user="dba_admin",
                password="DpzYAuwW9M13db0J",
                database="testdb1",
                charset="utf8"
)
#3.获取游标，目的就是要执行SQL语句
cursor=conn.cursor()
#准备sql,之前在mysql客户端如何编写sql,在Python程序里面还怎么编写
#注意点：对数据表完成添加、删除、修改操作，需要把修改的数据提交到数据库
#添加数据
sql="insert into db_course2(name,price) values('sci',520);"
#删除数据
# sql="delete from tb_user where id=1;"
#修改数据
# sql="update tb_user set name='haihai' where id=3;"


try:
    #4.执行sql语句
    cursor.execute(sql)
    #提交修改的数据到数据库
    conn.commit()
except Exception as e:
    #对修改的数据进行撤销，表示数据回滚（回到没有修改数据之前的状态）
    conn.rollback()
finally:
    #5.关闭游标
    cursor.close()
    #6.关闭连接
    conn.close()
