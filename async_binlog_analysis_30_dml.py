import datetime
import subprocess
import pymysql
from concurrent.futures import ThreadPoolExecutor, as_completed

def analysis_binlog_dml(info):
    try:
        time_begin = datetime.datetime.now()
        conn = pymysql.connect(host="192.168.9.208",
                    port=4306,
                    user="srv_binlog_rl",
                    password="jjmatch",
                    charset="utf8"
                            )
        cursor = conn.cursor()
        databases = []
        tables = []
        exclude_database = ["infra"]
        insert_total = 0
        update_total = 0
        delete_total = 0

        detail_info_all = []
        full_info = list(info)

        binlog_instancename = full_info[1]
        print("start for dml", binlog_instancename)
        binlog_name = full_info[5]
        binlog_ip = full_info[2]
        binlog_port = full_info[3]
        binlog_endtime = full_info[9]
        if binlog_ip.split(".")[1] != "30":
            return

        verify_info_sql = "select count(*),end_time from devopsdb_sql.binlog_info_log where slave_ip=%s and port=%s and binlog_name=%s and binlog_status=1;"
        cursor.execute(verify_info_sql, (binlog_ip, binlog_port, binlog_name))
        verify_info = cursor.fetchall()
        if verify_info[0][0] == 1:
            if (binlog_endtime - datetime.datetime.strptime(verify_info[0][1], "%Y-%m-%d %H:%M:%S")).seconds == 0:
                print("exist",binlog_instancename)
                return
            else:
                sql_delete_log = "delete from devopsdb_sql.binlog_info_log where slave_ip=%s and port=%s and binlog_name=%s and binlog_status=1;"
                cursor.execute(sql_delete_log, (binlog_ip, binlog_port, binlog_name))
                conn.commit()
                sql_delete_detail_log = "delete from devopsdb_sql.binlog_details_log where slave_ip=%s and port=%s and binlog_name=%s and binlog_status=1;"
                delete_sql = cursor.execute(sql_delete_detail_log, (binlog_ip, binlog_port, binlog_name))
                print("detail_log 删除已存在的解析数量（需要重新解析）", delete_sql)
                conn.commit()

        binlog_size_command = "du -sh /data/binlogbackup/%s/%s | awk '{print $1}' " % (binlog_instancename, binlog_name)
        binlog_size_res = subprocess.Popen(binlog_size_command, shell=True, stdout=subprocess.PIPE)
        binlog_size = binlog_size_res.stdout.read().decode("utf-8").replace("\n", "")

        command = "/usr/local/mysql/bin/mysqlbinlog --no-defaults --base64-output=decode-rows -v -v /data/binlogbackup/%s/%s | awk '/STMT_END_F/ { TIME=$1\" \"$2; getline; ACTION=$2; OJECT=$NF; count[TIME\" \"ACTION\" \"OJECT]++} END{for(i in count) print i, count[i]}' | sed -e '/infra/d' > /data/data_tmp/binlog_analysis/%s_%s.log" % (binlog_instancename, binlog_name, binlog_instancename, binlog_name)
        oper_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        res = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        res_error = res.stderr.read().decode("utf-8")
        if res_error:
            print("here have a error message",res_error)
            return
        res.wait()
        try:
            binlog_log = open(f'/data/data_tmp/binlog_analysis/{binlog_instancename}_{binlog_name}.log', 'r', encoding='utf-8')
        except Exception as e:
            print("there is error when open file", e)
        total_num_command = "wc -l /data/data_tmp/binlog_analysis/%s_%s.log | awk '{print $1}'" % (binlog_instancename, binlog_name)
        total_num_res = subprocess.Popen(total_num_command, shell=True, stdout=subprocess.PIPE)
        total_num = total_num_res.stdout.read().decode("utf-8").replace("\n", "")

        contents = binlog_log.read()
        binlog_log_info = contents.split('\n')
        binlog_log_info.pop()
        for binlog_single_info in binlog_log_info:
            if not binlog_single_info.startswith("#23"):
                continue
            binlog_single = binlog_single_info.split(" ")
            insert_num = 0
            update_num = 0
            delete_num = 0

            if binlog_single[0] and binlog_single[1]:
                execute_time = binlog_single[0].replace("#", "20")[0:4] + '-' + binlog_single[0].replace("#", "20")[4:6] + '-' + binlog_single[0].replace("#","20")[6:] + ' ' + binlog_single[1]
            else:
                continue
            if binlog_single[3].split(".")[0] != "":
                single_database = binlog_single[3].split(".")[0][1:-1]
                single_table = binlog_single[3].split(".")[1][1:-1]
            else:
                continue
            if single_database in exclude_database:
                continue
            if single_database not in databases:
                databases.append(single_database)
            if single_table not in tables:
                tables.append(single_table)

            if binlog_single[2] == "INSERT":
                insert_total += int(binlog_single[4])
                insert_num += int(binlog_single[4])
            elif binlog_single[2] == "UPDATE":
                update_total += int(binlog_single[4])
                update_num += int(binlog_single[4])
            else:
                delete_total += int(binlog_single[4])
                delete_num += int(binlog_single[4])

            single_detail_info =(
                "",
                "",
                "",
                full_info[2],
                full_info[3],
                full_info[5],
                single_database,
                single_table,
                execute_time,
                insert_num,
                update_num,
                delete_num,
                total_num,
                oper_time,
            )
            detail_info_all.append(single_detail_info)
        new_info = (
            "",
            "",
            "",
            full_info[2],
            full_info[3],
            command,
            str(databases),
            str(tables),
            full_info[5],
            binlog_size,
            full_info[8].strftime("%Y-%m-%d %H:%M:%S"),
            full_info[9].strftime("%Y-%m-%d %H:%M:%S"),
            full_info[6],
            full_info[7],
            insert_total,
            update_total,
            delete_total,
            oper_time,
        )

        insert_sql_info = "insert into `devopsdb_sql`.`binlog_info_log`(`app_code`,`service_name`,`host_ip`,`slave_ip`,`port`,`execute_command`,`databases`,`tables`,`binlog_name`,`binlog_size`,`start_time`,`end_time`,`start_pos`,`stop_pos`,`insert_operations`,`update_operations`,`delete_operations`,`record_time`) values(%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s);"
        insert_sql_detail = "insert into `devopsdb_sql`.`binlog_details_log`(`app_code`,`service_name`,`host_ip`,`slave_ip`,`port`,`binlog_name`,`database`,`table`,`execute_time`,`insert_operation`,`update_operation`,`delete_operation`,`total_record`,`record_time`) values(%s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s);"
        sql1 = cursor.execute(insert_sql_info, new_info)
        print("sql1_num", sql1)
        conn.commit()
        for i in range(0, len(detail_info_all),10000):
            diff_detail_info_all = detail_info_all[i:i+10000]
            sql2 = cursor.executemany(insert_sql_detail, diff_detail_info_all)
            print("sql2_num", sql2)
        # 提交修改的数据到数据库
            conn.commit()

        remove_command = "rm -f /data/data_tmp/binlog_analysis/%s_%s.log" % (binlog_instancename, binlog_name)
        remove_res = subprocess.Popen(remove_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        remove_res_error = remove_res.stderr.read().decode("utf-8")
        if remove_res_error:
            print("here have a error message", remove_res_error)
            return
        remove_res.wait()
        conn.close()
        time_end = datetime.datetime.now()
        print("{}解析dml时间：{}分".format(binlog_instancename,((time_end - time_begin).seconds)/60))
        return binlog_instancename+binlog_name
    except Exception as e:
        print("there is a error when analysis binlog", e)

if __name__ == '__main__':
    # 创建连接对象
    load_time = "2023-06-29 09:00:00"
    conn = pymysql.connect(host="192.168.9.208",
                port=4306,
                user="srv_binlog_rl",
                password="jjmatch",
                charset="utf8"
                        )
    cursor = conn.cursor()
    start_time = datetime.datetime.now()
    binlog_num = 0
    print("start time", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%s"))
    t1 = datetime.datetime.now()

    executor = ThreadPoolExecutor(max_workers=4)
    # 准备sql,进行之后的binlog解析
    sql = "select * from devopsdb.mysql_binlogdetails where is_delete=0 and record_createtime > %s;"
    # 执行sql语句
    conn.ping(reconnect=True)
    cursor.execute(sql, (load_time,))
    # 获取查询的结果
    info_all = cursor.fetchall()  # <class 'tuple'>
    work_list_analysis_dml = [executor.submit(analysis_binlog_dml, list(info)) for info in info_all]
    for future in as_completed(work_list_analysis_dml):
        binlog_num += 1
        data = future.result()
        print("analysis dml 任务{} down load success".format(data))

    t4 = datetime.datetime.now()
    print("总共分析的binlog数量", binlog_num)
    print("所有binlog日志dml解析analysis所花费时间：{}分".format(((t4-t1).seconds)/60))
    print("all done")
    print("END time",datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%s"))
    end_time = datetime.datetime.now()
    print("整个程序执行所花费时间：{}分".format(((end_time - start_time).seconds)/60))
    conn.close()