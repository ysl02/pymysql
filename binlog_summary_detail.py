import datetime
import subprocess

from django.forms import model_to_dict
from django.utils.decorators import method_decorator
from rest_framework.permissions import IsAuthenticated
from rest_framework.viewsets import GenericViewSet
from OpsManage.models import mysql_binlogdetails, binlog_details_log, binlog_info_log
from OpsManage.Utils.api_util import api_request_audit
from OpsManage.Utils.JsonResponse import JsonResponse



class BinlogInfoView(GenericViewSet):
    permission_classes = [IsAuthenticated]

    @method_decorator(api_request_audit)
    def binlog_info_summary_deatil(self, request):
        exist_ip = []
        info_all = mysql_binlogdetails.objects.filter(is_delete=0)
        for info in info_all:
            full_info = model_to_dict(info)
            binlog_ip = full_info["binlog_ip"]
            binlog_port = full_info["binlog_port"]
            identifier = binlog_ip + '_' + binlog_ip
            if identifier not in exist_ip:
                exist_ip.append(identifier)
            else:
                continue
            single_info_conf = mysql_binlogdetails.objects.filter(binlog_ip=binlog_ip, \
                                                             binlog_port=binlog_port ,\
                                                             is_delete=0).\
                                                    values_list("binlog_name",flat=True).\
                                                    order_by("binlog_name")
            first_binlog_conf = single_info_conf[0]
            last_binlog_conf = single_info_conf[len(single_info_conf)-1]


            single_info_log = binlog_info_log.objects.filter(slave_ip=binlog_ip, \
                                                            port=binlog_port, \
                                                            binlog_status=1). \
                                                    values_list("binlog_name", flat=True). \
                                                    order_by("binlog_name")
            if single_info_log.exists():
                first_binlog_log = single_info_log[0]
                last_binlog_log = single_info_log[len(single_info_log) - 1]
            else:
                continue


            if first_binlog_conf == first_binlog_log and last_binlog_conf == last_binlog_log:
                binlog_info_log.objects.filter(slave_ip=binlog_ip, \
                                               port=binlog_port, \
                                               binlog_status=1,\
                                               binlog_name=last_binlog_log).\
                                        delete()

                binlog_details_log.objects.filter(slave_ip=binlog_ip, \
                                               port=binlog_port, \
                                               binlog_status=1, \
                                               binlog_name=last_binlog_log). \
                                        delete()

            elif first_binlog_log < first_binlog_conf and last_binlog_log <= last_binlog_conf:
                binlog_info_log.objects.filter(slave_ip=binlog_ip, \
                                               port=binlog_port, \
                                               binlog_status=1, \
                                               binlog_name__lt=first_binlog_conf). \
                                        update(binlog_status=0)

                binlog_info_log.objects.filter(slave_ip=binlog_ip, \
                                               port=binlog_port, \
                                               binlog_status=1, \
                                               binlog_name=last_binlog_log). \
                                        delete()

                binlog_details_log.objects.filter(slave_ip=binlog_ip, \
                                               port=binlog_port, \
                                               binlog_status=1, \
                                               binlog_name__lt=first_binlog_conf). \
                                        update(binlog_status=0)

                binlog_details_log.objects.filter(slave_ip=binlog_ip, \
                                               port=binlog_port, \
                                               binlog_status=1, \
                                               binlog_name=last_binlog_log). \
                                        delete()
            elif last_binlog_log == first_binlog_conf:
                binlog_info_log.objects.filter(slave_ip=binlog_ip, \
                                               port=binlog_port, \
                                               binlog_status=1, \
                                               binlog_name__lte=last_binlog_log). \
                                        update(binlog_status=0)

                binlog_details_log.objects.filter(slave_ip=binlog_ip, \
                                                  port=binlog_port, \
                                                  binlog_status=1, \
                                                  binlog_name__lte=last_binlog_log). \
                                        update(binlog_status=0)


        info_all = mysql_binlogdetails.objects.filter(is_delete=0)
        databases = []
        tables = []
        exclude_database = ["infra"]
        insert_total = 0
        update_total = 0
        delete_total = 0
        i=0
        for info in info_all:
            i += 1
            detail_info_all = []
            full_info = model_to_dict(info)
            binlog_instancename = full_info["binlog_instancename"]
            binlog_name = full_info["binlog_name"]
            binlog_ip = full_info['binlog_ip']
            binlog_port = full_info['binlog_port']
            verify_info = binlog_info_log.objects.filter(slave_ip=binlog_ip, \
                                           port=binlog_port, \
                                           binlog_status=1, \
                                           binlog_name=binlog_name)
            if verify_info.exists():
                continue

            base_info = {
                'app_code': full_info['app_code'],
                'service_name': full_info['service_name'],
                'host_ip': full_info['host_ip'],
                'slave_ip': full_info['binlog_ip'],
                'port': full_info['binlog_port'],
                'binlog_name': full_info['binlog_name'],
                'start_pos': full_info['binlog_startlsn'],
                'stop_pos': full_info['binlog_endlsn'],
                'start_time': full_info['binlog_starttime'].strftime("%Y-%m-%d %H:%M:%S"),
                'end_time': full_info['binlog_endtime'].strftime("%Y-%m-%d %H:%M:%S"),
            }

            base_detail_info = {
                'app_code': full_info['app_code'],
                'service_name': full_info['service_name'],
                'host_ip': full_info['host_ip'],
                'slave_ip': full_info['slave_ip'],
                'port': full_info['binlog_port'],
                'binlog_name': full_info['binlog_name'],
                }



            binlog_size_command = "du -sh /data/binlogbackup/%s/%s | awk '{print $1}' " % (binlog_instancename, binlog_name)
            binlog_size_res = subprocess.Popen(binlog_size_command, shell=True, stdout=subprocess.PIPE)
            binlog_size = binlog_size_res.stdout.read().decode("utf-8").replace("\n","")


            command = "mysqlbinlog --no-defaults --base64-output=decode-rows -v -v /data/binlogbackup/%s/%s | awk '/STMT_END_F/ { TIME=$1\" \"$2; getline; ACTION=$2; OJECT=$NF; count[TIME\" \"ACTION\" \"OJECT]++} END{for(i in count) print i, count[i]}' > /data/data_tmp/binlog_analysis/%s_%s.log"  % (binlog_instancename,binlog_name,binlog_instancename,binlog_name)
            oper_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            res = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            res_error = res.stderr.read().decode("utf-8")
            if res_error:
                continue
            res_wait = res.wait()
            if res_wait != 0:
                pass
            binlog_log = open(f'/data/data_tmp/binlog_analysis/{binlog_instancename}_{binlog_name}.log', 'r', encoding='utf-8')

            total_num_command = "wc -l /data/data_tmp/binlog_analysis/%s_%s.log | awk '{print $1}'" % (binlog_instancename, binlog_name)
            total_num_res = subprocess.Popen(total_num_command, shell=True, stdout=subprocess.PIPE)
            total_num = total_num_res.stdout.read().decode("utf-8").replace("\n","")


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
                    execute_time = binlog_single[0].replace("#","20")[0:4] + '-' + binlog_single[0].replace("#","20")[4:6] + '-' + binlog_single[0].replace("#","20")[6:] + ' ' + binlog_single[1]
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

                single_detail_info = {
                    "database": single_database,
                    "table": single_table,
                    "insert_operation": insert_num,
                    "update_operation": update_num,
                    "delete_operation": delete_num,
                    "execute_command": command,
                    "record_time": oper_time,
                    "total_record": total_num,
                    "execute_time": execute_time
                }
                single_detail_info.update(base_detail_info)
                detail_info_all.append(binlog_details_log(**single_detail_info))

            new_info ={
                "execute_command": command,
                "databases": databases,
                "tables": tables,
                "insert_operations": insert_total,
                "update_operations": update_total,
                "delete_operations": delete_total,
                "record_time": oper_time,
                "binlog_size": binlog_size,
            }
            new_info.update(base_info)
            binlog_info_log.objects.create(**new_info)
            binlog_details_log.objects.bulk_create(detail_info_all,batch_size=200)
            # if i ==4:
            #     break
        return JsonResponse(success=True, data=[], message='success')




