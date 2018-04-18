# -*- coding: utf-8 -*- 

from __future__ import print_function

import sys
import json
from datetime import datetime
import requests

from influxdb import InfluxDBClient

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import datx

reload(sys)
sys.setdefaultencoding('utf8')

client = InfluxDBClient('localhost', '8086', 'spark', 'spark2018', 'spark')
ip_db = datx.City("/opt/work/performance_analysis/mydata4vipweek2_2018-04-11.datx")



def filter_danmu_message(k, v): 
    try:
        message_list = json.loads(v)
        if len(message_list) != 0:
            message_dict = message_list[0]

            danmu_action_code_exists = True if 'action_code' in message_dict and message_dict['action_code'] in ('msg_server_fail', 'msg_server_con')  else False    
            return danmu_action_code_exists
        else:
            return False
    except ValueError, e:
        return False
    except:
        return False

def filter_intact_message(k, v): 
    try:
        message_list = json.loads(v)
        if len(message_list) != 0:
            message_dict = message_list[0]

            ct_code_exists = True if 'ct_code' in message_dict else False    
            app_version_exists = True if 'app_version' in message_dict else False    
            ip_exists = True if 'ip' in message_dict else False 
            action_code_exists = True if 'action_code' in message_dict and message_dict['action_code'] in ('msg_server_fail', 'msg_server_con') else False    
            ext_exists = True if 'ext' in message_dict else False    
            ext_is_appa_exists = True if ext_exists and 'is_appa' in message_dict['ext'] else False    
            if ct_code_exists and app_version_exists and ip_exists and action_code_exists and ext_exists and ext_is_appa_exists:
                return True
            else:
                return False 
        else:
            return False
    except ValueError, e:
        return False
    except:
        return False


def reconstruct_message(k, v): 
    message_list = json.loads(v)    
    if len(message_list) != 0: 
        message_dict = message_list[0]  
        new_message = dict()   
        new_message['ct_code'] = message_dict['ct_code']
        new_message['app_version'] = message_dict['app_version']
        new_message['action_code'] = message_dict['action_code']
        #print(type(message_dict['ext']))
        if type(message_dict['ext']) is dict:
            new_message['ext_is_appa'] = str(message_dict['ext']['is_appa'])
            new_message['ext_svrip'] = message_dict['ext'].get('svrip', 'srv_ip_null') 
            new_message['ext_res_errorcode'] = message_dict['ext'].get('errorcode', -1) 
            new_message['ext_server_type'] = message_dict['ext'].get('server_type', -1) 
        else:
            new_message['ext_is_appa'] = str(json.loads(message_dict['ext'])['is_appa'])
            new_message['ext_svrip'] = json.loads(message_dict['ext']).get('svrip', 'srv_ip_null')
            new_message['ext_res_errorcode'] = json.loads(message_dict['ext']).get('res_errorcode', -1)
            new_message['ext_server_type'] = json.loads(message_dict['ext']).get('server_type', -1)
        if new_message['ext_svrip'].strip() == '':
            new_message['ext_svrip'] = 'srv_ip_null'

        if 'ip' in message_dict:
            info = ip_db.find(message_dict['ip'])
            #print(info)
            if len(info) > 4:
                new_message['country'] = info[0]
                new_message['province'] = info[1]
                new_message['city'] = info[2]
                isp_list_str = info[4]
                if '/' in isp_list_str:
                    if len(isp_list_str.split('/')) > 0:
                        new_message['isp'] = isp_list_str.split('/')[0]
                else:
                    new_message['isp'] = isp_list_str
        return new_message


def filter_message(k, v, is_appa_count=False, conn_success=None): 
    try:
        message_list = json.loads(v)
        if len(message_list) != 0:
            message_dict = message_list[0]

            danmu_action_code_exists = True if 'action_code' in message_dict and message_dict['action_code'] in ('msg_server_fail', 'msg_server_con')  else False    
            if is_appa_count:
                if danmu_action_code_exists and 'is_appa' in message_dict['ext']:
                    if conn_success is None:
                        return True
                    if conn_success and message_dict['action_code'] == 'msg_server_con':
                        return True
                    if not conn_success and message_dict['action_code'] == 'msg_server_fail':
                        return True
                else:
                    return False
            else:
                if danmu_action_code_exists:
                    return True
                else:
                    return False
        else:
            return False
    except ValueError, e:
        return False
    except:
        return False


def process(rdd):
    print('=================================%s' % datetime.now())
    #total_count = rdd.count()
    #print('total_count: %s' % total_count)

    #danmu_messages = rdd.filter(lambda (k,v): filter_danmu_message(k,v))
    #danmu_messages_count = danmu_messages.count()
    #print('danmu_messages_count: %s' % danmu_messages_count)

    intact_messages = rdd.filter(lambda (k,v): filter_intact_message(k,v))
    intact_messages_count = intact_messages.count()
    print('intact_messages_count: %s' % intact_messages_count)

    #print('===============================start=%s' % datetime.now())
    reconstructed_messages = intact_messages.map(lambda (k,v): reconstruct_message(k,v))
    #print(len(reconstructed_messages.collect()))
    #if len(reconstructed_messages.collect()) > 0:
        #print((reconstructed_messages.collect()[0]))
    #print('=============================== end =%s' % datetime.now())
    #print(reconstructed_messages.collect()[0])


    #success_count = reconstructed_messages.filter(lambda dict: dict['action_code'] == 'msg_server_con').count()
    #print('success_count: %s' % success_count)
    fail_messages= reconstructed_messages.filter(lambda dict: dict['action_code'] == 'msg_server_fail')
    fail_count = fail_messages.count()
    print('fail_count: %s' % fail_count)

    direct_conn_success_count = reconstructed_messages.filter(lambda dict: dict['action_code'] == 'msg_server_con' and dict['ext_is_appa'] == '0').count()
    print('direct_conn_success_count: %s' % direct_conn_success_count)


    direct_gateway_fail_messages = reconstructed_messages.filter(lambda dict: dict['action_code'] == 'msg_server_fail' and dict['ext_is_appa'] == '0' and dict['ext_server_type'] == 100)
    direct_gateway_fail_count = direct_gateway_fail_messages.count()
    print('direct_gateway_fail_count: %s' % direct_gateway_fail_count)
    

    direct_msgrepeater_fail_messages = reconstructed_messages.filter(lambda dict: dict['action_code'] == 'msg_server_fail' and  dict['ext_server_type'] == 101)
    direct_msgrepeater_fail_count = direct_msgrepeater_fail_messages.count()
    print('direct_msgrepeater_fail_count: %s' % direct_msgrepeater_fail_count)

    appa_gateway_success_count = reconstructed_messages.filter(lambda dict: dict['action_code'] == 'msg_server_con' and dict['ext_is_appa'] == '1').count()
    print('appa_gateway_success_count: %s' % appa_gateway_success_count)


    appa_gateway_fail_messages = reconstructed_messages.filter(lambda dict: dict['action_code'] == 'msg_server_fail' and dict['ext_is_appa'] == '1' and dict['ext_server_type'] == 100)
    appa_gateway_fail_count = appa_gateway_fail_messages.count()
    print('appa_gateway_fail_count: %s' % appa_gateway_fail_count)


    direct_gateway_fail_groups = direct_gateway_fail_messages.map(lambda dict: (dict['ext_svrip'], 1)).reduceByKey(lambda a,b: a+b)
    print('direct gateway fail group count: %s' % len(direct_gateway_fail_groups.collect()))
    points = list()
    current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    for item in direct_gateway_fail_groups.collect():
        point = dict()
        point['tags'] = dict()
        point['fields'] = dict()
        point['time'] = current_time
        point['measurement'] = 'direct_gateway_fail'
    
        point['tags']['ext_svrip'] = item[0]

        point['fields']['fail_count'] = item[1]
        point['fields']['total_count'] = direct_conn_success_count + direct_gateway_fail_count + direct_msgrepeater_fail_count
        points.append(point)
    client.write_points(points)


    direct_msgrepeater_fail_groups = direct_msgrepeater_fail_messages.map(lambda dict: (dict['ext_svrip'], 1)).reduceByKey(lambda a,b: a+b)
    print('direct msgrepeater fail group count: %s' % len(direct_msgrepeater_fail_groups.collect()))
    points = list()
    current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    for item in direct_msgrepeater_fail_groups.collect():
        point = dict()
        point['tags'] = dict()
        point['fields'] = dict()
        point['time'] = current_time
        point['measurement'] = 'direct_msgrepeater_fail'
            
        point['tags']['ext_svrip'] = item[0]

        point['fields']['fail_count'] = item[1]
        point['fields']['total_count'] = direct_conn_success_count + direct_gateway_fail_count + direct_msgrepeater_fail_count
        points.append(point)
    client.write_points(points)


    appa_gateway_fail_groups = appa_gateway_fail_messages.map(lambda dict: (dict['ext_svrip'], 1)).reduceByKey(lambda a,b: a+b)
    print('appa gateway fail group count: %s' % len(appa_gateway_fail_groups.collect()))
    points = list()
    current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    for item in appa_gateway_fail_groups.collect():
        point = dict()
        point['tags'] = dict()
        point['fields'] = dict()
        point['time'] = current_time
        point['measurement'] = 'appa_gateway_fail'
            
        point['tags']['ext_svrip'] = item[0]

        point['fields']['fail_count'] = item[1]
        point['fields']['total_count'] = appa_gateway_success_count + appa_gateway_fail_count
        points.append(point)
    client.write_points(points)


    total_fail_groups = fail_messages.map(lambda dict: (dict['ext_svrip'], 1)).reduceByKey(lambda a,b: a+b)
    print('total fail group count: %s' % len(total_fail_groups.collect()))
    points = list()
    current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    for item in total_fail_groups.collect():
        point = dict()
        point['tags'] = dict()
        point['fields'] = dict()
        point['time'] = current_time
        point['measurement'] = 'total_fail'
            
        point['tags']['ext_svrip'] = item[0]

        point['fields']['fail_count'] = item[1]
        point['fields']['total_count'] = intact_messages_count
        points.append(point)
    client.write_points(points)


    error_code_fail_groups = fail_messages.map(lambda dict: ((dict['ext_svrip'], dict['ext_res_errorcode']), 1)).reduceByKey(lambda a,b: a+b)
    print('error code fail group count: %s' % len(error_code_fail_groups.collect()))
    points = list()
    current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    for item in error_code_fail_groups.collect():
        point = dict()
        point['tags'] = dict()
        point['fields'] = dict()
        point['time'] = current_time
        point['measurement'] = 'error_code_fail'
    
        point['tags']['ext_svrip'] = item[0][0]
        point['tags']['ext_res_errorcode'] = item[0][1]

        point['fields']['fail_count'] = item[1]
        point['fields']['total_count'] = intact_messages_count
        points.append(point)
    client.write_points(points)


    # in order to compute the total count of isp info, construct a map with key:country+province+city, value is count
    total_location_count_dict = dict()
    total_location_groups = reconstructed_messages.map(lambda dict: ((dict['country'], dict['province'], dict['city'], dict['isp']), 1)).reduceByKey(lambda a,b: a+b)
    for item in total_location_groups.collect():
        key = item[0][0] + item[0][1] + item[0][2] + item[0][3]
        value = item[1]
        total_location_count_dict[key] = value


    location_groups = reconstructed_messages.map(lambda dict: ((dict['action_code'], dict['country'], dict['province'], dict['city'], dict['isp']), 1)).reduceByKey(lambda a,b: a+b)
    print('location group count: %s' % len(location_groups.collect()))
    points = list()
    current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    #print(location_groups.collect()[0])
    for item in location_groups.collect():
        point = dict()
        point['tags'] = dict()
        point['fields'] = dict()
        point['time'] = current_time
        point['measurement'] = 'location_fail'
    
        point['tags']['action_code'] = item[0][0]
        point['tags']['country'] = item[0][1]
        point['tags']['province'] = item[0][2]
        point['tags']['city'] = item[0][3]
        point['tags']['isp'] = item[0][4]
           
        point['fields']['count'] = item[1]
        key = item[0][1] + item[0][2] + item[0][3] + item[0][4]
        total_count = total_location_count_dict.get(key, 0)
        point['fields']['total_count'] = total_count
        if total_count > 0:
            point['fields']['rate'] = item[1] / (total_count * 1.0)
        else:
            point['fields']['rate'] = 0
        points.append(point)
    client.write_points(points)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingKafkaLineCount")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 60)

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-count-consumer", {topic: 1})

    kvs.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()


