# -*- coding: utf-8 -*- 

import os
import sys
import json
from datetime import datetime

from influxdb import InfluxDBClient

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import datx

import logging
import logging.handlers

fm = logging.Formatter('%(asctime)s %(levelname)s %(name)s.%(funcName)s@%(lineno)d %(message)s')
#fh = logging.handlers.RotatingFileHandler('/opt/spark/spark-1.6.0-bin-hadoop2.6/spark.log', 'a', 13107200, 5)
fh = logging.StreamHandler(sys.stdout)
fh.setFormatter(fm)
logger = logging.getLogger(__name__)
logger.addHandler(fh)
logger.setLevel(logging.DEBUG)

svrip_file_path = os.path.abspath(os.path.dirname(__file__)) + "/svrip_list.txt"
svrip_list = []
with open(svrip_file_path) as f:
    for eachline in f:
        if eachline.strip():
            svrip_list.append(eachline.strip())

logger.info("svrip_list: %s", svrip_list)

client = InfluxDBClient('10.11.0.94', '8086', 'spark', 'spark2018', 'spark')
ipdata_file_path = os.path.abspath(os.path.dirname(__file__)) + "/mydata4vipweek2_2018-04-11.datx"
#ip_db = datx.City("/opt/work/performance_analysis/mydata4vipweek2_2018-04-11.datx")
ip_db = datx.City(ipdata_file_path)



def get_ip_info(ip):
    country = province = city = isp = "unknown"
    info = ip_db.find(ip)
    if len(info) > 4:
        country = info[0]
        province = info[1]
        city = info[2]
        isp_list_str = info[4]
        if '/' in isp_list_str:
            if len(isp_list_str.split('/')) > 0:
                isp = isp_list_str.split('/')[0]
        else:
            isp = isp_list_str
    return country, province, city, isp

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
        message_dict_ext = message_dict['ext']
        if isinstance(message_dict['ext'], (str, unicode)):
            message_dict_ext = json.loads(message_dict['ext'])
        new_message['ext_is_appa'] = int(message_dict_ext['is_appa'])
        new_message['ext_svrip'] = message_dict_ext.get('svrip', 'svr_ip_missing')
        new_message['ext_res_errorcode'] = int(message_dict_ext.get('res_errorcode', -1))
        new_message['ext_server_type'] = int(message_dict_ext.get('server_type', -1))
        if new_message['ext_svrip'].strip() == '':
            new_message['ext_svrip'] = 'svr_ip_null'
        if new_message['ext_svrip'] == 'svr_ip_null':
            new_message['ext_svrip_status'] = "svr_ip_null"
        elif new_message['ext_svrip'] == 'svr_ip_missing':
            new_message['ext_svrip_status'] = "svr_ip_missing"
        elif new_message['ext_svrip'] in svrip_list:
            new_message['ext_svrip_status'] = "svr_ip_valid"
        else:
            new_message['ext_svrip_status'] = "svr_ip_invalid"

        if 'ip' in message_dict:
            country, province, city, isp = get_ip_info(message_dict['ip'])
            new_message['country'] = country
            new_message['province'] = province
            new_message['city'] = city
            new_message['isp'] = isp
        return new_message



def process(rdd):
    begin_time = datetime.now()
    logger.debug('=================================%s' % begin_time)
    current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    
    logger.debug('partitions number: %s' % rdd.getNumPartitions())

    rdd.cache()
    #total_count = rdd.count()
    #logger.debug('total_count: %s' % total_count)

    intact_messages = rdd.filter(lambda (k, v): filter_intact_message(k, v))
    logger.debug('rdd.cache() costs: %s' % str(datetime.now()-begin_time))
    logger.debug('filter intact_messages costs: %s' % str(datetime.now()-begin_time))
    intact_messages.cache()
    intact_messages_count = intact_messages.count()
    logger.debug('intact_messages_count: %s' % intact_messages_count)

    reconstructed_messages = intact_messages.map(lambda (k, v): reconstruct_message(k, v))
    reconstructed_messages.cache()

    fail_messages = reconstructed_messages.filter(lambda message: message['action_code'] == 'msg_server_fail')
    fail_count = fail_messages.count()
    logger.debug('fail_count: %s' % fail_count)

    direct_success_messages = reconstructed_messages.filter(
        lambda message: message['action_code'] == 'msg_server_con' and message['ext_is_appa'] == 0
    )
    direct_success_count = direct_success_messages.count()
    logger.debug('direct_success_count: %s' % direct_success_count)

    direct_gateway_fail_messages = reconstructed_messages.filter(
        lambda message: message['action_code'] == 'msg_server_fail'
        and message['ext_is_appa'] == 0 and message['ext_server_type'] == 100
    )
    direct_gateway_fail_count = direct_gateway_fail_messages.count()
    logger.debug('direct_gateway_fail_count: %s' % direct_gateway_fail_count)

    direct_msgrepeater_fail_messages = reconstructed_messages.filter(
        lambda message: message['action_code'] == 'msg_server_fail' and message['ext_server_type'] == 101
    )
    direct_msgrepeater_fail_count = direct_msgrepeater_fail_messages.count()
    logger.debug('direct_msgrepeater_fail_count: %s' % direct_msgrepeater_fail_count)

    appa_gateway_success_messages = reconstructed_messages.filter(
        lambda message: message['action_code'] == 'msg_server_con' and message['ext_is_appa'] == 1
    )
    appa_gateway_success_count = appa_gateway_success_messages.count()
    logger.debug('appa_gateway_success_count: %s' % appa_gateway_success_count)

    appa_gateway_fail_messages = reconstructed_messages.filter(
        lambda message: message['action_code'] == 'msg_server_fail'
        and message['ext_is_appa'] == 1 and message['ext_server_type'] == 100
    )
    appa_gateway_fail_count = appa_gateway_fail_messages.count()
    logger.debug('appa_gateway_fail_count: %s' % appa_gateway_fail_count)

    direct_gateway_fail_groups = direct_gateway_fail_messages.map(
        lambda message: (message['ext_svrip'], 1)
    ).reduceByKey(lambda a, b: a+b)
    logger.debug('direct gateway fail group count: %s' % direct_gateway_fail_groups.count())

    points = list()
    for item in direct_gateway_fail_groups.collect():
        point = dict()
        point['tags'] = dict()
        point['fields'] = dict()
        point['time'] = current_time
        point['measurement'] = 'direct_gateway_fail'
    
        point['tags']['ext_svrip'] = item[0]

        point['fields']['fail_count'] = item[1]
        point['fields']['total_count'] = direct_success_count + direct_gateway_fail_count + direct_msgrepeater_fail_count
        points.append(point)
    client.write_points(points)

    direct_msgrepeater_fail_groups = direct_msgrepeater_fail_messages.map(
        lambda message: (message['ext_svrip'], 1)
    ).reduceByKey(lambda a, b: a+b)
    logger.debug('direct msgrepeater fail group count: %s' % direct_msgrepeater_fail_groups.count())

    points = list()
    for item in direct_msgrepeater_fail_groups.collect():
        point = dict()
        point['tags'] = dict()
        point['fields'] = dict()
        point['time'] = current_time
        point['measurement'] = 'direct_msgrepeater_fail'
            
        point['tags']['ext_svrip'] = item[0]

        point['fields']['fail_count'] = item[1]
        point['fields']['total_count'] = direct_success_count + direct_gateway_fail_count + direct_msgrepeater_fail_count
        points.append(point)
    client.write_points(points)

    appa_gateway_fail_groups = appa_gateway_fail_messages.map(
        lambda message: (message['ext_svrip'], 1)
    ).reduceByKey(lambda a, b: a+b)
    logger.debug('appa gateway fail group count: %s' % appa_gateway_fail_groups.count())

    points = list()
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

    total_fail_groups = fail_messages.map(
        lambda message: (message['ext_svrip'], 1)
    ).reduceByKey(lambda a, b: a+b)
    logger.debug('total fail group count: %s' % total_fail_groups.count())

    points = list()
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

    error_code_fail_groups = fail_messages.map(
        lambda message: ((message['ext_svrip'], message['ext_res_errorcode']), 1)
    ).reduceByKey(lambda a, b: a+b)
    logger.debug('error code fail group count: %s' % error_code_fail_groups.count())

    points = list()
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

    # to group message by country city isp and ext_svrip_status
    location_groups = reconstructed_messages.map(
        lambda message: (
            (
                message['action_code'],
                message['ext_svrip_status'],
                message['country'],
                message['province'],
                message['city'],
                message['isp']
            ), 1)
    ).reduceByKey(lambda a, b: a+b)
    logger.debug('location group count: %s' % location_groups.count())

    points = list()
    for item in location_groups.collect():
        point = dict()
        point['tags'] = dict()
        point['fields'] = dict()
        point['time'] = current_time
        point['measurement'] = 'location_info'

        point['tags']['action_code'] = item[0][0]
        point['tags']['ext_svrip_status'] = item[0][1]
        point['tags']['country'] = item[0][2]
        point['tags']['province'] = item[0][3]
        point['tags']['city'] = item[0][4]
        point['tags']['isp'] = item[0][5]
           
        point['fields']['count'] = item[1]
        points.append(point)
    client.write_points(points)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        logger.debug("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingKafkaLineCount")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 60)

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-count-consumer", {topic: 10})

    #kvs.repartition(20)
    new_dstream = kvs.transform(lambda rdd: rdd.repartition(128))
    new_dstream.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()


