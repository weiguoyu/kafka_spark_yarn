# -*- coding: utf-8 -*-

import sys
import time
from influxdb import InfluxDBClient

import logging
import logging.handlers

fm = logging.Formatter('%(asctime)s %(levelname)s %(name)s.%(funcName)s@%(lineno)d %(message)s')
fh = logging.handlers.RotatingFileHandler('/opt/spark/spark-1.6.0-bin-hadoop2.6/spark_cron.log', 'a', 13107200, 5)
fh.setFormatter(fm)
logger = logging.getLogger(__name__)
logger.addHandler(fh)
logger.setLevel(logging.DEBUG)


client = InfluxDBClient('localhost', '8086', 'spark', 'spark2018', 'spark')

def influx_to_normal(value):
    province = value[0][1]["province"]
    city = value[0][1]["city"]
    isp = value[0][1]["isp"]
    sum = value[1].next()["sum"]
    return ((province, city, isp), sum)


def timestamp_to_utc_str(timestamp):
    utc_time_stamp = timestamp - 8 * 3600
    utc_str = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(utc_time_stamp))
    return utc_str

def str2unixtime(datastr):
    return time.mktime(time.strptime(datastr, '%Y-%m-%d %H:%M:%S'))

def svr_ip_task(start_time):
    svr_ip_invalid_query = u"""
        SELECT sum("count") FROM "location_info"
        WHERE "action_code" = 'msg_server_fail'
        AND "ext_svrip_status" = 'svr_ip_invalid'
        AND "country" = '中国'
        AND time >= '{start_time}'
        and time < '{end_time}'
        GROUP BY time(1h), "province", "city", "isp"  fill(null)
    """

    total_query = u"""
        SELECT sum("count") FROM "location_info"
        where "country" = '中国'
        AND time >= '{start_time}'
        and time < '{end_time}'
        GROUP BY time(1h), "province", "city", "isp"  fill(null)
    """

    end_time = start_time + 3600

    start_time = timestamp_to_utc_str(start_time)
    end_time = timestamp_to_utc_str(end_time)
    svr_ip_invalid_query = svr_ip_invalid_query.format(start_time=start_time, end_time=end_time)
    total_query = total_query.format(start_time=start_time, end_time=end_time)

    logger.debug("svr_ip_invalid_query %s", svr_ip_invalid_query)
    logger.debug("total_query %s", total_query)

    svr_ip_invalid_datas = client.query(svr_ip_invalid_query)
    svr_ip_invalid_datas = map(influx_to_normal, svr_ip_invalid_datas.items())
    total_datas = client.query(total_query)
    total_datas = map(influx_to_normal, total_datas.items())

    # 根据 省-市-运营商 获取失败个数排名前30的数据
    svr_ip_invalid_datas_top_30 = sorted(svr_ip_invalid_datas, key=lambda x: x[1], reverse=True)[0:30]

    # 根据 省-市-运营商 获取失败率排名前30的数据, 阈值设置为500
    svr_ip_invalid_rates = []
    svr_ip_invalid_dict = dict(svr_ip_invalid_datas)
    total_dict = dict(total_datas)
    for key in total_dict:
        if key not in svr_ip_invalid_dict:
            continue
        if total_dict[key] < 500:
            continue
        rate = '%.2f' % (svr_ip_invalid_dict[key] / float(total_dict[key]))
        svr_ip_invalid_rates.append((key, float(rate)))
    svr_ip_invalid_rates_top_30 = sorted(svr_ip_invalid_rates, key=lambda x: x[1], reverse=True)[0:30]

    logger.debug("svr_ip_invalid_datas_top_30 %s", svr_ip_invalid_datas_top_30)
    logger.debug("svr_ip_invalid_rates_top_30 %s", svr_ip_invalid_rates_top_30)

    points = list()
    for item in svr_ip_invalid_datas_top_30:
        point = dict()
        point['tags'] = dict()
        point['fields'] = dict()
        point['time'] = start_time
        point['measurement'] = 'svr_ip_hour_count_info'

        point['tags']['ext_svrip_status'] = 'svr_ip_invalid'
        point['tags']['country'] = u'中国'
        point['tags']['province'] = item[0][0]
        point['tags']['city'] = item[0][1]
        point['tags']['isp'] = item[0][2]

        point['fields']['count'] = item[1]
        points.append(point)
    client.write_points(points)

    points = list()
    for item in svr_ip_invalid_rates_top_30:
        point = dict()
        point['tags'] = dict()
        point['fields'] = dict()
        point['time'] = start_time
        point['measurement'] = 'svr_ip_hour_rate_info'

        point['tags']['ext_svrip_status'] = 'svr_ip_invalid'
        point['tags']['country'] = u'中国'
        point['tags']['province'] = item[0][0]
        point['tags']['city'] = item[0][1]
        point['tags']['isp'] = item[0][2]

        point['fields']['rate'] = item[1]
        points.append(point)
    client.write_points(points)


if __name__ == "__main__":

    start = sys.argv[1]
    end = sys.argv[2]
    logger.info("start: %s end: %s", start, end)

    start_time_point = int(str2unixtime(start))
    end_time_point = int(str2unixtime(end))

    # 补全弹幕延迟数据
    for point in range(start_time_point, end_time_point, 3600):
        svr_ip_task(point)





