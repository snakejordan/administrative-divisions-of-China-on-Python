# -*- coding: utf-8 -*-
import csv
import datetime
import json
import math
import time

import redis
from sshtunnel import SSHTunnelForwarder

from lib.crawler import StatsGovCn
from lib.util import DBUtilStatsGovCn


def fetch_stats_gov_cn(url, db_path, show_log=True, sleep_time=0):
    """
    采集统计局信息

    :param url: 统计局信息根网址
    :type url: str
    :param db_path: SQLite数据库路径
    :type db_path: str
    :param show_log: 是否显示日志
    :type show_log: bool
    :param sleep_time: 爬虫每次爬取后的休眠时间，单位为秒。
    :type sleep_time: int
    :return:
    """
    # 程序开始时间
    begin_time = time.time()

    stats_gov_cn_crawler = StatsGovCn()
    stats_gov_cn_crawler.sleep_time = sleep_time
    if stats_gov_cn_crawler.check(url.replace('$ROUTE$', 'index.html'))[0] != 'province':
        raise Exception('不是省级信息页面')

    # 数据库操作对象
    db_util = DBUtilStatsGovCn(db_path + 'db_stats.gov.cn.sqlite')
    # 上级 URL 地址
    url_base = {}
    url_base_temp = ''

    # 抓取并保存省级信息
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始抓取并保存省级信息')
    provinces = stats_gov_cn_crawler.province(url.replace('$ROUTE$', 'index.html'))
    db_util.truncate_province()
    for province in provinces:
        db_util.insert_province(province['statistical_code'], province['code'], province['name'])
        url_base[province['statistical_code']] = url.replace('$ROUTE$', '')
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成抓取并保存省级信息')
    print(f'[REPORT] 省级信息 {len(provinces)} 个')

    # 抓取并保存地级信息
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始抓取并保存地级信息')
    cities = []
    db_util.truncate_city()
    for province in provinces:
        if show_log:
            province_name_temp = province['name']
            print(f'[Log][{datetime.datetime.now()}] [{provinces.index(province) + 1}/{len(provinces)}] '
                  f'开始抓取并保存【{province_name_temp}】')
        if province['href'] != '':
            url_base_temp = url_base[province['statistical_code']] + province['href']
            cities_temp = stats_gov_cn_crawler.city(url_base_temp)
            for city in cities_temp:
                cities.append(city)
                db_util.insert_city(city['statistical_code'], city['code'], city['name'], province['statistical_code'])
                url_base[city['statistical_code']] = url_base_temp[0:url_base_temp.rfind('/')+1]
        if show_log:
            province_name_temp = province['name']
            print(f'[Log][{datetime.datetime.now()}] [{provinces.index(province) + 1}/{len(provinces)}] '
                  f'完成抓取并保存【{province_name_temp}】')
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成抓取并保存地级信息')
    print(f'[REPORT] 地级信息 {len(cities)} 个')

    # 抓取并保存县级信息
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始抓取并保存县级信息')
    counties = []
    db_util.truncate_county()
    for city in cities:
        city_db_temp = db_util.select_city(city['statistical_code'])
        if show_log:
            province_name_temp = db_util.select_province(city_db_temp['province_statistical_code'])['name']
            city_name_temp = city['name']
            print(f'[Log][{datetime.datetime.now()}] [{cities.index(city) + 1}/{len(cities)}] '
                  f'开始抓取并保存【{province_name_temp}】【{city_name_temp}】')
        if city['href'] != '':
            try:
                url_base_temp = url_base[city['statistical_code']] + city['href']
                counties_temp = stats_gov_cn_crawler.county(url_base_temp)
            except Exception as e:
                if e.args[0] == '不是县级信息页面':
                    counties_temp = [{
                        'href': city['href'][city['href'].find('/')+1:],
                        'statistical_code': city['statistical_code'],
                        'code': city['statistical_code'][0:6],
                        'name': city['name']
                    }]
                else:
                    raise e
            for county in counties_temp:
                counties.append(county)
                db_util.insert_county(
                    county['statistical_code'],
                    county['code'],
                    county['name'],
                    city_db_temp['province_statistical_code'],
                    city['statistical_code']
                )
                url_base[county['statistical_code']] = url_base_temp[0:url_base_temp.rfind('/')+1]
        if show_log:
            province_name_temp = db_util.select_province(city_db_temp['province_statistical_code'])['name']
            city_name_temp = city['name']
            print(f'[Log][{datetime.datetime.now()}] [{cities.index(city) + 1}/{len(cities)}] '
                  f'完成抓取并保存【{province_name_temp}】【{city_name_temp}】')
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成抓取并保存县级信息')
    print(f'[REPORT] 县级信息 {len(counties)} 个')

    # 抓取并保存乡级信息
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始抓取并保存乡级信息')
    towns = []
    db_util.truncate_town()
    for county in counties:
        county_db_temp = db_util.select_county(county['statistical_code'])
        if show_log:
            province_name_temp = db_util.select_province(county_db_temp['province_statistical_code'])['name']
            city_name_temp = db_util.select_city(county_db_temp['city_statistical_code'])['name']
            county_name_temp = county['name']
            print(f'[Log][{datetime.datetime.now()}] [{counties.index(county) + 1}/{len(counties)}] '
                  f'开始抓取并保存【{province_name_temp}】【{city_name_temp}】【{county_name_temp}】')
        if county['href'] != '':
            try:
                url_base_temp = url_base[county['statistical_code']] + county['href']
                towns_temp = stats_gov_cn_crawler.town(url_base_temp)
            except Exception as e:
                if e.args[0] == '不是乡级信息页面':
                    towns_temp = [{
                        'href': county['href'][county['href'].find('/')+1:],
                        'statistical_code': county['statistical_code'],
                        'code': county['statistical_code'][0:9],
                        'name': county['name']
                    }]
                else:
                    raise e
            for town in towns_temp:
                towns.append(town)
                db_util.insert_town(
                    town['statistical_code'],
                    town['code'],
                    town['name'],
                    county_db_temp['province_statistical_code'],
                    county_db_temp['city_statistical_code'],
                    county['statistical_code']
                )
                url_base[town['statistical_code']] = url_base_temp[0:url_base_temp.rfind('/')+1]
        if show_log:
            province_name_temp = db_util.select_province(county_db_temp['province_statistical_code'])['name']
            city_name_temp = db_util.select_city(county_db_temp['city_statistical_code'])['name']
            county_name_temp = county['name']
            print(f'[Log][{datetime.datetime.now()}] [{counties.index(county) + 1}/{len(counties)}] '
                  f'完成抓取并保存【{province_name_temp}】【{city_name_temp}】【{county_name_temp}】')
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成抓取并保存乡级信息')
    print(f'[REPORT] 乡级信息 {len(towns)} 个')

    # 抓取并保存村级信息
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始抓取并保存村级信息')
    villages = []
    db_util.truncate_village()
    for town in towns:
        town_db_temp = db_util.select_town(town['statistical_code'])
        if show_log:
            province_name_temp = db_util.select_province(town_db_temp['province_statistical_code'])['name']
            city_name_temp = db_util.select_city(town_db_temp['city_statistical_code'])['name']
            county_name_temp = db_util.select_county(town_db_temp['county_statistical_code'])['name']
            town_name_temp = town['name']
            print(f'[Log][{datetime.datetime.now()}] [{towns.index(town) + 1}/{len(towns)}] '
                  f'开始抓取并保存【{province_name_temp}】【{city_name_temp}】【{county_name_temp}】【{town_name_temp}】')
        if town['href'] != '':
            try:
                villages_temp = stats_gov_cn_crawler.village(url_base[town['statistical_code']] + town['href'])
            except Exception as e:
                if e.args[0] == '不是村级信息页面':
                    villages_temp = []
                else:
                    raise e
            for village in villages_temp:
                villages.append(village)
                db_util.insert_village(
                    village['statistical_code'],
                    village['code'],
                    village['name'],
                    town_db_temp['province_statistical_code'],
                    town_db_temp['city_statistical_code'],
                    town_db_temp['county_statistical_code'],
                    town['statistical_code']
                )
        if show_log:
            province_name_temp = db_util.select_province(town_db_temp['province_statistical_code'])['name']
            city_name_temp = db_util.select_city(town_db_temp['city_statistical_code'])['name']
            county_name_temp = db_util.select_county(town_db_temp['county_statistical_code'])['name']
            town_name_temp = town['name']
            print(f'[Log][{datetime.datetime.now()}] [{towns.index(town) + 1}/{len(towns)}] '
                  f'完成抓取并保存【{province_name_temp}】【{city_name_temp}】【{county_name_temp}】【{town_name_temp}】')
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成抓取并保存村级信息')
    print(f'[REPORT] 村级信息 {len(villages)} 个')
    # 程序结束时间
    end_time = time.time()
    print(f'[REPORT] 程序运行开始于 {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(begin_time))} '
          f'结束于 {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))} '
          f'总计用时 {int(end_time - begin_time)} 秒')


def export_csv_stats_gov_cn(db_path, show_log=True, encoding='UTF-8'):
    """
    导出统计局信息到 csv 文件

    :param db_path: SQLite数据库路径
    :type db_path: str
    :param show_log: 是否显示日志
    :type show_log: bool
    :param show_log: 输入文件的字符编码
    :type encoding: str
    :return:
    """
    # 程序开始时间
    begin_time = time.time()

    # 数据库操作对象
    db_util = DBUtilStatsGovCn(db_path + 'db_stats.gov.cn.sqlite')

    limit = 100

    header_province = ['statistical_code', 'code', 'name']
    header_city = ['statistical_code', 'code', 'name', 'province_statistical_code']
    header_county = ['statistical_code', 'code', 'name', 'province_statistical_code', 'city_statistical_code']
    header_town = ['statistical_code', 'code', 'name', 'province_statistical_code', 'city_statistical_code',
                   'county_statistical_code']
    header_village = ['statistical_code', 'code', 'name', 'province_statistical_code', 'city_statistical_code',
                      'county_statistical_code', 'town_statistical_code']

    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始导出省级信息数据')
    count_province = db_util.select_count_province()
    with open(f'{db_path}province_stats.gov.cn.csv', 'w', encoding=encoding) as csv_file:
        csv_province = csv.DictWriter(csv_file, header_province)
        csv_province.writeheader()
        for i in range(0, math.ceil(count_province / limit)):
            csv_province.writerows(db_util.select_provinces(limit, i * limit))
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成导出省级信息数据')
    print(f'[REPORT] 省级信息导出完成 {db_path}province_stats.gov.cn.csv')

    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始导出地级信息数据')
    count_city = db_util.select_count_city()
    with open(f'{db_path}city_stats.gov.cn.csv', 'w', encoding=encoding) as csv_file:
        csv_city = csv.DictWriter(csv_file, header_city)
        csv_city.writeheader()
        for i in range(0, math.ceil(count_city / limit)):
            csv_city.writerows(db_util.select_cities(limit, i * limit))
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成导出地级信息数据')
    print(f'[REPORT] 地级信息导出完成 {db_path}city_stats.gov.cn.csv')

    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始导出县级信息数据')
    count_county = db_util.select_count_county()
    with open(f'{db_path}county_stats.gov.cn.csv', 'w', encoding=encoding) as csv_file:
        csv_county = csv.DictWriter(csv_file, header_county)
        csv_county.writeheader()
        for i in range(0, math.ceil(count_county / limit)):
            csv_county.writerows(db_util.select_counties(limit, i * limit))
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成导出县级信息数据')
    print(f'[REPORT] 县级信息导出完成 {db_path}county_stats.gov.cn.csv')

    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始导出乡级信息数据')
    count_town = db_util.select_count_town()
    with open(f'{db_path}town_stats.gov.cn.csv', 'w', encoding=encoding) as csv_file:
        csv_town = csv.DictWriter(csv_file, header_town)
        csv_town.writeheader()
        for i in range(0, math.ceil(count_town / limit)):
            csv_town.writerows(db_util.select_towns(limit, i * limit))
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成导出乡级信息数据')
    print(f'[REPORT] 乡级信息导出完成 {db_path}town_stats.gov.cn.csv')

    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始导出村级信息数据')
    count_village = db_util.select_count_village()
    with open(f'{db_path}village_stats.gov.cn.csv', 'w', encoding=encoding) as csv_file:
        csv_village = csv.DictWriter(csv_file, header_village)
        csv_village.writeheader()
        for i in range(0, math.ceil(count_village / limit)):
            csv_village.writerows(db_util.select_villages(limit, i * limit))
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成导出村级信息数据')
    print(f'[REPORT] 村级信息导出完成 {db_path}village_stats.gov.cn.csv')

    # 程序结束时间
    end_time = time.time()
    print(f'[REPORT] 程序运行开始于 {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(begin_time))} '
          f'结束于 {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))} '
          f'总计用时 {int(end_time - begin_time)} 秒')


def export_json_stats_gov_cn(db_path, show_log=True):
    """
    导出统计局信息到 json 文件

    :param db_path: SQLite数据库路径
    :type db_path: str
    :param show_log: 是否显示日志
    :type show_log: bool
    :return:
    """
    # 程序开始时间
    begin_time = time.time()

    # 数据库操作对象
    db_util = DBUtilStatsGovCn(db_path + 'db_stats.gov.cn.sqlite')

    limit = 100

    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始导出省级信息数据')
    count_province = db_util.select_count_province()
    with open(f'{db_path}province_stats.gov.cn.json', 'w') as json_file:
        province_db = []
        for i in range(0, math.ceil(count_province / limit)):
            province_db += db_util.select_provinces(limit, i * limit)
        json.dump(province_db, json_file)
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成导出省级信息数据')
    print(f'[REPORT] 省级信息导出完成 {db_path}province_stats.gov.cn.json')

    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始导出地级信息数据')
    count_city = db_util.select_count_city()
    with open(f'{db_path}city_stats.gov.cn.json', 'w') as json_file:
        city_db = []
        for i in range(0, math.ceil(count_city / limit)):
            city_db += db_util.select_cities(limit, i * limit)
        json.dump(city_db, json_file)
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成导出地级信息数据')
    print(f'[REPORT] 地级信息导出完成 {db_path}city_stats.gov.cn.json')

    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始导出县级信息数据')
    count_county = db_util.select_count_county()
    with open(f'{db_path}county_stats.gov.cn.json', 'w') as json_file:
        county_db = []
        for i in range(0, math.ceil(count_county / limit)):
            county_db += db_util.select_counties(limit, i * limit)
        json.dump(county_db, json_file)
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成导出县级信息数据')
    print(f'[REPORT] 县级信息导出完成 {db_path}county_stats.gov.cn.json')

    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始导出乡级信息数据')
    count_town = db_util.select_count_town()
    with open(f'{db_path}town_stats.gov.cn.json', 'w') as json_file:
        town_db = []
        for i in range(0, math.ceil(count_town / limit)):
            town_db += db_util.select_towns(limit, i * limit)
        json.dump(town_db, json_file)
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成导出乡级信息数据')
    print(f'[REPORT] 乡级信息导出完成 {db_path}town_stats.gov.cn.json')

    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始导出村级信息数据')
    count_village = db_util.select_count_village()
    with open(f'{db_path}village_stats.gov.cn.json', 'w') as json_file:
        village_db = []
        for i in range(0, math.ceil(count_village / limit)):
            village_db += db_util.select_villages(limit, i * limit)
        json.dump(village_db, json_file)
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成导出村级信息数据')
    print(f'[REPORT] 村级信息导出完成 {db_path}village_stats.gov.cn.json')

    # 程序结束时间
    end_time = time.time()
    print(f'[REPORT] 程序运行开始于 {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(begin_time))} '
          f'结束于 {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))} '
          f'总计用时 {int(end_time - begin_time)} 秒')


def export_redis_stats_gov_cn(db_path, redis_host, redis_port, redis_pass, redis_db, ssh_config=None, show_log=True):
    """
    导出统计局信息到 Redis

    :param db_path: SQLite数据库路径
    :type db_path: str
    :param redis_host: Redis Host
    :type redis_host: str
    :param redis_port: Redis Port
    :type redis_port: str
    :param redis_pass: Redis Pass
    :type redis_pass: str
    :param redis_db: Redis db
    :type redis_db: str
    :param ssh_config: SSH 隧道配置
    :type ssh_config: dict
    :param show_log: 是否显示日志
    :type show_log: bool
    :return:
    """
    # 程序开始时间
    begin_time = time.time()

    # SSH 隧道
    ssh_server = None
    if ssh_config is not None:
        ssh_server = SSHTunnelForwarder(
            ssh_address_or_host=(ssh_config['host'], ssh_config['port']),
            ssh_username=ssh_config['username'],
            ssh_password=ssh_config['password'],
            ssh_pkey=ssh_config['pkey'],
            remote_bind_address=(ssh_config['bind_host'], ssh_config['bind_port'])
        )
        ssh_server.start()
        redis_host = ssh_server.local_bind_host
        redis_port = ssh_server.local_bind_port

    # 数据库操作对象
    db_util = DBUtilStatsGovCn(db_path + 'db_stats.gov.cn.sqlite')
    # redis 链接对象
    redis_util = redis.StrictRedis(
        connection_pool=redis.ConnectionPool(
            host=redis_host,
            port=redis_port,
            password=redis_pass,
            db=redis_db,
            decode_responses=True
        )
    )

    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始导出省级信息数据')
    count_province = db_util.select_count_province()
    province_db = db_util.select_provinces(count_province, 0)
    redis_util.set('stats.gov.cn_province', json.dumps(province_db))
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成导出省级信息数据')
    print(f'[REPORT] 省级信息导出完成 key: stats.gov.cn_province')

    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始导出地级信息数据')
    city_db = []
    for province in province_db:
        if show_log:
            print(f'[Log][{datetime.datetime.now()}] [{province_db.index(province) + 1}/{len(province_db)}] 地级信息')
        cities_temp = db_util.select_cities_by_top(province['statistical_code'])
        city_db += cities_temp
        redis_util.hset('stats.gov.cn_city', province['statistical_code'], json.dumps(cities_temp))
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成导出地级信息数据')
    print(f'[REPORT] 地级信息导出完成 key: stats.gov.cn_city')

    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始导出县级信息数据')
    county_db = []
    for city in city_db:
        if show_log:
            print(f'[Log][{datetime.datetime.now()}] [{city_db.index(city) + 1}/{len(city_db)}] 县级信息')
        counties_temp = db_util.select_counties_by_top(city['statistical_code'])
        county_db += counties_temp
        redis_util.hset('stats.gov.cn_county', city['statistical_code'], json.dumps(counties_temp))
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成导出县级信息数据')
    print(f'[REPORT] 县级信息导出完成 key: stats.gov.cn_county')

    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始导出乡级信息数据')
    town_db = []
    for county in county_db:
        if show_log:
            print(f'[Log][{datetime.datetime.now()}] [{county_db.index(county) + 1}/{len(county_db)}] 乡级信息')
        towns_temp = db_util.select_towns_by_top(county['statistical_code'])
        town_db += towns_temp
        redis_util.hset('stats.gov.cn_town', county['statistical_code'], json.dumps(towns_temp))
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成导出乡级信息数据')
    print(f'[REPORT] 乡级信息导出完成 key: stats.gov.cn_town')

    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 开始导出村级信息数据')
    for town in town_db:
        if show_log:
            print(f'[Log][{datetime.datetime.now()}] [{town_db.index(town) + 1}/{len(town_db)}] 村级信息')
        villages_temp = db_util.select_villages_by_top(town['statistical_code'])
        redis_util.hset('stats.gov.cn_village', town['statistical_code'], json.dumps(villages_temp))
    if show_log:
        print(f'[Log][{datetime.datetime.now()}] 完成导出村级信息数据')
    print(f'[REPORT] 乡级信息导出完成 key: stats.gov.cn_village')

    if ssh_config is not None:
        ssh_server.close()

    # 程序结束时间
    end_time = time.time()
    print(f'[REPORT] 程序运行开始于 {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(begin_time))} '
          f'结束于 {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))} '
          f'总计用时 {int(end_time - begin_time)} 秒')


if __name__ == '__main__':
    pass
