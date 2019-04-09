# -*- coding: utf-8 -*-
import datetime
import os

import config
from lib import worker


def _year_input():
    """
    接受用户年份输入

    :return:
    """
    # 接收输入并验证
    exit_str = ['q', 'quit', 'exit']
    while True:
        try:
            year_input = int(input(f'请输入需要的年份（例如{datetime.datetime.now().year-1}）：'))
            if year_input in exit_str:
                exit()
            if 2008 < year_input < datetime.datetime.now().year:
                return year_input
            else:
                input(f'输入错误，只支持 [2009-{datetime.datetime.now().year-1}] 年份，按任意键后重新输入。')
        except ValueError:
            input(f'输入错误，只支持 [2009-{datetime.datetime.now().year-1}] 年份，按任意键后重新输入。')


def _check_db_file_exist(year, site_name='stats.gov.cn'):
    """
    检查是否存在数据文件

    :param year: 数据的年份
    :type year: int
    :param site_name: 站点名（默认国家统计局stats.gov.cn）
    :type site_name: str
    :return: 是否存在
    """
    return os.path.isfile(f'{config.ROOT_PATH}data{os.sep}{year}{os.sep}db_{site_name}.sqlite')


def _fetch_stats_gov_cn():
    """
    抓取统计局信息

    :return:
    """
    # 接收输入并验证
    year = _year_input()
    if _check_db_file_exist(year) is True:
        confirm = input(f'{config.ROOT_PATH}data{os.sep}{year}{os.sep}db_stats.gov.cn.sqlite '
                        f'文件已存在，继续抓取会覆盖原文件，是否继续？(y or n) ')
        print(confirm)
        if confirm != 'y' and confirm != 'Y':
            print('Bye.')
            exit()
    print(f'开始 {year} 年统计局信息抓取')
    worker.fetch_stats_gov_cn(
        config.STATS_GOV_CN_SITE.replace('$YEAR$', str(year)),
        f'{config.ROOT_PATH}data{os.sep}{year}{os.sep}',
        config.SHOW_LOG,
        config.CRAWLER_SLEEP_TIME
    )
    print(f'完成 {year} 年统计局信息抓取，数据保存在 {config.ROOT_PATH}data{os.sep}{year}{os.sep}db_stats.gov.cn.sqlite 文件中。')


def _export_csv_stats_gov_cn():
    """
    导出统计局信息到 csv 文件

    :return:
    """
    # 接收输入并验证
    year = _year_input()
    if _check_db_file_exist(year) is False:
        print(f'指定 {year} 年份的数据文件 {config.ROOT_PATH}data{os.sep}{year}{os.sep}db_stats.gov.cn.sqlite 不存在，请确认是否已采集。Bye.')
        exit()
    print(f'开始 {year} 年统计局信息导出 csv 文件')
    worker.export_csv_stats_gov_cn(
        f'{config.ROOT_PATH}data{os.sep}{year}{os.sep}',
        config.SHOW_LOG,
        config.CSV_OUTPUT_FILE_ENCODING
    )
    print(f'完成 {year} 年统计局信息导出 csv 文件，文件在 {config.ROOT_PATH}data{os.sep}{year}{os.sep} 目录下。')


def _export_json_stats_gov_cn():
    """
    导出统计局信息到 json 文件

    :return:
    """
    # 接收输入并验证
    year = _year_input()
    if _check_db_file_exist(year) is False:
        print(f'指定 {year} 年份的数据文件 {config.ROOT_PATH}data{os.sep}{year}{os.sep}db_stats.gov.cn.sqlite 不存在，请确认是否已采集。Bye.')
        exit()
    print(f'开始 {year} 年统计局信息导出 json 文件')
    worker.export_json_stats_gov_cn(
        f'{config.ROOT_PATH}data{os.sep}{year}{os.sep}',
        config.SHOW_LOG,
    )
    print(f'完成 {year} 年统计局信息导出 json 文件，文件在 {config.ROOT_PATH}data{os.sep}{year}{os.sep} 目录下。')


def _export_redis_stats_gov_cn():
    """
    导出统计局信息到 Redis

    :return:
    """
    # 接收输入并验证
    year = _year_input()
    if _check_db_file_exist(year) is False:
        print(f'指定 {year} 年份的数据文件 {config.ROOT_PATH}data{os.sep}{year}{os.sep}db_stats.gov.cn.sqlite 不存在，请确认是否已采集。Bye.')
        exit()
    print(f'开始 {year} 年统计局信息导出到 Redis。')
    worker.export_redis_stats_gov_cn(
        db_path=f'{config.ROOT_PATH}data{os.sep}{year}{os.sep}',
        redis_host=f'{config.REDIS_HOST}',
        redis_port=f'{config.REDIS_PORT}',
        redis_pass=f'{config.REDIS_PASS}',
        redis_db=f'{config.REDIS_DB}',
        ssh_config={
            'host': config.SSH_HOST,
            'port': config.SSH_PORT,
            'username': config.SSH_USERNAME,
            'password': config.SSH_PASSWORD,
            'pkey': config.SSH_PKEY,
            'bind_host': config.SSH_BIND_HOST,
            'bind_port': config.SSH_BIND_PORT,
        },
        show_log=config.SHOW_LOG,
    )
    print(f'完成 {year} 年统计局信息导出到 Redis。{os.linesep}'
          f'Redis 信息如下：{os.linesep}'
          f'HOST: {config.REDIS_HOST}{os.linesep}'
          f'PORT: {config.REDIS_PORT}{os.linesep}'
          f'PASS: {config.REDIS_PASS}{os.linesep}'
          f'DB  : {config.REDIS_DB}')


def main():
    """
    主方法

    :return:
    """
    exit_str = ['q', 'quit', 'exit']
    while True:
        print('1\t抓取统计局信息并保存入库。')
        print('2\t导出统计局信息中所有省、地、县、乡、村数据的 csv 版本。')
        print('3\t导出统计局信息中所有省、地、县、乡、村数据的 json 版本。')
        print('4\t导出统计局信息中所有省、地、县、乡、村数据到 Redis。')
        operate = input('请选择：')
        if operate in exit_str:
            exit()
        elif operate == '1':
            _fetch_stats_gov_cn()
            exit()
        elif operate == '2':
            _export_csv_stats_gov_cn()
            exit()
        elif operate == '3':
            _export_json_stats_gov_cn()
            exit()
        elif operate == '4':
            _export_redis_stats_gov_cn()
            exit()
        else:
            print('输入错误，请重新输入。')


if __name__ == '__main__':
    main()
