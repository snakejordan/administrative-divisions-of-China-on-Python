# -*- coding: utf-8 -*-
import os

ROOT_PATH = os.path.abspath(os.path.dirname(__file__)) + os.path.sep
"""
工程物理根路径

:type: str
"""

STATS_GOV_CN_SITE = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/$YEAR$/$ROUTE$'
"""
国家统计局站点地址，需替换掉抓取的年份（$YEAR$）和路由（$ROUTE$）。

:type: str
"""

SHOW_LOG = True
"""
是否显示详细日志

:type: bool
"""

CRAWLER_SLEEP_TIME = 0
"""
爬虫每次爬取后的休眠时间，单位为秒。

:rtype: int
"""

CSV_OUTPUT_FILE_ENCODING = 'UTF-8'
"""
csv 输出文件的字符编码，默认为 UTF-8，为了 Microsoft Office Excel 可以正常显示可以设置为 GBK，但是 GBK 可能会出现字符编码异常导致程序运行失败。

:type: str
"""

REDIS_HOST = '127.0.0.1'
"""
Redis 地址

:type: str
"""
REDIS_PORT = 6379
"""
Redis 端口

:type: int
"""
REDIS_PASS = ''
"""
Redis 密码

:type: str
"""
REDIS_DB = 3
"""
Redis 库号

:type: int
"""

SSH_HOST = ''
"""
SSH 隧道地址

:type: str
"""
SSH_PORT = 22
"""
SSH 隧道端口

:type: int
"""
SSH_USERNAME = ''
"""
SSH 隧道用户名

:type: str
"""
SSH_PASSWORD = ''
"""
SSH 隧道密码

:type: str
"""
SSH_PKEY = ''
"""
SSH 隧道证书文件（完整物理路径）

:type: str
"""
SSH_BIND_HOST = '127.0.0.1'
"""
SSH 隧道绑定地址

:type: str
"""
SSH_BIND_PORT = 6379
"""
SSH 隧道绑定端口

:type: int
"""

if __name__ == '__main__':
    pass
