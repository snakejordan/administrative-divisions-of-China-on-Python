# 中华人民共和国行政区划数据
[![license](https://img.shields.io/github/license/snakejordan/administrative-divisions-of-China-on-Python.svg)](https://github.com/snakejordan/administrative-divisions-of-China-on-Python/blob/master/LICENSE)
[![author](https://img.shields.io/badge/Author-Snake-blue.svg)](https://ty289.com/)
## 环境要求
* macOS or Linux or Windows
* python (3.6+)
* pip
* sqlite3
## 依赖包
[![requests_html](https://img.shields.io/pypi/v/requests_html.svg?label=requests_html)](https://pypi.org/project/requests_html/)
[![pyquery](https://img.shields.io/pypi/v/pyquery.svg?label=pyquery)](https://pypi.org/project/pyquery/)
[![sshtunnel](https://img.shields.io/pypi/v/sshtunnel.svg?label=sshtunnel)](https://pypi.org/project/sshtunnel/)
[![redis](https://img.shields.io/pypi/v/redis.svg?label=redis)](https://pypi.org/project/redis/)
## 数据来源
#### 数据来源说明：
中华人民共和国行政区划官方数据分为两个渠道（本人已知渠道，如有其它官方渠道可通过 [issues](https://github.com/snakejordan/administrative-divisions-of-China-on-Python/issues) 进行交流），分别为[民政部](http://www.mca.gov.cn/article/sj/xzqh/)和[统计局](http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/)，本项目采用统计局数据。
#### 数据来源区别：
* 民政部：
    * 数据历史久远，包括从 1980 年至今行政区划数据；
    * 数据层级较少，最多分为省、市、区三级数据。

* 统计局：
    * 数据历史较新，包括从 2009 年至今行政区划数据；
    * 数据层级较多，最多分为省级、地级、县级、乡级、村级五级数据。
#### 本项目数据源：
为了数据更详细，所以采用[统计局](http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/)数据，具体数据源链接如下：
* [2018 年](http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/)
* [2017 年](http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2017/)
* [2016 年](http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2016/)
* [2015 年](http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2015/)
* [2014 年](http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2014/)
* [2013 年](http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2013/)
* [2012 年](http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2012/)
* [2011 年](http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2011/)
* [2010 年](http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2010/)
* [2009 年](http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2009/)
#### 统计用区划代码和城乡划分代码编制规则：
参考：[统计用区划代码和城乡划分代码编制规则](http://www.stats.gov.cn/tjsj/tjbz/200911/t20091125_8667.html)
## 配置文件
```python
# 为 True 会详细显示操作中的每句日志内容。
SHOW_LOG = True

# 爬虫每次爬取后的休眠时间，单位为秒，正常情况下无需休眠。
CRAWLER_SLEEP_TIME = 0

# csv 输出文件的字符编码，默认为 UTF-8，为了 Microsoft Office Excel 可以正常显示可以设置为 GBK，但是 GBK 可能会出现字符编码异常导致程序运行失败。
CSV_OUTPUT_FILE_ENCODING = 'UTF-8'

# Redis 地址
REDIS_HOST = '127.0.0.1'
# Redis 端口
REDIS_PORT = 6379
# Redis 密码
REDIS_PASS = ''
# Redis 库号
REDIS_DB = 3

# Redis 服务器在本地不可访问的远程服务器时，可通过配置 ssh 隧道的方式访问 Redis，从而保存数据到远程 Redis 服务器。
# SSH 隧道地址
SSH_HOST = ''
# SSH 隧道端口
SSH_PORT = 22
# SSH 隧道用户名
SSH_USERNAME = ''
# SSH 隧道密码
SSH_PASSWORD = ''
# SSH 隧道证书文件（完整物理路径）
SSH_PKEY = ''
# SSH 隧道绑定地址
SSH_BIND_HOST = '127.0.0.1'
# SSH 隧道绑定端口
SSH_BIND_PORT = 6379
```
## 数据格式
```sqlite
-- 省级
CREATE TABLE `province` (`statistical_code` CHAR(12) PRIMARY KEY, `code` CHAR(2), `name` VARCHAR(100))
```
```sqlite
-- 地级
CREATE TABLE `city` (`statistical_code` CHAR(12) PRIMARY KEY, `code` CHAR(4), `name` VARCHAR(100), `province_statistical_code` CHAR(12))
```
```sqlite
-- 县级
CREATE TABLE `county` (`statistical_code` CHAR(12) PRIMARY KEY, `code` CHAR(6), `name` VARCHAR(100), `province_statistical_code` CHAR(12), `city_statistical_code` CHAR(12))
```
```sqlite
-- 乡级
CREATE TABLE `town` (`statistical_code` CHAR(12) PRIMARY KEY, `code` CHAR(9), `name` VARCHAR(100), `province_statistical_code` CHAR(12), `city_statistical_code` CHAR(12), `county_statistical_code` CHAR(12))
```
```sqlite
-- 村级
CREATE TABLE `village` (`statistical_code` CHAR(12) PRIMARY KEY, `code` CHAR(12), `name` VARCHAR(100), `province_statistical_code` CHAR(12), `city_statistical_code` CHAR(12), `county_statistical_code` CHAR(12), `town_statistical_code` CHAR(12))
```
![数据格式](https://raw.githubusercontent.com/snakejordan/static-file/master/administrative-divisions-of-China-on-Python/doc/images/sqlite_data_structure.png "数据格式")
## 使用说明
完成环境配置及依赖安装后，可通过运行 main.py 文件的方式运行本项目，本项目运行后采用交互式命令行进行交互提示。
#### 运行命令：
```cmd
$ python3 main.py
```
#### 功能列表：
* 抓取统计局信息并保存入库。（输入1）
* 导出统计局信息中所有省、地、县、乡、村数据的 csv 版本。（输入2）
* 导出统计局信息中所有省、地、县、乡、村数据的 json 版本。（输入3）
* 导出统计局信息中所有省、地、县、乡、村数据到 Redis。（输入4）
#### 运行示例：
![运行示例](https://raw.githubusercontent.com/snakejordan/static-file/master/administrative-divisions-of-China-on-Python/doc/images/running_example.gif "运行示例")
## 在线接口
#### 五级在线接口地址：
* 省级：https://pcctv.public.ty289.com/stats/province
* 地级：https://pcctv.public.ty289.com/stats/city
* 县级：https://pcctv.public.ty289.com/stats/county
* 乡级：https://pcctv.public.ty289.com/stats/town
* 村级：https://pcctv.public.ty289.com/stats/village
#### 请求参数：
除省级无需请求参数以外，其它四级均需要名称为 topCode 的上级编号作为请求参数，topCode 的值为上一级数据的 statistical_code 值。
#### 请求方法：
支持 GET POST PUT 等请求方法，支持 XHR fetch 等请求方式。
#### Postman 示例：
省级示例：

![省级示例](https://raw.githubusercontent.com/snakejordan/static-file/master/administrative-divisions-of-China-on-Python/doc/images/api_example_province.png "省级示例")

地级示例：

![地级示例](https://raw.githubusercontent.com/snakejordan/static-file/master/administrative-divisions-of-China-on-Python/doc/images/api_example_city.png "地级示例")

县级示例：

![县级示例](https://raw.githubusercontent.com/snakejordan/static-file/master/administrative-divisions-of-China-on-Python/doc/images/api_example_county.png "县级示例")

乡级示例：

![乡级示例](https://raw.githubusercontent.com/snakejordan/static-file/master/administrative-divisions-of-China-on-Python/doc/images/api_example_town.png "乡级示例")

村级示例：

![村级示例](https://raw.githubusercontent.com/snakejordan/static-file/master/administrative-divisions-of-China-on-Python/doc/images/api_example_village.png "村级示例")
#### 特例：
部分地区不存在五级数据，为保证一致性接口统一返回五级数据，本级数据与上级数据的 statistical_code 相同时意味着层级不足五级，需调用者进行额外处理。

例如：

广东省（440000000000） -> 东莞市（441900000000） -> 东城街道办事处（441900003000） -> 岗贝社区居民委员会（441900003001）

接口调用及返回参考下图：

东莞市省级示例：

![东莞市省级示例](https://raw.githubusercontent.com/snakejordan/static-file/master/administrative-divisions-of-China-on-Python/doc/images/api_example_province_dongguan.png "东莞市省级示例")

东莞市地级示例：

![东莞市地级示例](https://raw.githubusercontent.com/snakejordan/static-file/master/administrative-divisions-of-China-on-Python/doc/images/api_example_city_dongguan.png "东莞市地级示例")

东莞市县级示例：

![东莞市县级示例](https://raw.githubusercontent.com/snakejordan/static-file/master/administrative-divisions-of-China-on-Python/doc/images/api_example_county_dongguan.png "东莞市县级示例")

东莞市乡级示例：

![东莞市乡级示例](https://raw.githubusercontent.com/snakejordan/static-file/master/administrative-divisions-of-China-on-Python/doc/images/api_example_town_dongguan.png "东莞市乡级示例")

东莞市村级示例：

![东莞市村级示例](https://raw.githubusercontent.com/snakejordan/static-file/master/administrative-divisions-of-China-on-Python/doc/images/api_example_village_dongguan.png "东莞市村级示例")
## 下载数据
已完成以下年份（2009、2010、2011、2012、2013、2014、2015、2016、2017、2018）的 sqlite3 格式数据采集，需要 sqlite3 格式数据可通过邮件 [snakejordan@gmail.com](mailto:snakejordan@gmail.com) 索取。
> 提示：邮件回复的下载链接为 google drive 中存储的文件，是否能下载你懂得🙃。
## Over
恩，以上是所有说明，后面没有要说的了😊。

-----

## 特别鸣谢
本项目是受 [modood](https://github.com/modood) 的 [Administrative-divisions-of-China](https://github.com/modood/Administrative-divisions-of-China) 项目启发而编写，由于不擅长 NodeJS 以及觉得 [Administrative-divisions-of-China](https://github.com/modood/Administrative-divisions-of-China) 在数据及使用便利性上有一点点问题，所以重复造了个轮子，在此对 [modood](https://github.com/modood) 表示特别鸣谢。