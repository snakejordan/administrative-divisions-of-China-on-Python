# -*- coding: utf-8 -*-
import copy
import time

from pyquery import PyQuery
from requests_html import HTMLSession


class CrawlerBase(object):
    """
    爬虫基类
    """

    def __init__(self):
        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
            'Accept-Encoding': ''
        }
        """
        头信息
        """

        self._session = HTMLSession()
        """
        HTMLSession 对象
        """


class StatsGovCn(CrawlerBase):
    """
    国家统计局爬虫
    """

    def __init__(self):
        super(StatsGovCn, self).__init__()
        self._sleep_time = 0

    @property
    def sleep_time(self):
        return self._sleep_time

    @sleep_time.setter
    def sleep_time(self, value):
        if isinstance(value, int):
            self._sleep_time = value

    def province(self, url):
        """
        国家统计局省级抓取爬虫

        :param url: 抓取链接
        :type url: str
        :return: 省份信息数组，数组内元素包括链接、代码、名称、统计用区划代码。
        """

        retry = 3
        while True:
            # 有的节点有链接但其实是 404 页面，也就是并没有下级信息了，所以捕获 404 异常并直接返回空数据。
            try:
                check_result = self.check(url)
                if check_result[0] != 'province':
                    retry -= 1
                    if retry < 0:
                        raise Exception('不是省级信息页面')
                    else:
                        print('[Error] 不是省级信息页面吗？休眠 10 秒再试一次。')
                        time.sleep(10)
                else:
                    doc = check_result[1]
                    break
            except Exception as e:
                # 如果错误信息是 404 则直接返回空信息
                if e.args[0].find('404') != -1:
                    return []
                else:
                    raise e

        try:
            result = []
            province_trs = doc.find('.provincetr')
            for province_tr in province_trs.items():
                province_tds = province_tr.find('td')
                for province_td in province_tds.items():
                    province_el = province_td.find('a')
                    if province_el:
                        href_temp = province_el.attr('href')
                        province = {
                            'href': href_temp,
                            'statistical_code': href_temp[0:2].ljust(12, '0'),
                            'code': href_temp[0:2],
                            'name': province_el.text()
                        }
                        result.append(province)
                    else:
                        name_temp = province_td.text()
                        if name_temp != '':
                            province = {
                                'href': '',
                                'statistical_code': name_temp.ljust(12, '0'),
                                'code': '',
                                'name': name_temp
                            }
                            result.append(province)
            time.sleep(self._sleep_time)
            return result
        except Exception as e:
            print(e)
            print('[Error] province出错，休眠 30 秒重试。')
            time.sleep(30)
            self.province(url)

    def city(self, url):
        """
        国家统计局地级抓取爬虫

        :param url: 抓取链接
        :type url: str
        :return: 地级信息数组，数组内元素包括链接、代码、名称、统计用区划代码。
        """

        retry = 3
        while True:
            # 有的节点有链接但其实是 404 页面，也就是并没有下级信息了，所以捕获 404 异常并直接返回空数据。
            try:
                check_result = self.check(url)
                if check_result[0] != 'city':
                    retry -= 1
                    if retry < 0:
                        raise Exception('不是地级信息页面')
                    else:
                        print('[Error] 不是地级信息页面吗？休眠 10 秒再试一次。')
                        time.sleep(10)
                else:
                    doc = check_result[1]
                    break
            except Exception as e:
                # 如果错误信息是 404 则直接返回空信息
                if e.args[0].find('404') != -1:
                    return []
                else:
                    raise e

        try:
            result = []
            city_trs = doc.find('.citytr')
            for city_tr in city_trs.items():
                city_tds = city_tr.find('td')
                if city_tds.length != 2:
                    raise Exception(f'地级信息页面节点错误，url: {url}')
                city_td0 = city_tds.eq(0)
                city_td1 = city_tds.eq(1)
                href_temp = ''
                if city_td0.find('a'):
                    href_temp = city_td0.find('a').attr('href')
                city = {
                    'href': href_temp,
                    'statistical_code': city_td0.text().ljust(12, '0'),
                    'code': city_td0.text()[0:4],
                    'name': city_td1.text()
                }
                result.append(city)
            time.sleep(self._sleep_time)
            return result
        except Exception as e:
            print(e)
            print('[Error] city出错，休眠 30 秒重试。')
            time.sleep(30)
            self.city(url)

    def county(self, url):
        """
        国家统计局县级抓取爬虫

        :param url: 抓取链接
        :type url: str
        :return: 县级信息数组，数组内元素包括链接、代码、名称、统计用区划代码。
        """

        retry = 3
        while True:
            # 有的节点有链接但其实是 404 页面，也就是并没有下级信息了，所以捕获 404 异常并直接返回空数据。
            try:
                check_result = self.check(url)
                if check_result[0] != 'county':
                    retry -= 1
                    if retry < 0:
                        raise Exception('不是县级信息页面')
                    else:
                        print('[Error] 不是县级信息页面吗？休眠 10 秒再试一次。')
                        time.sleep(10)
                else:
                    doc = check_result[1]
                    break
            except Exception as e:
                # 如果错误信息是 404 则直接返回空信息
                if e.args[0].find('404') != -1:
                    return []
                else:
                    raise e

        try:
            result = []
            county_trs = doc.find('.countytr')
            for county_tr in county_trs.items():
                county_tds = county_tr.find('td')
                if county_tds.length != 2:
                    raise Exception(f'县级信息页面节点错误，url: {url}')
                county_td0 = county_tds.eq(0)
                county_td1 = county_tds.eq(1)
                href_temp = ''
                if county_td0.find('a'):
                    href_temp = county_td0.find('a').attr('href')
                county = {
                    'href': href_temp,
                    'statistical_code': county_td0.text().ljust(12, '0'),
                    'code': county_td0.text()[0:6],
                    'name': county_td1.text()
                }
                result.append(county)
            time.sleep(self._sleep_time)
            return result
        except Exception as e:
            print(e)
            print('[Error] county出错，休眠 30 秒重试。')
            time.sleep(30)
            self.county(url)

    def town(self, url):
        """
        国家统计局乡级抓取爬虫

        :param url: 抓取链接
        :type url: str
        :return: 乡级信息数组，数组内元素包括链接、代码、名称、统计用区划代码。
        """

        retry = 3
        while True:
            # 有的节点有链接但其实是 404 页面，也就是并没有下级信息了，所以捕获 404 异常并直接返回空数据。
            try:
                check_result = self.check(url)
                if check_result[0] != 'town':
                    retry -= 1
                    if retry < 0:
                        raise Exception('不是乡级信息页面')
                    else:
                        print('[Error] 不是乡级信息页面吗？休眠 10 秒再试一次。')
                        time.sleep(10)
                else:
                    doc = check_result[1]
                    break
            except Exception as e:
                # 如果错误信息是 404 则直接返回空信息
                if e.args[0].find('404') != -1:
                    return []
                else:
                    raise e

        try:
            result = []
            town_trs = doc.find('.towntr')
            for town_tr in town_trs.items():
                town_tds = town_tr.find('td')
                if town_tds.length != 2:
                    raise Exception(f'乡级信息页面节点错误，url: {url}')
                town_td0 = town_tds.eq(0)
                town_td1 = town_tds.eq(1)
                href_temp = ''
                if town_td0.find('a'):
                    href_temp = town_td0.find('a').attr('href')
                town = {
                    'href': href_temp,
                    'statistical_code': town_td0.text().ljust(12, '0'),
                    'code': town_td0.text()[0:9],
                    'name': town_td1.text()
                }
                result.append(town)
            time.sleep(self._sleep_time)
            return result
        except Exception as e:
            print(e)
            print('[Error] town出错，休眠 30 秒重试。')
            time.sleep(30)
            self.town(url)

    def village(self, url):
        """
        国家统计局村级抓取爬虫

        :param url: 抓取链接
        :type url: str
        :return: 村级信息数组，数组内元素包括链接、代码、名称、统计用区划代码。
        """

        retry = 3
        while True:
            # 有的节点有链接但其实是 404 页面，也就是并没有下级信息了，所以捕获 404 异常并直接返回空数据。
            try:
                check_result = self.check(url)
                if check_result[0] != 'village':
                    retry -= 1
                    if retry < 0:
                        raise Exception('不是村级信息页面')
                    else:
                        print('[Error] 不是村级信息页面吗？休眠 10 秒再试一次。')
                        time.sleep(10)
                else:
                    doc = check_result[1]
                    break
            except Exception as e:
                # 如果错误信息是 404 则直接返回空信息
                if e.args[0].find('404') != -1:
                    return []
                else:
                    raise e

        try:
            result = []
            village_trs = doc.find('.villagetr')
            for village_tr in village_trs.items():
                village_tds = village_tr.find('td')
                if village_tds.length != 3:
                    raise Exception(f'村级信息页面节点错误，url: {url}')
                village_td0 = village_tds.eq(0)
                village_td2 = village_tds.eq(2)
                href_temp = ''
                if village_td0.find('a'):
                    href_temp = village_td0.find('a').attr('href')
                village = {
                    'href': href_temp,
                    'statistical_code': village_td0.text().ljust(12, '0'),
                    'code': village_td0.text()[0:12],
                    'name': village_td2.text()
                }
                result.append(village)
            time.sleep(self._sleep_time)
            return result
        except Exception as e:
            print(e)
            print('[Error] village出错，休眠 30 秒重试。')
            time.sleep(30)
            self.village(url)

    def check(self, url, retry=3):
        """
        检查当前链接属于省级、地级、县级、乡级、村级中的哪个，并返回文档对象。

        :param url: 检查的链接地址
        :type url: str
        :param retry: 重试次数
        :type retry: int
        :return: 'province'=省级、'city'=地级、'county'=县级、'town'=乡级、'village'=村级、''=未匹配到
        """
        while True:
            try:
                response = self._session.get(url, headers=copy.deepcopy(self._headers))
                response.encoding = 'gbk'
                if response.status_code == 403:
                    print('[Error] 403 休眠 5 分钟重试。')
                    time.sleep(300)
                elif response.status_code != 200:
                    raise Exception(response.raise_for_status())
                else:
                    doc = PyQuery(response.text)
                    province_el = doc.find('.provincetr')
                    city_el = doc.find('.citytr')
                    county_el = doc.find('.countytr')
                    town_el = doc.find('.towntr')
                    village_el = doc.find('.villagetr')
                    if province_el:
                        return 'province', doc
                    if city_el:
                        return 'city', doc
                    if county_el:
                        return 'county', doc
                    if town_el:
                        return 'town', doc
                    if village_el:
                        return 'village', doc
                    return '', doc
            except Exception as e:
                retry = retry - 1
                if retry < 0:
                    raise e
                else:
                    print(e)
                    print('[Error] check出错，休眠 30 秒重试。')
                    time.sleep(30)


if __name__ == '__main__':
    pass
