# -*- coding: utf-8 -*-
import os
import sqlite3


class DBUtilStatsGovCn(object):
    """
    数据库工具
    """

    def __init__(self, database):
        # 如果数据库目录不存在则创建
        if os.path.exists(os.path.dirname(database)) is False:
            os.makedirs(os.path.dirname(database))

        self._conn = sqlite3.connect(database)
        """
        数据库连接类
        """

        # 修改 SQLite 默认查询返回数据类型，由 tuple 改为带列名的 dict 类型。
        self._conn.row_factory = lambda cursor, row: dict((col[0], row[idx]) for idx, col in enumerate(cursor.description))

        self._curs = self._conn.cursor()
        """
        数据库游标
        """

        # 如果数据表不存在这创建数据表
        sql = [
            'CREATE TABLE IF NOT EXISTS `province` '
            '(`statistical_code` CHAR(12) PRIMARY KEY, `code` CHAR(2), `name` VARCHAR(100));',

            'CREATE TABLE IF NOT EXISTS `city` '
            '(`statistical_code` CHAR(12) PRIMARY KEY, '
            '`code` CHAR(4), `name` VARCHAR(100), `province_statistical_code` CHAR(12));',

            'CREATE TABLE IF NOT EXISTS `county` '
            '(`statistical_code` CHAR(12) PRIMARY KEY, '
            '`code` CHAR(6), `name` VARCHAR(100), `province_statistical_code` CHAR(12), '
            '`city_statistical_code` CHAR(12));',

            'CREATE TABLE IF NOT EXISTS `town` '
            '(`statistical_code` CHAR(12) PRIMARY KEY, '
            '`code` CHAR(9), `name` VARCHAR(100), `province_statistical_code` CHAR(12), '
            '`city_statistical_code` CHAR(12), `county_statistical_code` CHAR(12));',

            'CREATE TABLE IF NOT EXISTS `village` '
            '(`statistical_code` CHAR(12) PRIMARY KEY, '
            '`code` CHAR(12), `name` VARCHAR(100), `province_statistical_code` CHAR(12), '
            '`city_statistical_code` CHAR(12), `county_statistical_code` CHAR(12), `town_statistical_code` CHAR(12));'
        ]
        for s in sql:
            self._curs.execute(s)
        self._conn.commit()

    def insert_province(self, statistical_code, code, name):
        """
        插入省级信息

        :param statistical_code: 统计用区划代码
        :type statistical_code: str
        :param code: 省级代码
        :type code: str
        :param name: 名称
        :type name: str
        :return:
        """
        sql = 'INSERT INTO `province` (`statistical_code`, `code`, `name`) ' \
              'VALUES(?, ?, ?);'
        self._curs.execute(sql, (statistical_code, code, name))
        self._conn.commit()

    def insert_city(self, statistical_code, code, name, province_statistical_code):
        """
        插入地级信息

        :param statistical_code: 统计用区划代码
        :type statistical_code: str
        :param code: 地级代码
        :type code: str
        :param name: 名称
        :type name: str
        :param province_statistical_code: 省级统计用区划代码
        :type province_statistical_code: str
        :return:
        """
        sql = 'INSERT INTO `city` (`statistical_code`, `code`, `name`, `province_statistical_code`) ' \
              'VALUES(?, ?, ?, ?);'
        self._curs.execute(sql, (statistical_code, code, name, province_statistical_code))
        self._conn.commit()

    def insert_county(self, statistical_code, code, name, province_statistical_code, city_statistical_code):
        """
        插入县级信息

        :param statistical_code: 统计用区划代码
        :type statistical_code: str
        :param code: 县级代码
        :type code: str
        :param name: 名称
        :type name: str
        :param province_statistical_code: 省级统计用区划代码
        :type province_statistical_code: str
        :param city_statistical_code: 地级统计用区划代码
        :type city_statistical_code: str
        :return:
        """
        sql = 'INSERT INTO `county` ' \
              '(`statistical_code`, `code`, `name`, `province_statistical_code`, `city_statistical_code`) ' \
              'VALUES(?, ?, ?, ?, ?);'
        self._curs.execute(sql, (statistical_code, code, name, province_statistical_code, city_statistical_code))
        self._conn.commit()

    def insert_town(self, statistical_code, code, name, province_statistical_code, city_statistical_code, county_statistical_code):
        """
        插入乡级信息

        :param statistical_code: 统计用区划代码
        :type statistical_code: str
        :param code: 乡级代码
        :type code: str
        :param name: 名称
        :type name: str
        :param province_statistical_code: 省级统计用区划代码
        :type province_statistical_code: str
        :param city_statistical_code: 地级统计用区划代码
        :type city_statistical_code: str
        :param county_statistical_code: 县级统计用区划代码
        :type county_statistical_code: str
        :return:
        """
        sql = 'INSERT INTO `town` ' \
              '(`statistical_code`, `code`, `name`, `province_statistical_code`, `city_statistical_code`, ' \
              '`county_statistical_code`) ' \
              'VALUES(?, ?, ?, ?, ?, ?);'
        self._curs.execute(
            sql,
            (statistical_code, code, name, province_statistical_code, city_statistical_code, county_statistical_code)
        )
        self._conn.commit()

    def insert_village(self, statistical_code, code, name, province_statistical_code, city_statistical_code, county_statistical_code, town_statistical_code):
        """
        插入村级信息

        :param statistical_code: 统计用区划代码
        :type statistical_code: str
        :param code: 村级代码
        :type code: str
        :param name: 名称
        :type name: str
        :param province_statistical_code: 省级统计用区划代码
        :type province_statistical_code: str
        :param city_statistical_code: 地级统计用区划代码
        :type city_statistical_code: str
        :param county_statistical_code: 县级统计用区划代码
        :type county_statistical_code: str
        :param town_statistical_code: 乡级统计用区划代码
        :type town_statistical_code: str
        :return:
        """
        sql = 'INSERT INTO `village` ' \
              '(`statistical_code`, `code`, `name`, `province_statistical_code`, `city_statistical_code`, ' \
              '`county_statistical_code`, `town_statistical_code`) ' \
              'VALUES(?, ?, ?, ?, ?, ?, ?);'
        self._curs.execute(
            sql,
            (statistical_code, code, name, province_statistical_code, city_statistical_code, county_statistical_code,
             town_statistical_code)
        )
        self._conn.commit()

    def truncate_province(self):
        """
        清空省级信息

        :return:
        """
        self._truncate_data('province')

    def truncate_city(self):
        """
        清空地级信息

        :return:
        """
        self._truncate_data('city')

    def truncate_county(self):
        """
        清空县级信息

        :return:
        """
        self._truncate_data('county')

    def truncate_town(self):
        """
        清空乡级信息

        :return:
        """
        self._truncate_data('town')

    def truncate_village(self):
        """
        清空村级信息

        :return:
        """
        self._truncate_data('village')

    def _truncate_data(self, name):
        """
        清空指定名称表信息

        :param name: 表名
        :type name: str
        :return:
        """
        self._curs.execute(f'DELETE FROM `{name}`;')
        self._conn.commit()

    def select_province(self, statistical_code):
        """
        查询省级信息

        :param statistical_code: 统计用区划代码
        :type statistical_code: str
        :return: 省级信息
        :rtype: dict
        """
        return self._select_data('province', statistical_code)

    def select_city(self, statistical_code):
        """
        查询地级信息

        :param statistical_code: 统计用区划代码
        :type statistical_code: str
        :return: 地级信息
        :rtype: dict
        """
        return self._select_data('city', statistical_code)

    def select_county(self, statistical_code):
        """
        查询县级信息

        :param statistical_code: 统计用区划代码
        :type statistical_code: str
        :return: 县级信息
        :rtype: dict
        """
        return self._select_data('county', statistical_code)

    def select_town(self, statistical_code):
        """
        查询乡级信息

        :param statistical_code: 统计用区划代码
        :type statistical_code: str
        :return: 乡级信息
        :rtype: dict
        """
        return self._select_data('town', statistical_code)

    def select_village(self, statistical_code):
        """
        查询村级信息

        :param statistical_code: 统计用区划代码
        :type statistical_code: str
        :return: 村级信息
        :rtype: dict
        """
        return self._select_data('village', statistical_code)

    def _select_data(self, name, statistical_code):
        """
        查询指定名称表单个信息

        :param name: 表名
        :type name: str
        :param statistical_code: 统计用区划代码
        :type statistical_code: str
        :return: 指定名称表单个信息
        :rtype: dict
        """
        self._curs.execute(f'SELECT * FROM `{name}` WHERE `statistical_code`=?;', (statistical_code,))
        return self._curs.fetchone()

    def select_provinces(self, limit, offset):
        """
        查询指定分页的省级信息

        :param limit: 多少个
        :type limit: int
        :param offset: 起始位
        :type offset: int
        :return: 省级信息
        :rtype: list
        """
        return self._select_data_more('province', limit, offset)

    def select_cities(self, limit, offset):
        """
        查询指定分页的地级信息

        :param limit: 多少个
        :type limit: int
        :param offset: 起始位
        :type offset: int
        :return: 地级信息
        :rtype: list
        """
        return self._select_data_more('city', limit, offset)

    def select_counties(self, limit, offset):
        """
        查询指定分页的县级信息

        :param limit: 多少个
        :type limit: int
        :param offset: 起始位
        :type offset: int
        :return: 县级信息
        :rtype: list
        """
        return self._select_data_more('county', limit, offset)

    def select_towns(self, limit, offset):
        """
        查询指定分页的乡级信息

        :param limit: 多少个
        :type limit: int
        :param offset: 起始位
        :type offset: int
        :return: 乡级信息
        :rtype: list
        """
        return self._select_data_more('town', limit, offset)

    def select_villages(self, limit, offset):
        """
        查询指定分页的村级信息

        :param limit: 多少个
        :type limit: int
        :param offset: 起始位
        :type offset: int
        :return: 村级信息
        :rtype: list
        """
        return self._select_data_more('village', limit, offset)

    def _select_data_more(self, name, limit, offset):
        """
        查询指定名称表分页信息

        :param name: 表名
        :type name: str
        :param limit: 多少个
        :type limit: int
        :param offset: 起始位
        :type offset: int
        :return: 指定名称表分页信息
        :rtype: list
        """
        self._curs.execute(f'SELECT * FROM `{name}` LIMIT ? OFFSET ?;', (limit, offset))
        return self._curs.fetchall()

    def select_count_province(self):
        """
        查询省级信息数据记录数量

        :return: 数量
        :rtype: int
        """
        return self._select_count('province')

    def select_count_city(self):
        """
        查询地级信息数据记录数量

        :return: 数量
        :rtype: int
        """
        return self._select_count('city')

    def select_count_county(self):
        """
        查询县级信息数据记录数量

        :return: 数量
        :rtype: int
        """
        return self._select_count('county')

    def select_count_town(self):
        """
        查询乡级信息数据记录数量

        :return: 数量
        :rtype: int
        """
        return self._select_count('town')

    def select_count_village(self):
        """
        查询村级信息数据记录数量

        :return: 数量
        :rtype: int
        """
        return self._select_count('village')

    def _select_count(self, name):
        """
        查询指定名称表的记录数量

        :param name: 表名
        :type name: str
        :return: 数量
        :rtype: int
        """
        self._curs.execute(f'SELECT COUNT(*) AS `count` FROM `{name}`;')
        return self._curs.fetchone()['count']

    def select_cities_by_top(self, top_code):
        """
        通过上级编号查询地级所有信息数据

        :param top_code: 上级编号
        :return: 地级信息
        :rtype: list
        """
        return self._select_data_more_by_top('city', 'province_statistical_code', top_code)

    def select_counties_by_top(self, top_code):
        """
        通过上级编号查询县级所有信息数据

        :param top_code: 上级编号
        :return: 县级信息
        :rtype: list
        """
        return self._select_data_more_by_top('county', 'city_statistical_code', top_code)

    def select_towns_by_top(self, top_code):
        """
        通过上级编号查询乡级所有信息数据

        :param top_code: 上级编号
        :return: 乡级信息
        :rtype: list
        """
        return self._select_data_more_by_top('town', 'county_statistical_code', top_code)

    def select_villages_by_top(self, top_code):
        """
        通过上级编号查询村级所有信息数据

        :param top_code: 上级编号
        :return: 村级信息
        :rtype: list
        """
        return self._select_data_more_by_top('village', 'town_statistical_code', top_code)

    def _select_data_more_by_top(self, name, top_name, top_code):
        """
        查询指定名称表指定上级编号的所有记录

        :param name: 表名
        :type name: str
        :param top_name: 上级名称
        :type top_name: str
        :param top_code: 上级编号
        :type top_code: str
        :return: 数量
        :rtype: int
        """
        self._curs.execute(
            f'SELECT `statistical_code`, `code`, `name` FROM `{name}` WHERE `{top_name}`=?;',
            (top_code,)
        )
        return self._curs.fetchall()

    def __del__(self):
        self._conn.close()


if __name__ == '__main__':
    pass
