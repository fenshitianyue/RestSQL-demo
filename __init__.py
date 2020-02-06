#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import json
from restsql_datasource import database
from restsql_datasource import elastic
import restsql_utility

data_source_map = {
    "database": database,
    "elastic": elastic,
    "sngapm": database,
}

def check_data_source(table_name):
    table_prefix = table_name.split(".")[0]
    try:
        return data_source_map[table_prefix]
    except KeyError:
        return elastic

def sort_result(dataframe, query):
    sort_methods = []
    for field in query["sort"]:
        if field.find("-") != -1:
            field = field.split("-")[0]
            sort_methods.append(False)
        else:
            sort_methods.append(True)
    dataframe.sort_values(by=query["sort"], ascending=sort_methods)
    return dataframe

def package_query_data(dataframe, alias_map):
    """函数作用:
    按照一定的格式将pandas结果集存储到python的数据结构中
    """
    data_list = json.loads(dataframe.to_json(orient='records'))

    for line in data_list:
        # key 是原列名，value 是列别名
        for key, value in alias_map.items():
            if value in line.keys():
                continue
            line.update({value: line[key]})  # 添加 {列别名: 值} 的键值对
            del line[key]  # 删除 {原列名: 值} 的键值对

    return data_list

def search(query):
    raw_search_result_list = []  # [{"join_type": xxx, "data": DataFrame, "on": xxx, "export": xxx}, ...]
    # 收集主语法块查询结果
    main_query_item = {
        "query": {
            "select": query["select"]
        }
    }
    raw_search_result_list.append(check_data_source(query["select"]["from"]).search(main_query_item))
    # 收集 join 语法块所有查询结果
    for join_item in query['join']:
        raw_search_result_list.append(check_data_source(join_item["query"]["select"]["from"]).search(query["select"]))

    # 在内存中join
    dataframe_main = ""
    if query["join"]:
        for item in raw_search_result_list:
            if not item["data"].index:
                continue
            alias_map = {}
            for field in item["export"]:
                if field.find("@") != -1:
                    alias_map.update({field.split("@")[0]: field.split("@")[1]})
            item["data"] = item["data"].rename(columns=alias_map)
            # TODO: 打印单表结果
            if "left_join" == item["join_type"]:
                dataframe_main = dataframe_main.merge(item["data"], on=item["on"], how="left")
            elif "inner_join" == item["join_type"]:
                dataframe_main = dataframe_main.merge(item["data"], on=item["on"])
            elif "full_join" == item["join_type"]:
                dataframe_main = dataframe_main.merge(item["data"], on=item["on"], how="outer")
            else:
                pass  # TODO: 抛出 restsql 语法错误
    # if len(dataframe_main) == 0:
    if not dataframe_main.index:
        return [], []
    # TODO: 打印 join 结果
    dataframe_main = sort_result(dataframe_main, query)
    dataframe_main = dataframe_main[0:restsql_utility.get_table_limit(query.get("limit"))]

    dataframe_main = dataframe_main.fillna("null")

    # 封装 grafana 预处理数据集
    alias_map = {}
    for field in query['fields']:
        if field.find('@') != -1:
            alias_map.update({field.split('@')[0]: field.split('@')[1]})

    # 根据顶级字段fields中的值对查询结果集进行过滤，去掉顶级字段fields中不存在的列
    need_filter_columns = list(set(dataframe_main.columns).difference(set(alias_map.keys())))
    df_main = dataframe_main.drop(need_filter_columns, axis='columns')

    data_list = package_query_data(df_main, alias_map)
    field_list = alias_map.values()
    return data_list, field_list

