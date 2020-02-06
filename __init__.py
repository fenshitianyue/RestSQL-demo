#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from restsql_datasource import database
from restsql_datasource import elastic

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
    # TODO: 打印 join 结果

    # 封装 grafana 预处理数据集
    if len(dataframe_main) == 0:
        return [], []


