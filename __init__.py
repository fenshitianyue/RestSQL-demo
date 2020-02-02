#!/usr/bin/env python
# -*- coding: utf-8 -*-

import database
import elastic
import pandas as pd

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
    raw_search_result_list = []  # [{"join_type": xxx, "data": xxx, "on": xxx, "export": xxx}, ...]
    # 收集主语法块查询结果
    main_query_item = {
        "query": {
            "select": query["select"]
        }
    }
    raw_search_result_list.append(check_data_source(query["select"]["from"]).search(main_query_item))
    # 收集 join 语法块所有查询结果
    for table in query['join']:
        raw_search_result_list.append(check_data_source(table["query"]["select"]["from"]).search(query["select"]))

    # 在内存中join
    df_main = ""
    if query["join"]:
        for item in raw_search_result_list:
            if not item.exists():
                continue
            item["data"] = pd.DataFrame.from_records(list(item["data"]))
            alias_map = {}
            for field in item["export"]:
                if field.find("@") != -1:
                    alias_map.update({field.split("@")[0]: field.split("@")[1]})
            item["data"] = item["data"].rename(columns=alias_map)
            # TODO: 打印单表结果
            if "left_join" == item["join_type"]:
                df_main = df_main.merge(item["data"], on=item["on"], how="left")
            elif "inner_join" == item["join_type"]:
                df_main = df_main.merge(item["data"], on=item["on"])
            elif "full_join" == item["join_type"]:
                df_main = df_main.merge(item["data"], on=item["on"], how="outer")
            else:
                pass
    # TODO: 打印 join 结果

    # 封装 grafana 预处理数据集
    if len(df_main) == 0:
        return [], []


