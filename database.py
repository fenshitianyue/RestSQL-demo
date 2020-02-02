#!/usr/bin/env python
# -*- coding: utf-8 -*-

import django.db.models
import model_set
# from init_table_map import *
from init_table_map import table_map

DEFAULT_LIMIT = 1000

def get_model(model_name):
    return getattr(model_set, table_map[model_name])

def get_attribute(attribute_name):
    return getattr(django.db.models, attribute_name)

def process_table_name(raw_table_name):
    """函数作用：
    兼容已有的以sngapm开头的表，对restsql中的表名做处理:
    schema_name.table_name -> "schema_name"."table_name"
    """
    pos = raw_table_name.find(".")
    if pos != -1:
        schema, table_name = raw_table_name.split(".")[0], raw_table_name.split(".")[1]
        return '"{}"."{}"'.format(schema, table_name)
    else:
        return raw_table_name

def get_table_limit(limit):
    if limit is None or limit > DEFAULT_LIMIT:
        return DEFAULT_LIMIT
    return limit

def build_aggregation_dict(raw_aggregation):
    agg_dict = {}
    for it in raw_aggregation:
        field = it[:it.find("__")]
        if it.find("__count") != -1:
            if it.find("_distinct") != -1:
                agg_dict.update({it: get_attribute("Count")(field, distinct=True)})
            else:
                agg_dict.update({it: get_attribute("Count")(field)})
        if it.find("__sum") != -1:
            if it.find("_distinct") != -1:
                agg_dict.update({it: get_attribute("Sum")(field, distinct=True)})
            else:
                agg_dict.update({it: get_attribute("Sum")(field)})
        if it.find("__avg") != -1:
            if it.find("_distinct") != -1:
                agg_dict.update({it: get_attribute("Avg")(field, distinct=True)})
            else:
                agg_dict.update({it: get_attribute("Avg")(field)})
    return agg_dict

def search_impl(table, select, agg_dict, table_limit, search_option):
    if search_option["has_filter"] and search_option["has_groupby"]:
        result = table.objects.values(*select["group_by"]).filter(**select["filter"]).annotate(**agg_dict)
    elif not search_option["has_filter"] and search_option["has_groupby"]:
        result = table.objects.values(*select["group_by"]).annotate(**agg_dict)
    elif search_option["has_filter"] and not search_option["has_groupby"]:
        for it in select["fields"]:
            if it.find("__avg") != -1 or it.find("__sum") != -1 or it.find("__count") != -1:
                pass  # 抛出 restsql 语法错误
        result = table.objects.filter(**select["filter"]).values(*select["fields"])
    else:
        for it in select["fields"]:
            if it.find("__avg") != -1 or it.find("__sum") != -1 or it.find("__count") != -1:
                pass  # 抛出 restsql 语法错误
        result = table.objects.values(*select["fields"])
    if select.get("sort"):
        return result.order_by(*select["sort"])[:table_limit]
    else:
        return result[:table_limit]

def package_result(query_item, raw_result):
    result = {
        "data": raw_result,
        "on": [],
        "export": []
    }
    if query_item.get("type"):
        result["type"] = query_item["type"]
        for key in query_item["on"]:
            result["on"].append(key)
        # 这里的逻辑是: join块中的fields字段的元素数目减去on字段的key个数必须等于export字段的元素个数
        diff = len(query_item['query']['select']['fields']) - len(query_item["on"]) - len(query_item['export'])
        if diff > 0:
            raise SyntaxError('The elements in the "fields" field of the "join" query contain unused columns')
        elif diff < 0:
            raise SyntaxError('Check your "join" syntax')
        # 收集export字段
        result["export"].append(query_item['export'])
    # return {"data": raw_result}
    return result

def search(query_item):
    """return
    [{"join_type": xxx, "data": xxx, "on": xxx, "export": xxx}]
    """
    select = query_item["query"]["select"]
    table = get_model(process_table_name(select["from"]))
    search_option = {
        "has_filter": False,
        "has_groupby": False
    }
    if select.get("filter"):
        search_option["has_filter"] = True
    if select.get("group_by"):
        search_option['has_groupby'] = True
        if select.get("aggregation"):
            agg_dict = build_aggregation_dict(select["aggregation"])
        else:
            pass  # 抛出 restsql 语法错误
    table_limit = get_table_limit(select.get("limit"))
    raw_result = search_impl(table, select, agg_dict, table_limit, search_option)
    # TODO: 打印此时生成的 sql
    return package_result(query_item, raw_result)

