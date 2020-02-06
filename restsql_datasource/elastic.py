#!/usr/bin/env python
# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from sngapm_web.settings import ES_CONN
import pandas as pd
import restsql_utility


def parse_restsql_to_dsl(select_syntax_item):
    if select_syntax_item["from"] is None or select_syntax_item["from"] == "":
        pass  # 抛出 restsql 语法错误

    dsl = {
        'size': restsql_utility.get_table_limit(select_syntax_item.get("limit")),
        'query': {
            'bool': {
                'must': []
            }
        },
        'sort': [],
        '_source': {
            'includes': []
        },
        'aggs': {
            'groupby': {
                'terms': {
                    'script': {
                        'source': ''
                    }
                },
                'aggs': {}
            }
        },
        'from': select_syntax_item['from']
    }
    dsl['_source']['includes'] = select_syntax_item['fields']
    dsl_where = dsl['query']['bool']['must']
    dsl_group_by = ''
    dsl_aggs = dsl['aggs']['groupby']['aggs']
    dsl_sort = dsl['sort']

    # 处理filter
    for field, value in select_syntax_item['filter'].items():
        if '__' not in field:
            dsl_where.append({
                'term': {
                    field: value
                }
            })
        else:
            op = field.split('__')[1]
            field_name = field.split('__')[0]
            if op == 'gt':
                dsl_where.append({
                    'range': {
                        field_name: {'gt': value}
                    }
                })
            elif op == 'lt':
                dsl_where.append({
                    'range': {
                        field_name: {'lt': value}
                    }
                })
            elif op == 'gte':
                dsl_where.append({
                    'range': {
                        field_name: {'gte': value}
                    }
                })
            elif op == 'lte':
                dsl_where.append({
                    'range': {
                        field_name: {'lte': value}
                    }
                })
            elif op == 'contains':
                """"
                TODO: 本来想用match/match_phrase来进行模糊匹配，但是由于这两种查询由于分词的缘故，现有的
                      分词情况并不能完美的模拟sql中的like，所以暂时采用正则查询。正则查询的效率很低。
                dsl_where.append({
                    'match_phrase': {
                        field_name: {
                            'query': value
                        }
                    }
                })
                """
                dsl_where.append({
                    'wildcard': {field_name: ''.join(['*', value, '*'])}
                })
            elif op == 'startswith':
                dsl_where.append({
                    'prefix': {field_name: value}
                })
            elif op == 'endswith':
                dsl_where.append({
                    'wildcard': {field_name: ''.join(['*', value])}
                })
            elif op == 'range':
                if len(value) != 2:
                    raise SyntaxError('Check your "range" query')
                dsl_where.append({
                    'range': {
                        field_name: {'gte': value[0], 'lte': value[1]}
                    }
                })
            elif op == 'in':
                dsl_where.append({
                    'terms': {field_name: value}
                })
            else:
                raise SyntaxError('cat not support op: {0}, field: {1}'.format(op, field))
    if select_syntax_item.get('group_by'):
        # 处理 group by
        """
        由于ES 6.x以下版本不支持 composite 语法，所以这里采用script方式来实现group by，用来兼容不同版本ES这部分语法的差异性
        script中source的格式：key:value;key:value
        定义成这个样子是方便后面从查询结果中提取数据
        """
        for field in select_syntax_item['group_by']:
            dsl_group_by = ''.join([dsl_group_by, "'", field, "'", " + ':' + ", "doc['", field, "'].value", " + ';' + "])
        dsl_group_by = dsl_group_by[:len(dsl_group_by)-len(" + ';' + ")]  # 去掉结尾的 " + ';' + "
        dsl['aggs']['groupby']['terms']['script']['source'] = dsl_group_by
        # 处理 aggregation
        for field in select_syntax_item['aggregation']:
            field_name, op = field.split('__')[0], field.split('__')[1]
            func_map = {'count': 'value_count', 'sum': 'sum', 'avg': 'avg', 'max': 'max', 'min': 'min', 'count_distinct': 'cardinality'}
            if op in func_map:
                dsl_aggs[field] = {func_map[op]: {'field': field_name}}
            else:
                raise SyntaxError('cat not support aggregation operation: {}'.format(op))
    else:
        del dsl['aggs']

    # 处理 sort
    if select_syntax_item.get('sort'):
        for sort_it in select_syntax_item['sort']:
            is_reverse = sort_it.find('-')
            if is_reverse != 0:
                dsl_sort.append({
                    sort_it: {'order': 'asc'}
                })
            else:
                field = ''.join(sort_it.split('-')[1:])
                dsl_sort.append({
                    field: {'order': 'desc'}
                })
    else:
        del dsl['sort']
    # TODO: 打印此时生成的 DSL
    return dsl

def extract_useful_result(raw_result):
    if 'aggs' in raw_result or 'aggregations' in raw_result:
        if raw_result.get('aggregations'):
            result = raw_result['aggregations']['groupby']['buckets']
        else:
            result = raw_result['agg']['groupby']['buckets']
        for it in result:
            pair = it['key'].split(';')
            for pair_item in pair:
                it.update({pair_item.split(':')[0]: pair_item.split(':')[1]})
            del it['key']
            del it['doc_count']  # TODO: 暂时没用的一个字段
            for key, value in it.items():
                if isinstance(value, dict) and 'value' in value:
                    it[key] = value['value']
    elif 'hits' in raw_result and 'hits' in raw_result['hits']:
        result = list(map(lambda x: x['_source'], raw_result['hits']['hits']))
    return result

def search_impl(dsl):
    es_client = Elasticsearch(ES_CONN)
    index = dsl["from"]
    del dsl["from"]
    return extract_useful_result(es_client.search(index=index, body=dsl))

def convert_result_to_dataframe(result, fields):
    data = []
    for item in result:
        line = []
        for field in fields:
            line.append(item[field])
        data.append(line)
    result_dataframe = pd.DataFrame(data, columns=fields)
    return result_dataframe

def package_result(query_item, result):
    result = {
        "data": result,
        "on": [],
        "export": []
    }
    if query_item.get("type"):
        result["type"] = query_item["type"]
        result["on"] = query_item["on"].keys()
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
    [{"join_type": xxx, "data": DataFrame, "on": xxx, "export": xxx}]
    """
    select = query_item["query"]["select"]
    dsl = parse_restsql_to_dsl(select)
    result = search_impl(dsl)
    result_dataframe = convert_result_to_dataframe(result, select["fields"])
    return package_result(query_item, result_dataframe)

