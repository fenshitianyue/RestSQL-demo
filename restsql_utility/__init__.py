#!/usr/bin/env python
# -*- coding: utf-8 -*-


DEFAULT_LIMIT = 1000

def get_table_limit(raw_limit):
    if raw_limit is None or raw_limit > DEFAULT_LIMIT:
        return DEFAULT_LIMIT
    return raw_limit

