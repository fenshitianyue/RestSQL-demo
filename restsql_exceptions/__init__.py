# -*- coding:UTF-8 -*-

__author__ = "aiden"
__email__ = "1262167092@qq.com"

import logging


ERROR_MESSAGE_MAP = {
    1: "Authentication failed",
    2: "User syntax error",
    99: "Unkown error",
}

LOG_CATALOG = "/home/aiden/project/restsql/restsql_log/restsql.log"

logging.basicConfig(filename=LOG_CATALOG, format="%(asctime)s - %(levelname)s:%(message)s", level=logging.ERROR)


class RestsqlException(BaseException):
    """
    RestSQL 异常基类
    """

    def __init__(self, code=99, message=ERROR_MESSAGE_MAP[99]):
        self.code = code
        self.message = message

    def __unicode__(self):
        return self.message


class PermissionError(RestsqlException):
    """权限不足:
    1. token 鉴权异常
    2. 用户的查询语法块中包含没有权限的 p_id
    """

    def __init__(self, code=1, message=ERROR_MESSAGE_MAP[1]):
        super(PermissionError, self).__init__(code, message)


class ParseError(RestsqlException):
    """解析错误:
    1. 用户的查询语法有误
    2. 代码可能漏了某种行为的处理逻辑
    """

    def __init__(self, code=2, message=ERROR_MESSAGE_MAP[2]):
        super(ParseError, self).__init__(code, message)


class UnknowError(RestsqlException):
    """未知错误:
    1. 通常为代码有bug
    2. 用户的某些异常行为没有考虑到，没有写相应的处理逻辑
    """

    def __init__(self, code=99, message=ERROR_MESSAGE_MAP[99]):
        super(UnknowError, self).__init__(code, message)


def capture(func):
    def decorator(*args, **options):
        rep = "error"
        try:
            rep = func(*args, **options)
        except RestsqlException as e:
            if e.code in ERROR_MESSAGE_MAP and ERROR_MESSAGE_MAP[e.code]:
                logging.error(ERROR_MESSAGE_MAP[e.code])
            else:
                raise e
        return rep
    return decorator

def reload(code):
    """为后续特定异常提供的接口"""
    def decorator(func):
        ERROR_MESSAGE_MAP[code] = func
    return decorator

