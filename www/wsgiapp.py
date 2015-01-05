#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Michael Liao'

'''
A WSGI application entry.
'''

import logging
logging.basicConfig(level=logging.INFO)

import os
import time
from datetime import datetime

from transwarp import db
from transwarp.web import WSGIApplication, Jinja2TemplateEngine

from config import configs


# 时间处理的过滤器
def datetime_filter(t):
    delta = int(time.time() - t)
    if delta < 60:
        return u'1分钟前'
    if delta < 3600:
        return u'%s分钟前' % (delta // 60)
    if delta < 86400:
        return u'%s小时前' % (delta // 3600)
    if delta < 604800:
        return u'%s天前' % (delta // 86400)
    dt = datetime.fromtimestamp(t)
    return u'%s年%s月%s日' % (dt.year, dt.month, dt.day)

# init db:
db.create_engine(**configs.db)

# init wsgi app:
# os.path.abspath(__file__) 获取当前文件的绝度路径
# os.path.dirname 获取当前文件目录
# os.path.basename(path) 获取文件名
wsgi = WSGIApplication(os.path.dirname(os.path.abspath(__file__)))

# os.path.join 将多个路径组合后返回，第一个绝对路径之前的参数将被忽略
# 这里是获取templates文件夹的路径
template_engine = Jinja2TemplateEngine(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates'))

# 添加时间处理的过滤器
template_engine.add_filter('datetime', datetime_filter)

# 设置模板引擎
wsgi.template_engine = template_engine

import urls

# 添加拦截器
wsgi.add_interceptor(urls.user_interceptor)
wsgi.add_interceptor(urls.manage_interceptor)

# 动态加载module
wsgi.add_module(urls)

if __name__ == '__main__':
    wsgi.run(9000, host='0.0.0.0')
else:
    application = wsgi.get_wsgi_application()
