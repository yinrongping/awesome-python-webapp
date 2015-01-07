#!/usr/bin/python
# coding=utf-8
import math
import random
import decimal
import sys
from fractions import Fraction

print 0o100, 0x100, 100
# 第二位表示前面字符串表示的进制
print int('100', 2), int('100', 8), int('100', 16)
# 转化为进制
print oct(64), hex(64), bin(64)
# eval 将字符串转化位python代码
# print eval('10+20')
print math.pi, math.e
print math.pow(2, 4), 2 ** 4
print random.random()

# 设置返回的小数位数
print decimal.Decimal(1) / decimal.Decimal(7)
# 设置全局的精度
decimal.getcontext().prec = 4
print decimal.Decimal(1) / decimal.Decimal(7)

# 临时将精度设置为小数点后2位
with decimal.localcontext() as ctx:
    ctx.prec = 2
    print decimal.Decimal(2) / decimal.Decimal(7)
print decimal.Decimal(2) / decimal.Decimal(7)

# 将小数转化为分数格式
print Fraction(*(1, 2))
print 2.5.as_integer_ratio()
f = 2.5
print Fraction(*f.as_integer_ratio())

# set集合 无序，唯一，不可变
print set('hello')  # set(['h', 'e', 'l', 'o'])

a = set('hello')
b = a.add('d')
print id(a), id(b)

a = 3
b = a
b = a + 2
print a, b

# 查询引用的次数
print sys.getrefcount('a')

L = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
# slice表示切片对象
print L[slice(1, 3)]
# 从偏移1开始，以3位步长，一直到10
B = L[1:10:3]
print B
# 反转字符串
a = 'abcdefg'
print a[::-1]
# 去掉最后一个字符
print a[:-1]

import time
print time.time()
