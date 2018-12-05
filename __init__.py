# encoding:UTF-8

"""
quantOS的行情和模拟交易接口
官方网址：https://www.quantos.org/
"""
    
from vnpy.trader import vtConstant
from quantosGateway import QuantosGateway

gatewayClass = QuantosGateway
gatewayName = 'QuantOS'
gatewayDisplayName = u'QuantOS'
gatewayType = vtConstant.GATEWAYTYPE_EQUITY
gatewayQryEnabled = True

