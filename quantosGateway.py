# encoding:UTF-8

import os
import json

try:
    from jaqs.data import DataApi
    from jaqs.trade import TradeApi
except ImportError:
    print u'请先安装QuantOS的JAQS模块'
    
from vnpy.trader.vtEvent import *
from vnpy.trader.vtConstant import *
from vnpy.trader.vtGateway import *
from vnpy.trader.vtFunction import getJsonPath

from .language import text

class QuantosGateway(VtGateway):
    """QuantOS接口"""
    
    # 订阅个股行情时的字段对应表
    qsqParamMap = {}
    qsqParamMap['time'] = 'time'
    qsqParamMap['date'] = 'trade_date'
    qsqParamMap['openPrice'] = 'open'
    qsqParamMap['highPrice'] = 'high'
    qsqParamMap['lowPrice'] = 'low'
    qsqParamMap['lastPrice'] = 'last'
    qsqParamMap['openInterest'] = 'oi'
    qsqParamMap['volume'] = 'volume'
    qsqParamMap['upperLimit'] = 'limit_up'
    qsqParamMap['lowerLimit'] = 'limit_down'
    qsqParamMap['preClosePrice'] = 'preclose'
    qsqParamMap['askPrice1'] = 'askprice1'
    qsqParamMap['askPrice2'] = 'askprice2'
    qsqParamMap['askPrice3'] = 'askprice3'
    qsqParamMap['askPrice4'] = 'askprice4'
    qsqParamMap['askPrice5'] = 'askprice5'
    qsqParamMap['bidPrice1'] = 'bidprice1'
    qsqParamMap['bidPrice2'] = 'bidprice2'
    qsqParamMap['bidPrice3'] = 'bidprice3'
    qsqParamMap['bidPrice4'] = 'bidprice4'
    qsqParamMap['bidPrice5'] = 'bidprice5'
    qsqParamMap['askVolume1'] = 'askvolume1'
    qsqParamMap['askVolume2'] = 'askvolume2'
    qsqParamMap['askVolume3'] = 'askvolume3'
    qsqParamMap['askVolume4'] = 'askvolume4'
    qsqParamMap['askVolume5'] = 'askvolume5'
    qsqParamMap['bidVolume1'] = 'bidvolume1'
    qsqParamMap['bidVolume2'] = 'bidvolume2'
    qsqParamMap['bidVolume3'] = 'bidvolume3'
    qsqParamMap['bidVolume4'] = 'bidvolume4'
    qsqParamMap['bidVolume5'] = 'bidvolume5'
    
    # 交易所对照表
    exchangeMap = {}
    exchangeMap[EXCHANGE_SZSE] = 'SZ'
    exchangeMap[EXCHANGE_SSE] = 'SH'
    exchangeMap[EXCHANGE_CFFEX] = 'CFE'
    exchangeMap[EXCHANGE_SHFE] = 'SHF'
    exchangeMap[EXCHANGE_CZCE] = 'CZC'
    exchangeMap[EXCHANGE_DCE] = 'DCE'
    exchangeMap[EXCHANGE_SGE] = 'SGE'
    exchangeMap[EXCHANGE_HKEX] = 'HK'
    exchangeMap[EXCHANGE_UNKNOWN] = None
    exchangeMapReverse = {v:k for k,v in exchangeMap.items()}
    
    # 交易方向对照表
    action = {}
    action[OFFSET_OPEN] = 'Buy'
    action[DIRECTION_SHORT] = 'Short'
    action[DIRECTION_SELL] = 'Cover'
    action[OFFSET_CLOSE] = 'Sell'
    action[OFFSET_CLOSETODAY] = 'CoverToday'
    action[OFFSET_CLOSEYESTERDAY] = 'CoverYesterday'
    # action[''] = 'SellToday'
    # action[''] = 'SellYesterday'
    action[DIRECTION_UNKNOWN] = None
    actionReverse = {v:k for k,v in action.items()}
    
    # 订单状态对照表
    status = {}
    status[STATUS_NOTTRADED] = 'Accepted'
    status[STATUS_ALLTRADED] = 'Filled'
    status[STATUS_REJECTED] = 'Rejected'
    status[STATUS_CANCELLED] = 'Cancelled'
    status[STATUS_UNKNOWN] = None
    statusReverse = {v:k for k,v in status.items()}
    
    #----------------------------------------------------------------------
    def __init__(self, eventEngine, gatewayName='QuantOS'):
        """Constructor"""
        super(QuantosGateway, self).__init__(eventEngine, gatewayName)
            
        self.qryEnabled = False                 # 循环查询开关
        self.qsqApi = None                      # 行情API
        self.qstApi = None                      # 交易API
        self.setting = []                       # API配置
        self.user_info = {}                     # 交易账号信息
        self.strategyID = EMPTY_INT             # 策略编号
        
        # API配置文件
        self.fileName = self.gatewayName + '_connect.json'
        self.filePath = getJsonPath(self.fileName, __file__)
        
        # 获取配置信息
        self.getConfig()
        
    #----------------------------------------------------------------------
    def connect(self):
        """连接"""
        try:
            dataServer = self.setting['DATA_SERVER']
            tradeServer = self.setting['TRADE_SERVER']
            userID = self.setting['USER_ID']
            token = self.setting['TOKEN']   
        except KeyError:
            self.writeLog(text.CONFIG_KEY_MISSING)        
            return
        
        # 连接行情API
        try:
            api = DataApi(addr=dataServer)
            api.login(userID, token)
            self.writeLog(text.DATA_SERVER_CONNECTED)            
        except:
            self.writeLog(text.DATA_SERVER_FAILED)
            return
        
        # 连接交易API
        t_api = TradeApi(addr=tradeServer)
        user_info, msg = t_api.login(userID, token)
        self.writeLog(text.TRADING_SERVER_CONNECTED)
        
        if msg == self.setting['ISOK']:
            self.user_info = user_info
            strategy_list = self.user_info['strategies']
            self.strategyID = strategy_list[self.setting['DEFAULT_STRATEGYID']] # 设置默认的策略编号
        else:
            self.writeLog(text.TRADING_SERVER_FAILED)
            return            
        
        self.qsqApi = api
        self.qstApi = t_api
        self.initContract()         # 初始化合约信息
        self.initTradingEnv()       # 初始化交易环境
        self.qryAccount()           # 获取账户信息
        self.qryPosition()          # 获取交易信息
        self.initOrder()            # 获取订单信息
        self.initTrade()            # 获取成交信息
    #----------------------------------------------------------------------
    def initContract(self):
        """初始化合约信息"""
        
        # 获取合约基础数据 inist_type=1表示股票
        df, msg = self.qsqApi.query(view="jz.instrumentInfo",
                                    fields="symbol, market, product, name, inst_type, pricetick, underlying",
                                    filter="inst_type=1&status=1",
                                    data_format='pandas')
        
        # 根据返回值判断API合约数据是否正常
        if not msg:
            self.writeLog(text.CONTRACT_DATA_FAILED)
            return
        
        if len(df):
            # 循环获取合约数据
            for i in range(len(df)):
                self.putContract(df.iloc[i])    
            self.writeLog(text.CONTRACT_DATA_RECEIVED)
        else:
            self.writeLog(text.CONTRACT_DATA_FAILED)
            return
                
    #----------------------------------------------------------------------    
    def putContract(self, data):
        """合成合约数据并推送到事件引擎"""
        
        # 实例化合约对象
        ct = VtContractData()
        symbol_split = str(data['symbol']).split('.')
        ct.symbol = symbol_split[0]
        exchange = self.exchangeMapReverse.get(symbol_split[1], None)
        if exchange is not None:
            ct.exchange = exchange
            ct.vtSymbol = '.'.join([symbol_split[0], exchange])
            ct.name = unicode(data['name'])
            ct.productClass = str(u'股票')
            ct.size = 0.00
            ct.priceTick = float(data['pricetick'])
            ct.gatewayName = self.gatewayName
            
            event = Event(type_=EVENT_CONTRACT)
            event.dict_['data'] = ct
            
            self.eventEngine.put(event)                 # 推送给事件引擎做后续处理
    #----------------------------------------------------------------------
    def initTradingEnv(self):
        """初始化交易环境"""
        
        sid, msg = self.qstApi.use_strategy(self.strategyID)            # 绑定交易策略账号
        
        if msg == self.setting['ISOK']:
            self.writeLog(u'%s, 策略编号为：%s' % (text.STRATEGY_ID_BINDING_COMPLETE, sid))
            self.qstApi.set_ordstatus_callback(self.orderCallBack)      # 订单状态推送
            self.qstApi.set_trade_callback(self.tradeCallBack)          # 成交回报推送
        else:
            self.writeLog(u'%s, 失败原因为：%s' % (text.STRATEGT_ID_BINDING_FAILED, msg))
    #----------------------------------------------------------------------
    def initOrder(self):
        """初始化委托数据"""
        # 委托数据查询
        orders, msg = self.qstApi.query_order(format=self.setting['FORMAT'])
        
        if msg == self.setting['ISOK']:
            for i in range(len(orders)):
                order_data = VtOrderData()
                
                symbol_split = str(orders.iloc[i]['security']).split('.')
                order_data.symbol = symbol_split[0]
                order_data.exchange = self.exchangeMapReverse.get(symbol_split[1], None)
                order_data.vtSymbol = '.'.join([order_data.symbol, order_data.exchange])
                order_data.gatewayName = self.gatewayName
                order_data.orderID = str(orders.iloc[i]['entrust_no'])
                order_data.vtOrderID = self.gatewayName+order_data.orderID
                order_data.direction = ''
                order_data.offset = self.actionReverse.get(unicode(orders.iloc[i]['entrust_action']), None)
                order_data.price = float(orders.iloc[i]['entrust_price'])
                order_data.totalVolume = float(orders.iloc[i]['entrust_size'])
                order_data.tradedVolume = float(orders.iloc[i]['fill_size'])
                order_data.status = self.statusReverse.get(str(orders.iloc[i]['order_status']), None)
                order_data.orderTime = str(orders.iloc[i]['entrust_time'])
                order_data.cancelTime = ''
                
                self.onOrder(order_data)
        else:
            self.writeLog(u'委托数据查询失败，错误原因：%s' % msg)
    #----------------------------------------------------------------------
    def initTrade(self):
        """初始化成交数据"""
        # 成交数据查询
        trades, msg = self.qstApi.query_trade(format=self.setting['FORMAT'])
        
        if msg == self.setting['ISOK']:
            for i in range(len(trades)):
                trade_data = VtTradeData()
                
                symbol_split = str(trades.iloc[i]['security']).split('.')
                trade_data.symbol = symbol_split[0]
                trade_data.exchange = self.exchangeMapReverse.get(symbol_split[1], None)
                trade_data.vtSymbol = '.'.join([trade_data.symbol, trade_data.exchange])
                trade_data.gatewayName = self.gatewayName
                trade_data.tradeID = str(trades.iloc[i]['fill_no'])
                trade_data.vtTradeID = self.gatewayName + trade_data.tradeID
                trade_data.orderID = str(trades.iloc[i]['entrust_no'])
                trade_data.vtOrderID = self.gatewayName + trade_data.orderID
                trade_data.direction = ''
                trade_data.offset = self.actionReverse.get(unicode(trades.iloc[i]['entrust_action']), None)
                trade_data.price = float(trades.iloc[i]['fill_price'])
                trade_data.volume = float(trades.iloc[i]['fill_size'])
                trade_data.tradeTime = float(trades.iloc[i]['fill_time'])
        
                self.onTrade(trade_data)
        else:
            self.writeLog(u'成交数据查询失败，错误原因：%s' % msg)
    #----------------------------------------------------------------------
    def onError(self, error):
        pass
    #----------------------------------------------------------------------
    def subscribe(self, subscribeReq):
        """订阅行情"""
        exchange = self.exchangeMap.get(subscribeReq.exchange, None)
        qsqSymbol = '.'.join([subscribeReq.symbol, exchange])
        
        # 若已经连接则直接订阅
        if self.qsqApi:
            subs_list, msg = self.qsqApi.subscribe(symbol=qsqSymbol, func=self.qsqCallBack, fields='')
        # 若失去连接则报告错误日志    
        else:
            self.writeLog(text.DATA_SERVER_DISCONNECTED)
    #----------------------------------------------------------------------
    def sendOrder(self, orderReq):
        """发送订单"""
        
        symbol = orderReq.symbol
        exchange = self.exchangeMap.get(orderReq.exchange, None)
        if exchange:
            symbol = '.'.join([symbol, exchange])
            offset = self.action.get(orderReq.offset, None)
            price = orderReq.price
            volume = orderReq.volume
        else:
            self.writeLog(u'订单发生错误：订单对应交易所不存在')
            return
        
        # 发送订单
        order_id, msg = self.qstApi.place_order(symbol, offset, price, volume)
        
        if msg == self.setting['ISOK']:
            pass
        else:
            self.writeLog(u'订单发生错误：%s' % msg)
        
    #----------------------------------------------------------------------
    def cancelOrder(self, cancelOrderReq):
        """取消订单委托"""
        
        order_id = cancelOrderReq.orderID           # 获取订单ID
        self.qstApi.cancel_order(order_id)          # 取消订单
    #----------------------------------------------------------------------
    def qryAccount(self):
        """查询账户信息"""
        
        df, msg = self.qstApi.query_account()
        if msg == self.setting['ISOK']:
            account = VtAccountData()
            acc = df[df['type'] == 'SA']
            account.accountID = str(acc.iloc[0]['id'])
            account.vtAccountID = '.'.join([self.gatewayName, account.accountID])
            account.gatewayName = self.gatewayName
            account.preBalance = 0.00
            account.balance = float(acc.iloc[0]['float_pnl'])
            account.available = float(acc.iloc[0]['enable_balance'])
            account.margin = float(acc.iloc[0]['margin'])
            account.commission = float(acc.iloc[0]['trading_pnl'])
            
            self.onAccount(account)
        else:
            self.writeLog(u'账户查询失败，错误原因：%s' % msg)
    #----------------------------------------------------------------------
    def qryPosition(self):
        """查询持仓信息"""
        df, msg = self.qstApi.query_position()
        
        if msg == self.setting['ISOK']:
            for i in range(len(df)):
                position = VtPositionData()
                
                symbol_split = str(df.iloc[i]['security']).split('.')
                position.symbol = symbol_split[0]
                position.exchange = self.exchangeMapReverse.get(symbol_split[1], None)
                position.vtSymbol = '.'.join([position.symbol, position.exchange])
                position.gatewayName = self.gatewayName
                position.direction = str(df.iloc[i]['side'])
                position.position = int(df.iloc[i]['current_size'])
                position.frozen = int(df.iloc[i]['frozen_size'])
                position.price = float(df.iloc[i]['cost_price'])
                position.vtPositionName = position.vtSymbol + position.direction
                position.ydPosition = int(df.iloc[i]['pre_size'])
                position.positionProfit = float(df.iloc[i]['float_pnl'])
                
                self.onPosition(position)
        else:
            self.writeLog(u'账户查询失败，错误原因：%s' % msg)
    #----------------------------------------------------------------------
    def qsqCallBack(self, t, data):
        """行情订阅回调函数"""
        
        tick = VtTickData()
        tick.gatewayName = self.gatewayName
        symbol_split = data['symbol'].split('.')
        tick.symbol = symbol_split[0]
        exchange = self.exchangeMapReverse.get(symbol_split[1], None)
        tick.exchange = exchange
        tick.vtSymbol = '.'.join([tick.symbol, tick.exchange])
        
        d = tick.__dict__
        for k, v in self.qsqParamMap.items():
            d[k] = data[v]
        
        self.onTick(tick)       
    #----------------------------------------------------------------------
    def orderCallBack(self, order):
        """订单状态回调函数"""
        order_data = VtOrderData()
        
        symbol_split = str(order['security']).split('.')
        order_data.symbol = symbol_split[0]
        order_data.exchange = self.exchangeMapReverse.get(symbol_split[1], None)
        order_data.vtSymbol = '.'.join([order_data.symbol, order_data.exchange])
        order_data.gatewayName = self.gatewayName
        order_data.orderID = str(order['entrust_no'])
        order_data.vtOrderID = self.gatewayName+order_data.orderID
        order_data.direction = ''
        order_data.offset = self.actionReverse.get(unicode(order['entrust_action']), None)
        order_data.price = float(order['entrust_price'])
        order_data.totalVolume = float(order['entrust_size'])
        order_data.tradedVolume = float(order['fill_size'])
        order_data.status = self.statusReverse.get(str(order['order_status']), None)
        order_data.orderTime = str(order['entrust_time'])
        
        self.onOrder(order_data)                # 推送订单数据到事件引擎
        self.qryAccount()
        self.qryPosition()        
    #----------------------------------------------------------------------
    def tradeCallBack(self, trade):
        """成交回报回调函数"""
        trade_data = VtTradeData()
        
        symbol_split = str(trade['security']).split('.')
        trade_data.symbol = symbol_split[0]
        trade_data.exchange = self.exchangeMapReverse.get(symbol_split[1], None)
        trade_data.vtSymbol = '.'.join([trade_data.symbol, trade_data.exchange])
        trade_data.gatewayName = self.gatewayName
        trade_data.tradeID = str(trade['fill_no'])
        trade_data.vtTradeID = self.gatewayName + trade_data.tradeID
        trade_data.orderID = str(trade['entrust_no'])
        trade_data.vtOrderID = self.gatewayName + trade_data.orderID
        trade_data.direction = ''
        trade_data.offset = self.actionReverse.get(unicode(trade['entrust_action']), None)
        trade_data.price = float(trade['fill_price'])
        trade_data.volume = float(trade['fill_size'])
        trade_data.tradeTime = float(trade['fill_time'])
        
        self.onTrade(trade_data)                  # 推送成交数据到事件引擎
        self.qryAccount()
        self.qryPosition()        
    #----------------------------------------------------------------------
    def close(self):
        pass
    #----------------------------------------------------------------------
    def setQryEnabled(self, qryEnabled):
        """设置是否要启动循环查询"""
        self.qryEnabled = qryEnabled
    #----------------------------------------------------------------------
    def getConfig(self):
        """获取配置信息"""
        # 文件读取检查
        try:
            f = file(self.filePath)
        except IOError:
            self.writeLog(text.LOADING_ERROR)
            return
        
        # 解析json文件
        self.setting = json.load(f)
        
    #----------------------------------------------------------------------
    def writeLog(self, content):
        """快速发出日志"""
        log = VtLogData()
        log.gatewayName = self.gatewayName
        log.logContent = content
        self.onLog(log) 
    

