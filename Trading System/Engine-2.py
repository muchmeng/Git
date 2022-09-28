import dolphindb as ddb 
import pandas as pd
import numpy as np
from datetime import datetime

class Engine():
	"""
	带有预测自定义order功能的撮合引擎
	"""	
	def __init__(self,InstrumentID:str,TradingDay:str):
		"""
		InstruemntID:`000576
		TradingDay:2019.11.11
		"""
		self.InstrumentID = InstrumentID
		self.TradingDay = TradingDay
		self.dbPath = "dfs://hfd"

		self.orderBooks = {}
		self.currTradeId = 0

		self.allTraded = False			
		#跟踪订单是否全部成交
		self.timeThreshold = None

		self.statusAfterCall = {}

	def Init(self):
		"""
		初始化，计算出集合竞价完毕后的OrderBook字典的状态，缓存
		之后不需要重复模拟集合竞价的状态（提高效率）
		"""
		self.session = ddb.session()
		self.session.connect("10.2.47.67",8900,"fanglm","sufefinlab")
		self.td = self.session.loadTable(tableName="trades",dbPath=self.dbPath)
		self.td = (self.td.select(['TradeIndex','BuyIndex','SellIndex','TradeTime','TradeMillisec','Price','Volume','OrderKind','FunctionCode'])
						.where("TradingDay="+self.TradingDay)
						.where("InstrumentID="+self.InstrumentID)
						.contextby(['TradingDay','InstrumentID',"TradeIndex"])
						.top(1).toDF())
		self.od = self.session.loadTable(tableName="orders",dbPath=self.dbPath)
		self.od = (self.od.select(['OrderIndex','OrderTime','OrderMillisec','Price','Volume','OrderKind','FunctionCode'])
						.where("TradingDay="+self.TradingDay)
						.where("InstrumentID="+self.InstrumentID)
						.contextby(['TradingDay','InstrumentID','OrderIndex'])
						.top(1).toDF())
		self.oq = self.session.loadTable(tableName="orderQueue",dbPath=self.dbPath)
		self.oq = (self.oq.select(['OrderTime','Price','Direction','Orders'])
							.where("TradingDay="+self.TradingDay)
							.where("InstrumentID="+self.InstrumentID)
							.contextby(['TradingDay','Direction','OrderTime'])
							.top(1).toDF())
		self.od['TimeIndex'] = self.od['OrderIndex']
		self.od = self.od.astype({"OrderKind":"str"})
		self.oq = self.oq.astype({"Direction":"int"})

		self.preprocessMarketOd()						#将市价单类型细分，添加MarketType字段
		self.simCallStatus()							#还原出集合竞价撮合后OrderBook的状态

	def _Init(self):
		"""
		再次复原集合竞价的状态
		"""
		self.orderBooks = self.statusAfterCall.copy()
		self.currTradeId = 0

	def addOrder(self,instmt,orderIndex,price,qty,side,marketType):
		"""
		market_type:市价转限价为1，U为2，其余为0
		Add an order
		调用一次add_order，返回生成的order对象和撮合完成的trade列表
		"""
		assert side == Side.BUY or side == Side.SELL, \
				"Invalid side %s" % side

		order_book = self.orderBooks.setdefault(instmt, OrderBook())

		trades = []
		order_id = orderIndex
		order = Order(order_id, instmt, price, qty, side)

		if side == Side.BUY:
			best_price = min(order_book.asks.keys()) if len(order_book.asks) > 0 \
							else None
			if marketType == 2:
				price = max(order_book.bids.keys()) if len(order_book.bids) > 0 \
								else None

			while best_price is not None and \
				  (price == 0.0 or price >= best_price ) and \
				  order.leaves_qty > 0:
				best_price_qty = sum([ask.leaves_qty for ask in order_book.asks[best_price]])
				match_qty = min(best_price_qty, order.leaves_qty)
				assert match_qty > 0, "Match quantity must be larger than zero"

				order.cum_qty += match_qty
				order.leaves_qty -= match_qty

				while match_qty > 0:
					hit_order = order_book.asks[best_price][0]
					order_match_qty = min(match_qty, hit_order.leaves_qty)
					self.currTradeId += 1
					trades.append(Trade(order.order_id,hit_order.order_id, instmt, best_price, \
										order_match_qty, \
										self.currTradeId))
					hit_order.cum_qty += order_match_qty
					hit_order.leaves_qty -= order_match_qty
					match_qty -= order_match_qty
					if hit_order.leaves_qty == 0:
						del order_book.asks[best_price][0]

				if len(order_book.asks[best_price]) == 0:
					del order_book.asks[best_price]

				if marketType == 1 :
					price = best_price
				best_price = min(order_book.asks.keys()) if len(order_book.asks) > 0 \
								else None

			if order.leaves_qty > 0:
				depth = order_book.bids.setdefault(price, [])
				depth.append(order)
				order_book.order_id_map[order_id] = order
		else:
			best_price = max(order_book.bids.keys()) if len(order_book.bids) > 0 \
							else None
			if marketType == 2:
				price = min(order_book.asks.keys()) if len(order_book.asks) > 0 \
								else None
			while best_price is not None and \
				  (price == 0.0 or price <= best_price) and \
				  order.leaves_qty > 0:
				best_price_qty = sum([bid.leaves_qty for bid in order_book.bids[best_price]])
				match_qty = min(best_price_qty, order.leaves_qty)
				assert match_qty > 0, "Match quantity must be larger than zero"

				order.cum_qty += match_qty
				order.leaves_qty -= match_qty

				while match_qty > 0:
					hit_order = order_book.bids[best_price][0]
					order_match_qty = min(match_qty, hit_order.leaves_qty)
					self.currTradeId += 1
					trades.append(Trade(hit_order.order_id,order.order_id, instmt, best_price, \
										order_match_qty, \
										self.currTradeId))
					hit_order.cum_qty += order_match_qty
					hit_order.leaves_qty -= order_match_qty
					match_qty -= order_match_qty
					if hit_order.leaves_qty == 0:
						del order_book.bids[best_price][0]

				if len(order_book.bids[best_price]) == 0:
					del order_book.bids[best_price]

				if marketType==1:
					price = best_price

				best_price = max(order_book.bids.keys()) if len(order_book.bids) > 0 \
								else None

			if order.leaves_qty > 0:
				depth = order_book.asks.setdefault(price, [])
				depth.append(order)
				order_book.order_id_map[order_id] = order

		return order, trades

	def cancelOrder(self,orderId,instmt):
		""""""
		assert instmt in self.orderBooks.keys(), \
				"Instrument %s is not valid in the order book" % instmt
		order_book = self.orderBooks[instmt]

		if order_id not in order_book.order_id_map.keys():
			return None

		order = order_book.order_id_map[order_id]
		order_price = order.price
		order_id = order.order_id
		side = order.side

		if side == Side.BUY:
			assert order_price in order_book.bids.keys(), \
				 "Order price %.6f is not in the bid price depth" % order_price
			price_level = order_book.bids[order_price]
		else:
			assert order_price in order_book.asks.keys(), \
				 "Order price %.6f is not in the ask price depth" % order_price
			price_level = order_book.asks[order_price]

		index = 0
		price_level_len = len(price_level)
		while index < price_level_len:
			if price_level[index].order_id == order_id:
				del price_level[index]
				break
			index += 1

		if index == price_level_len:
			return None

		if side == Side.BUY and len(order_book.bids[order_price]) == 0:
			del order_book.bids[order_price]
		elif side == Side.SELL and len(order_book.asks[order_price]) == 0:
			del order_book.asks[order_price]

		del order_book.order_id_map[order_id]

		order.leaves_qty = 0

		return order

	def reduce2state(self,orderTime:int):
		"""
		还原出OrderIndex（包括）之前的orderbook状态
		OrderTime:94010010
		"""
		sec = changeSec(orderTime)
		tmp_df = self.od[self.od.sec<sec].copy()

		self._Init()

		for r in zip(tmp_df['OrderIndex'],tmp_df['Price'],tmp_df['Volume'],tmp_df['FunctionCode'],tmp_df['MarketType']):
			if r[3] == 'C':
				self.cancel_order(r[0],self.InstrumentID)
				continue
			price = r[1]
			#if r[2]!='0':
				#price = 0
			if r[3]=='B':
				side = Side.BUY
			elif r[3] =='S':
				side = Side.SELL
			self.addOrder(self.InstrumentID,r[0],price,r[2],side,r[4])

	def predictTrade(self,orderTime:int,price,volume,side,marketType):
		"""
		输入自定义order，在给定的OrderBook状态下撮合返回成交的信息
		orderTime:94000500
		规则：每次虚拟撮合互不影响；假定以对手最优价格发单，5分钟不成交就撤单,14：50：00后不能报单
		这种orderkind，必然是以被动单的形式成交的
		"""
		self.reduce2state(orderTime)#orderbook状态已还原
		o,t = self.addOrder(self.InstrumentID,-1,price,volume,side,marketType)

		odRemained = self.od[self.od.sec>=orderTime].copy()
		startTime = changeSec(orderTime)
		endTime = changeSec(orderTime+300000)

		for r in zip(odRemained.OrderIndex,odRemained.Price,odRemained.Volume,odRemained.FunctionCode,odRemained.MarketType,odRemained.sec):
			        
			if r[5] >=endTime:
				break
			if r[3] == 'C':
				self.cancelOrder(r[0],self.InstrumentID)
				continue
			price = r[1]
			#  if r[3]!='0':
			#  price = 0
			if r[3]=='B':
				side = Side.BUY
			elif r[3] =='S':
				side = Side.SELL
			tmp_o,tmp_t = self.addOrder(self.InstruemntID,r[0],price,r[2],side,r[4])
			if tmp_t:
				if tmp_t[0].buy_id == -1 or tmp_t[0].sell_id == -1:
					t.append(tmp_t)
					o.leaves_qty -= tmp_t[0].trade_qty
					o.cum_qty += tmp_t[0].trade_qty

			if o.leaves_qty<=0:
				break
		return o,t

	def preprocessMarketOd(self):
		""""""
		od_market = self.od[self.od.OrderKind == '1']
		od_market_buy = od_market[od_market['FunctionCode']=='B']
		od_market_sell = od_market[od_market['FunctionCode']=='S']
		buy_id = od_market_buy.OrderIndex
		sell_id = od_market_sell.OrderIndex

		td_m_buy = self.td[self.td.BuyIndex.isin(buy_id)][['BuyIndex','TradeMillisec']]
		td_m_sell = self.td[self.td.SellIndex.isin(sell_id)][['SellIndex','TradeMillisec']]
		m_grp_buy = td_m_buy.groupby("BuyIndex")['TradeMillisec'].nunique()
		m_grp_sell = td_m_sell.groupby("SellIndex")['TradeMillisec'].nunique()
		buy_mar = m_grp_buy[m_grp_buy!=1].index
		sell_mar = m_grp_sell[m_grp_sell!=1].index
		spc_mar_od = buy_mar.append(sell_mar)

		self.od['MarketType'] = 0
		self.od.loc[self.od.OrderIndex.isin(spc_mar_od),'MarketType'] = 1
		self.od.loc[self.od.OrderKind == 'U','MarketType'] = 2

	def preprocessAfterCall(self):
		"""
		将连续竞价的撤单与报单按顺序
		"""
		td_canceled = self.td[self.td['FunctionCode']=='C']
		od2cancel = pd.DataFrame({"OrderIndex":td_canceled['Index'],
								  "OrderTime":td_canceled['TradeTime'],
								  "OrderMillisec":td_canceled['TradeMillisec'],
								  "Price":0,
								  "Volume":td_canceled['Volume'],
								  "OrderKind":0,
								  "FunctionCode":"C",
								  "TimeIndex":td_canceled['TradeIndex'],
								  "MarketType":0,
								  "sec":td_canceled["sec"]})
		self.od = pd.concat([self.od,od2cancel])
		self.od = self.od.sort_values(by='TimeIndex')

	def simCallStatus(self):
		"""
		还原出集合竞价撮合结束后的OrderBook状态，并缓存
		"""
		self.od['sec'] = self.od['OrderTime'].apply(datetime2sec)+self.od['OrderMillisec']
		self.td['sec'] = self.td['TradeTime'].apply(datetime2sec)+self.td['TradeMillisec']
		self.oq['sec'] = self.oq['OrderTime'].apply(datetime2sec)
		self.td['Index'] = self.td.apply(lambda x:max(x.BuyIndex,x.SellIndex),axis=1)
		od_call = self.od[self.od["sec"]<34200000]
		td_call = self.td[self.td["sec"]<34200000]
		self.od = self.od[(self.od["sec"]>=34200000) & (self.od["sec"]<53820000)]
		self.td = self.td[(self.td["sec"]>=34200000) & (self.td["sec"]<53820000)]

		odtd = pd.merge(od_call,td_call[td_call['FunctionCode']=='C'],how="left",left_on='OrderIndex',right_on='Index')
		od_no_cancel = odtd[odtd['FunctionCode_y']!='C']
		od_no_cancel = od_no_cancel.iloc[:,:10]
		od_no_cancel.rename(columns={'Price_x':"Price","Volume_x":"Volume","OrderKind_x":"OrderKind","FunctionCode_x":"FunctionCode","sec_x":"sec"},inplace=True)
		bid_after_call =  list(self.oq[self.oq.Direction==0]['Price'])[0]
		ask_after_call = list(self.oq[self.oq.Direction==1]['Price'])[0]
		od_filled = od_no_cancel[((od_no_cancel['FunctionCode']=='S')&(od_no_cancel['Price']<ask_after_call))|((od_no_cancel['FunctionCode']=='B')&(od_no_cancel['Price']>bid_after_call))]
		filled_buy = sum(od_filled[od_filled['FunctionCode']=='B'].Volume)
		filled_sell = sum(od_filled[od_filled['FunctionCode']=='S'].Volume)
		delta = abs(filled_buy - filled_sell)

		od_no_fill_buy = od_no_cancel[od_no_cancel['FunctionCode']=='B']
		od_no_fill_sell = od_no_cancel[od_no_cancel['FunctionCode']=='S']
		if(delta>0):
			tmp = od_no_fill_sell[od_no_fill_sell['Price']==ask_after_call]
			od_no_fill_buy = od_no_fill_buy[od_no_fill_buy['Price']<=bid_after_call]
			od_no_fill_sell = od_no_fill_sell[od_no_fill_sell['Price']>ask_after_call]
			for r in zip(tmp['Price'],tmp['Volume'],tmp['OrderIndex'],tmp['MarketType']):
				o,t = self.addOrder(self.InstrumentID,r[2],r[0],r[1],Side.SELL,r[3])
			o,t = self.addOrder(self.InstrumentID,0,ask_after_call,delta,Side.BUY,0)
			for r in zip(od_no_fill_buy['Price'],od_no_fill_buy['Volume'],od_no_fill_buy['OrderIndex'],od_no_fill_buy['MarketType']):
				o,t = self.addOrder(self.InstrumentID,r[2],r[0],r[1],Side.BUY,r[3])
			for r in zip(od_no_fill_sell['Price'],od_no_fill_sell['Volume'],od_no_fill_sell['OrderIndex'],od_no_fill_sell['MarketType']):
				o,t = self.addOrder(self.InstrumentID,r[2],r[0],r[1],Side.SELL,r[3])
		elif(delta<0):
			tmp = od_no_fill_buy[od_no_fill_buy['Price']==bid_after_call]
			od_no_fill_buy = od_no_fill_buy[od_no_fill_buy['Price']<bid_after_call]
			od_no_fill_sell = od_no_fill_sell[od_no_fill_sell['Price']>=ask_after_call]
			for r in zip(tmp['Price'],tmp['Volume'],tmp['OrderIndex'],tmp['MarketType']):
				o,t = self.addOrder(self.InstrumentID,r[2],r[0],r[1],Side.SELL,r[3])
			o,t = self.addOrder(self.InstruemntID,0,bid_after_call,delta,Side.SELL,0)
			for r in zip(od_no_fill_buy['Price'],od_no_fill_buy['Volume'],od_no_fill_buy['OrderIndex'],od_no_fill_buy['MarketType']):
				o,t = self.addOrder(self.InstrumentID,r[2],r[0],r[1],Side.BUY,r[3])
			for r in zip(od_no_fill_sell['Price'],od_no_fill_sell['Volume'],od_no_fill_sell['OrderIndex'],od_no_fill_sell['MarketType']):
				o,t = self.addOrder(self.InstrumentID,r[2],r[0],r[1],Side.SELL,r[3])
		else:
			for r in zip(od_no_fill_buy['Price'],od_no_fill_buy['Volume'],od_no_fill_buy['OrderIndex'],od_no_fill_buy['MarketType']):
				o,t = self.addOrder(self.InstrumentID,r[2],r[0],r[1],Side.BUY,r[3])
			for r in zip(od_no_fill_sell['Price'],od_no_fill_sell['Volume'],od_no_fill_sell['OrderIndex'],od_no_fill_sell['MarketType']):
				o,t = self.addOrder(self.InstrumentID,r[2],r[0],r[1],Side.SELL,r[3])

		self.statusAfterCall = self.orderBooks.copy()

class Side():
	BUY = 1
	SELL = 2

class OrderBook():
	def __init__(self):
		self.bids = {}
		self.asks = {}
		self.order_id_map = {}

class Order():
	def __init__(self, order_id, instmt, price, qty, side):
		self.order_id = order_id
		self.instmt = instmt
		self.price = price
		self.qty = qty
		self.cum_qty = 0
		self.leaves_qty = qty
		self.side = side

class Trade():
	def __init__(self, buy_id,sell_id, instmt, trade_price, trade_qty, trade_id):
		self.buy_id = buy_id
		self.sell_id = sell_id
		self.instmt = instmt
		self.trade_price = trade_price
		self.trade_qty = trade_qty
		self.trade_id = trade_id

def datetime2sec(s:datetime):
	""""""
	sec = s.hour*3600000+s.minute*60000+s.second*1000
	return sec

def changeSec(s:int):
	"""
	93000000->34200000
	"""
	hour = s//10000000
	minute = (s-hour*10000000)//100000
	sec = (s-hour*10000000-minute*100000)//1000
	microsec = s%1000
	return (hour*3600000+minute*60000+sec*1000+microsec)
