from Engine import *
import numpy as np 
import pandas as pd

class Backtester(Engine):
	"""
	回测框架，在撮合引擎的基础上，添加如下功能：
	orderAction:np.array
	上午第一个点：9：30：03，最后一个点：11：29：57
	下午第一个点：13：00：03，最后一个点：14：50：00
	一共4597个点
	第一个因子时间戳为09:30:03.000->34203000
	"""
	def __init__(self,InstrumentID,TradingDay,orderAction):
		""""""
		super().__init__(InstrumentID:str,TradingDay:str)

		self.OrderTime = np.append(np.arange(34203000,41397000,3000),np.arange(46803000,53400000,3000))
		self.order2place = pd.DataFrame({"OrderTime":self.OrderTime,"Action":orderAction})
		
		self.trade = []

		self.timeIndex = None

		self.action2side = {-1:Side.SELL,1:Side.BUY}

	def run(self):
		"""
		self.fator_seq已知每一时刻的action，即在t时刻，存在0，-1，1三种可能
		通过for循环遍历行，如果是1或者-1，下单之后通过self.allTraded跟踪上笔
		委托是否全部完成
		只需要遍历3s整点时刻，如果需要下单，则predictTrade，储存信息
		然后找到成交的时刻，从下个整点开始判断下单
		"""
		nextTradeTime = None
		for orderTime in self.OrderTime:
			if orderTime != nextTradeTime:
				continue
			action = self.order2place[self.order2place.OrderTime==orderTime]
			if action:
				side = self.action2side[action]
				tmp_o,tmp_t = self.predictTrade(time,0,500,side,2)
				if tmp_t:
					self.trade.append(tmp_t.copy())
				#如果完成成交完，则用index找时间
				#如果没成交完，则+5分钟
				if not o.leave_qty:
					if side == Side.BUY:
						odIndex = tmp_t[-1].sell_id
					else:
						odIndex = tmp_t[-1].buy_id
					nextTime = 	self.findNewestFactTime(odIndex)
					nextTradeTime = nextTime
					continue 
				else:
					nextTradeTime = orderTime + 300000
					if((nextTradeTime-41400000)*(46800000-nextTradeTime)>0:
						nextTradeTime = 46800000+(nextTradeTime-41400000)
					continue


	def findNewestFactTime(self,orderIndex):
		""""""
		tmp = self.od[self.od.OrderIndex==orderIndex]
		sec = tmp.iloc[0,8].copy()
		sec = sec-sec%1000
		residual = sec%3000
		sec -= residual
		if residual:
			sec += 3000
		else:
			sec += 6000
		return sec

	def calculate_result(self):
		""""""
		pass