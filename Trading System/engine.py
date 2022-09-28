class Side(object):
    BUY = 1
    SELL = 2

class OrderBook(object):
    def __init__(self):
        self.bids = {}
        self.asks = {}
        self.order_id_map = {}


class Order(object):
    def __init__(self, order_id, instmt, price, qty, side):
        self.order_id = order_id
        self.instmt = instmt
        self.price = price
        self.qty = qty
        self.cum_qty = 0
        self.leaves_qty = qty
        self.side = side


class Trade(object):
    def __init__(self, buy_id,sell_id, instmt, trade_price, trade_qty, trade_id):
        self.buy_id = buy_id
        self.sell_id = sell_id
        self.instmt = instmt
        self.trade_price = trade_price
        self.trade_qty = trade_qty
        self.trade_id = trade_id
#一笔trade对应buy_index和sell_index

class MatchingEngine(object):
    def __init__(self):
        self.order_books = {}
        self.curr_trade_id = 0

    def add_order(self, instmt,order_id, price, qty, side, market_type):
        """
        market_type:市价转限价为1，U为2，其余为0
        Add an order
        调用一次add_order，返回生成的order对象和撮合完成的trade列表
        """
        assert side == Side.BUY or side == Side.SELL, \
                "Invalid side %s" % side

        order_book = self.order_books.setdefault(instmt, OrderBook())

        trades = []
        order_id = order_id
        order = Order(order_id, instmt, price, qty, side)

        if side == Side.BUY:
            best_price = min(order_book.asks.keys()) if len(order_book.asks) > 0 \
                            else None
            if market_type == 2:
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
                    self.curr_trade_id += 1
                    trades.append(Trade(order.order_id,hit_order.order_id, instmt, best_price, \
                                        order_match_qty, \
                                        self.curr_trade_id))
                    hit_order.cum_qty += order_match_qty
                    hit_order.leaves_qty -= order_match_qty
                    match_qty -= order_match_qty
                    if hit_order.leaves_qty == 0:
                        del order_book.asks[best_price][0]

                if len(order_book.asks[best_price]) == 0:
                    del order_book.asks[best_price]

                if market_type:
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
            if market_type == 2:
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
                    self.curr_trade_id += 1
                    trades.append(Trade(hit_order.order_id,order.order_id, instmt, best_price, \
                                        order_match_qty, \
                                        self.curr_trade_id))
                    hit_order.cum_qty += order_match_qty
                    hit_order.leaves_qty -= order_match_qty
                    match_qty -= order_match_qty
                    if hit_order.leaves_qty == 0:
                        del order_book.bids[best_price][0]

                if len(order_book.bids[best_price]) == 0:
                    del order_book.bids[best_price]

                if market_type==1:
                    price = best_price

                best_price = max(order_book.bids.keys()) if len(order_book.bids) > 0 \
                                else None

            if order.leaves_qty > 0:
                depth = order_book.asks.setdefault(price, [])
                depth.append(order)
                order_book.order_id_map[order_id] = order

        return order, trades

    def cancel_order(self, order_id, instmt):
        assert instmt in self.order_books.keys(), \
                "Instrument %s is not valid in the order book" % instmt
        order_book = self.order_books[instmt]

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



