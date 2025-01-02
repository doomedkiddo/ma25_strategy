import ccxt
import time
import pandas as pd
import toml
from loguru import logger
import requests
import pandas_ta as ta  # 导入 pandas_ta 库

# 配置loguru日志记录
logger.add("strategy.log", rotation="500MB", retention=3, level="INFO")
logger.add(lambda msg: send_feishu_notification(msg), level="CRITICAL")

# 加载配置文件
config = toml.load('config.toml')
okx_config = config['okx']
feishu_config = config['feishu']
trading_config = config['trading']

# 获取飞书 Webhook URL
FEISHU_WEBHOOK = feishu_config['webhook_url']

# 获取交易对、杠杆倍数和合约张数
symbol = trading_config['symbol'] + ':USDT'  # 永续合约交易对，例如 BTC/USDT:USDT
leverage = trading_config['leverage']
contract_amount = trading_config['contract_amount']

# 初始化交易所实例
exchange = ccxt.okx({
    'apiKey': okx_config['api_key'],
    'secret': okx_config['api_secret'],
    'password': okx_config['passphrase'],
})

# 设置杠杆倍数
exchange.set_leverage(leverage, symbol)

# 定义时间间隔
interval = '5m'
limit = 200  # 获取最近65根K线（包括当前K线）

# 全局变量
position = None  # 当前持仓状态（'long', 'short', None）
stop_loss_order_id = None  # 当前止损单ID

def fetch_usdt_balance():
    """获取账户 USDT 余额"""
    logger.info("Fetching USDT balance...")
    try:
        # 获取账户余额
        balance = exchange.fetch_balance()
        usdt_balance = balance['total'].get('USDT', 0)  # 获取 USDT 余额，如果没有则返回 0
        logger.info(f"Current USDT balance: {usdt_balance}")
        return usdt_balance
    except Exception as e:
        logger.error(f"Failed to fetch USDT balance: {e}")
        return None

def send_feishu_notification(message):
    """发送飞书通知"""
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "msg_type": "text",
        "content": {
            "title": "交易通知",
            "text": message
        }
    }
    response = requests.post(FEISHU_WEBHOOK, headers=headers, json=data)
    if response.status_code != 200:
        logger.error(f"Failed to send notification to Feishu: {response.text}")


def fetch_historical_klines(symbol, interval, limit):
    """获取历史K线数据"""
    logger.info(f"Fetching historical K-lines for {symbol} with interval {interval}...")
    klines = exchange.fetch_ohlcv(symbol, interval, limit=limit)
    df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True).dt.tz_convert('Asia/Shanghai')
    df.set_index('timestamp', inplace=True)
    logger.info(f"Fetched {len(df)} K-lines.")
    return df


def calculate_ma(df, window=25):
    """计算MA25"""
    logger.info("Calculating MA25...")
    df['MA25'] = ta.sma(df['close'], length=window)  # 使用 pandas_ta 计算 SMA
    logger.info(f"MA25 calculated for {len(df)} K-lines.")
    return df

def calculate_stop_loss_price(df, window=144, posSide='long'):
    """计算过去144根K线的最低点作为止损价格"""
    logger.info("Calculating stop loss price...")
    if posSide == 'long':
        stop_loss_price = df['low'].rolling(window=window).min().iloc[-1]
    elif posSide == 'short':
        stop_loss_price = df['high'].rolling(window=window).max().iloc[-1]
    logger.info(f"Stop loss price calculated: {stop_loss_price}")
    return stop_loss_price

def calculate_take_profit(entry_price, take_profit_points=2000, posSide='long'):
    """计算止盈价格"""
    if posSide == 'long':
        return entry_price + take_profit_points
    else:
        return entry_price - take_profit_points

def fetch_open_positions(symbol):
    """获取当前持仓"""
    logger.info(f"Fetching open positions for {symbol}...")
    positions = exchange.fetch_positions([symbol])
    for pos in positions:
        if pos['symbol'] == symbol and float(pos['contracts']) > 0:
            if pos['side'] == 'long':
                logger.info("Current position: LONG")
                return 'long'
            elif pos['side'] == 'short':
                logger.info("Current position: SHORT")
                return 'short'
    logger.info("Current position: NONE")
    return None


def get_current_price(symbol):
    """获取当前价格"""
    ticker = exchange.fetch_ticker(symbol)
    return float(ticker['last'])


def place_limit_order(symbol, side, amount, price=None, leverage=10, posSide='long'):
    """下限价单"""
    order_type = 'limit'
    logger.info(f"Placing {side} {order_type} order for {amount} {symbol} at price {price} with leverage {leverage}...")
    params = {'leverage': leverage, 'posSide': posSide}
    if side == 'buy':
        order = exchange.create_limit_buy_order(symbol, amount, price, params)
    elif side == 'sell':
        order = exchange.create_limit_sell_order(symbol, amount, price, params)
    logger.info(f"Placed {side} {order_type} order: {order}")
    return order['id']


def place_market_order(symbol, side, amount, leverage=10, posSide='long'):
    """下市价单"""
    order_type = 'market'
    logger.info(f"Placing {side} {order_type} order for {amount} {symbol} with leverage {leverage}...")
    params = {'leverage': leverage, 'posSide': posSide}
    if side == 'buy':
        order = exchange.create_market_buy_order(symbol, amount, params)
    elif side == 'sell':
        order = exchange.create_market_sell_order(symbol, amount, params)
    logger.info(f"Placed {side} {order_type} order: {order}")
    return order['id']

def place_order_with_tp_sl(symbol, side, amount, entry_price, leverage=10, posSide='long'):
    """下单并设置止盈和止损"""
    try:
        # 计算止损价格和止盈价格
        stop_loss_price = calculate_stop_loss_price(df)
        take_profit_price = calculate_take_profit(entry_price, posSide)

        # 下单
        if side == 'buy':
            order = place_limit_order(symbol, 'buy', amount, entry_price, leverage, posSide)
        elif side == 'sell':
            order = place_limit_order(symbol, 'sell', amount, entry_price, leverage, posSide)

        # 设置止损单
        stop_loss_order_id = place_stop_loss_order(symbol, side, amount, stop_loss_price, leverage, posSide)

        # 设置止盈单
        take_profit_order_id = place_limit_order(symbol, 'sell' if side == 'buy' else 'buy', amount, take_profit_price, leverage, posSide)

        logger.info(f"Placed {side} order with stop loss at {stop_loss_price} and take profit at {take_profit_price}")
        return order['id'], stop_loss_order_id, take_profit_order_id
    except Exception as e:
        logger.error(f"Failed to place order with TP/SL: {e}")
        return None, None, None


def place_stop_loss_order(symbol, side, amount, stop_price=None, leverage=10, posSide='long'):
    """下止损单并立即反向开单"""
    try:
        # 确保 stop_price 和 amount 是数字
        stop_price = float(stop_price)
        amount = int(amount)
    except (ValueError, TypeError) as e:
        logger.error(f"Invalid stop_price or amount: {e}")
        return None

    # 市价平仓
    try:
        logger.info(f"Closing position for {symbol} with {side} market order...")
        if side == 'sell':  # 平多单
            close_order = exchange.create_market_sell_order(symbol, amount, {'leverage': leverage, 'posSide': posSide})
        elif side == 'buy':  # 平空单
            close_order = exchange.create_market_buy_order(symbol, amount, {'leverage': leverage, 'posSide': posSide})
        logger.info(f"Closed position: {close_order}")
    except Exception as e:
        logger.error(f"Failed to close position: {e}")
        return None

    # 反向开单
    try:
        reverse_side = 'buy' if side == 'sell' else 'sell'  # 反向开单的方向
        reverse_posSide = 'short' if posSide == 'long' else 'long'  # 反向开单的持仓方向
        logger.info(f"Placing reverse {reverse_side} market order for {symbol}...")
        reverse_order = exchange.create_market_order(symbol, reverse_side, amount, {'leverage': leverage, 'posSide': reverse_posSide})
        logger.info(f"Placed reverse order: {reverse_order}")
        return reverse_order['id']
    except Exception as e:
        logger.error(f"Failed to place reverse order: {e}")
        return None


def cancel_order(order_id, symbol):
    """取消订单"""
    logger.info(f"Canceling order {order_id} for {symbol}...")
    exchange.cancel_order(order_id, symbol)
    logger.info(f"Cancelled order: {order_id}")


def update_klines(df, symbol, interval):
    """更新K线数据并重新计算MA25"""
    logger.info("Fetching latest K-line...")
    new_klines = exchange.fetch_ohlcv(symbol, interval, limit=1)
    if new_klines and new_klines[-1][0] > df.index[-1].timestamp() * 1000:
        # 添加新的 K 线
        new_df = pd.DataFrame(new_klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        new_df['timestamp'] = pd.to_datetime(new_df['timestamp'], unit='ms', utc=True).dt.tz_convert('Asia/Shanghai')
        new_df.set_index('timestamp', inplace=True)
        df = pd.concat([df, new_df])
        df = df[~df.index.duplicated(keep='last')]
    else:
        # 更新最后一根 K 线
        current_price = get_current_price(symbol)
        df.iloc[-1, df.columns.get_loc('close')] = current_price
        df.iloc[-1, df.columns.get_loc('high')] = max(df.iloc[-1]['high'], current_price)
        df.iloc[-1, df.columns.get_loc('low')] = min(df.iloc[-1]['low'], current_price)

    # 重新计算MA25
    df = calculate_ma(df, window=25)
    return df


def main():
    global position, stop_loss_order_id
    # 获取当前 USDT 余额
    usdt_balance = fetch_usdt_balance()

    # 发送启动交易的飞书通知
    start_message = "### 交易策略启动\n时间: {}\n交易对: {}\n杠杆倍数: {}\n合约张数: {}\n当前 USDT 余额: {}".format(
        pd.Timestamp.now(), symbol, leverage, contract_amount, usdt_balance
    )
    logger.critical(start_message)
    send_feishu_notification(start_message)

    # 初始化数据
    df = fetch_historical_klines(symbol, interval, limit)
    df = calculate_ma(df, window=25)  # 计算 MA25

    while True:
        try:
            # 读取控制信号
            with open('control_signal.txt', 'r') as f:
                signal = f.read().strip()

            if signal == 'stop':
                stop_message = "### 交易策略停止\n时间: {}\n状态: 收到停止信号".format(pd.Timestamp.now())
                logger.critical(stop_message)
                send_feishu_notification(stop_message)
                while True:
                    with open('control_signal.txt', 'r') as f:
                        signal = f.read().strip()
                    if signal == 'start':
                        start_message = "### 交易策略恢复运行\n时间: {}\n状态: 收到开始信号".format(pd.Timestamp.now())
                        logger.critical(start_message)
                        send_feishu_notification(start_message)
                        break  # 退出等待状态，继续交易逻辑
                    time.sleep(2)  # 每隔5秒检查一次信号

            # 如果读到开始信号，则继续交易
            if signal == 'start':
                # 更新K线数据
                df = update_klines(df, symbol, interval)

                # 获取前一根和当前根K线
                prev_kline = df.iloc[-3]
                current_kline = df.iloc[-2]

                # 获取当前持仓
                position = fetch_open_positions(symbol)

                # 打印当前K线和MA25的值
                logger.info(f"Current K-line:Open={current_kline['open']}, High={current_kline['high']}, Low={current_kline['low']}, Close={current_kline['close']}")
                logger.info(f"Current MA25: {df['MA25'].iloc[-1]}")

                # 只有在没有持仓时才检测开单条件
                if position is None:
                    # 开多单条件
                    if (current_kline['low'] >= df['MA25'].iloc[-2] and
                        current_kline['close'] > df['MA25'].iloc[-2] and
                        prev_kline['low'] <= df['MA25'].iloc[-3]):
                        # 获取当前价格
                        current_price = get_current_price(symbol)
                        message = f"### 开多单\n时间: {pd.Timestamp.now()}\n价格: {current_price}\n数量: {contract_amount}张"
                        logger.critical(message)

                        # 详细日志记录开单条件
                        condition_message = (
                            f"开多单条件满足，详细条件如下：\n"
                            f"当前K线最低价: {current_kline['low']} >= MA25: {df['MA25'].iloc[-2]}\n"
                            f"当前K线收盘价: {current_kline['close']} > MA25: {df['MA25'].iloc[-2]}\n"
                            f"前一根K线最低价: {prev_kline['low']} <= MA25: {df['MA25'].iloc[-3]}\n"
                            f"前一根K线信息: 开盘价={prev_kline['open']}, 最高价={prev_kline['high']}, 最低价={prev_kline['low']}, 收盘价={prev_kline['close']}"
                        )
                        logger.info(condition_message)

                        # 发送飞书通知
                        send_feishu_notification(f"{message}\n\n{condition_message}")

                        # 下限价单
                        order_id = place_limit_order(symbol, 'buy', contract_amount, current_price, leverage, posSide='long')
                        time.sleep(2)  # 等待2秒检查订单状态
                        order_status = exchange.fetch_order(order_id, symbol)
                        if order_status['status'] != 'closed':
                            # 撤销限价单，下市价单
                            cancel_order(order_id, symbol)
                            place_market_order(symbol, 'buy', contract_amount, leverage, posSide='long')
                        position = 'long'

                        # 设置止损和止盈
                        stop_loss_price = calculate_stop_loss_price(df, posSide='long')
                        take_profit_price = calculate_take_profit(current_price, posSide='long')
                        place_stop_loss_order(symbol, 'sell', contract_amount, stop_loss_price, leverage, posSide='long')
                        place_limit_order(symbol, 'sell', contract_amount, take_profit_price, leverage, posSide='long')

                    # 开空单条件
                    elif (current_kline['close'] < df['MA25'].iloc[-2] and
                          current_kline['high'] < df['MA25'].iloc[-2] and
                          prev_kline['high'] >= df['MA25'].iloc[-3]):
                        # 获取当前价格
                        current_price = get_current_price(symbol)
                        message = f"### 开空单\n时间: {pd.Timestamp.now()}\n价格: {current_price}\n数量: {contract_amount}张"
                        logger.critical(message)

                        # 详细日志记录开单条件
                        condition_message = (
                            f"开空单条件满足，详细条件如下：\n"
                            f"当前K线收盘价: {current_kline['close']} < MA25: {df['MA25'].iloc[-2]}\n"
                            f"当前K线最高价: {current_kline['high']} < MA25: {df['MA25'].iloc[-2]}\n"
                            f"前一根K线最高价: {prev_kline['high']} >= MA25: {df['MA25'].iloc[-3]}\n"
                            f"前一根K线信息: 开盘价={prev_kline['open']}, 最高价={prev_kline['high']}, 最低价={prev_kline['low']}, 收盘价={prev_kline['close']}"
                        )
                        logger.info(condition_message)

                        # 发送飞书通知
                        send_feishu_notification(f"{message}\n\n{condition_message}")

                        # 下限价单
                        order_id = place_limit_order(symbol, 'sell', contract_amount, current_price, leverage, posSide='short')
                        time.sleep(2)  # 等待2秒检查订单状态
                        order_status = exchange.fetch_order(order_id, symbol)
                        if order_status['status'] != 'closed':
                            # 撤销限价单，下市价单
                            cancel_order(order_id, symbol)
                            place_market_order(symbol, 'sell', contract_amount, leverage, posSide='short')
                        position = 'short'

                        # 设置止损和止盈
                        stop_loss_price = calculate_stop_loss_price(df, posSide='short')
                        take_profit_price = calculate_take_profit(current_price, posSide='short')
                        place_stop_loss_order(symbol, 'buy', contract_amount, stop_loss_price, leverage, posSide='short')
                        place_limit_order(symbol, 'buy', contract_amount, take_profit_price, leverage, posSide='short')
            time.sleep(5)
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
