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
limit = 200  # 获取最近200根K线

# 全局变量
long_position = None  # 当前多单持仓状态
short_position = None  # 当前空单持仓状态

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

def calculate_ma(df, window=60):
    """计算MA60"""
    logger.info(f"Calculating MA{window}...")
    df[f'MA{window}'] = ta.sma(df['close'], length=window)  # 使用 pandas_ta 计算 SMA
    logger.info(f"MA{window} calculated for {len(df)} K-lines.")
    return df

def calculate_take_profit(entry_price, take_profit_points=750, posSide='long'):
    """计算止盈价格"""
    if posSide == 'long':
        return entry_price + take_profit_points
    else:
        return entry_price - take_profit_points

def calculate_stop_loss(entry_price, stop_loss_points=3000, posSide='long'):
    """计算止损价格"""
    if posSide == 'long':
        return entry_price - stop_loss_points
    else:
        return entry_price + stop_loss_points

def fetch_open_positions(symbol):
    """获取当前持仓"""
    logger.info(f"Fetching open positions for {symbol}...")
    positions = exchange.fetch_positions([symbol])
    long_pos = None
    short_pos = None
    for pos in positions:
        if pos['symbol'] == symbol and float(pos['contracts']) > 0:
            if pos['side'] == 'long':
                long_pos = 'long'
            elif pos['side'] == 'short':
                short_pos = 'short'
    logger.info(f"Current positions - Long: {long_pos}, Short: {short_pos}")
    return long_pos, short_pos

def get_current_price(symbol):
    """获取当前价格"""
    ticker = exchange.fetch_ticker(symbol)
    return float(ticker['last'])

def place_order_with_tp_sl(symbol, side, amount, entry_price, leverage=10, posSide='long'):
    """下单并设置止盈和止损"""
    try:
        # 计算止损价格和止盈价格
        take_profit_price = calculate_take_profit(entry_price, posSide=posSide)
        stop_loss_price = calculate_stop_loss(entry_price, posSide=posSide)

        # 设置止盈止损参数
        params = {
            'leverage': leverage,
            'posSide': posSide,
            'stopLoss': {
                'triggerPrice': stop_loss_price,
                'price': stop_loss_price,
                'type': 'market'  # 止损单类型，可以是 'limit' 或 'market'
            },
            'takeProfit': {
                'triggerPrice': take_profit_price,
                'price': take_profit_price,
                'type': 'market'  # 止盈单类型，可以是 'limit' 或 'market'
            }
        }

        # 下单
        order = exchange.create_order(symbol, 'market', side, amount, entry_price, params)
        logger.info(f"Placed {side} order with stop loss at {stop_loss_price} and take profit at {take_profit_price}")
        return order['id']
    except Exception as e:
        logger.error(f"Failed to place order with TP/SL: {e}")
        return None

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

def update_klines(df, symbol, interval):
    """更新K线数据并重新计算MA60"""
    global long_position, short_position

    logger.info("Fetching latest K-line...")
    new_klines = exchange.fetch_ohlcv(symbol, interval, limit=1)
    current_price = get_current_price(symbol)
    if new_klines and new_klines[-1][0] > df.index[-1].timestamp() * 1000:
        # 添加新的 K 线
        new_df = pd.DataFrame(new_klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        new_df['timestamp'] = pd.to_datetime(new_df['timestamp'], unit='ms', utc=True).dt.tz_convert('Asia/Shanghai')
        new_df.set_index('timestamp', inplace=True)
        df = pd.concat([df, new_df])
        df = df[~df.index.duplicated(keep='last')]
    else:
        # 更新最后一根 K 线
        df.iloc[-1, df.columns.get_loc('close')] = current_price
        df.iloc[-1, df.columns.get_loc('high')] = max(df.iloc[-1]['high'], current_price)
        df.iloc[-1, df.columns.get_loc('low')] = min(df.iloc[-1]['low'], current_price)

    # 重新计算MA60
    df = calculate_ma(df, window=60)

    return df

def main():
    global long_position, short_position
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
    df = calculate_ma(df, window=60)  # 计算 MA60

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
                long_position, short_position = fetch_open_positions(symbol)

                # 打印当前K线和MA60的值
                logger.info(f"Current K-line: Open={current_kline['open']}, High={current_kline['high']}, Low={current_kline['low']}, Close={current_kline['close']}")
                logger.info(f"Current MA60: {df['MA60'].iloc[-1]}")

                # 只有在没有持仓时才检测开单条件
                if long_position is None and short_position is None:
                    # 开多单条件：K线上穿MA60，收盘价在MA60以上
                    if (current_kline['low'] >= df['MA60'].iloc[-2] and
                        current_kline['close'] > df['MA60'].iloc[-2] and
                        prev_kline['low'] <= df['MA60'].iloc[-3]):
                        # 获取当前价格
                        current_price = get_current_price(symbol)
                        message = f"### 开多单\n时间: {pd.Timestamp.now()}\n价格: {current_price}\n数量: {contract_amount}张"
                        logger.critical(message)

                        # 详细日志记录开单条件
                        condition_message = (
                            f"开多单条件满足，详细条件如下：\n"
                            f"前一根K线收盘价: {prev_kline['close']} < MA60: {df['MA60'].iloc[-2]}\n"
                            f"当前K线收盘价: {current_kline['close']} > MA60: {df['MA60'].iloc[-1]}\n"
                            f"前一根K线信息: 开盘价={prev_kline['open']}, 最高价={prev_kline['high']}, 最低价={prev_kline['low']}, 收盘价={prev_kline['close']}"
                        )
                        logger.info(condition_message)

                        # 发送飞书通知
                        send_feishu_notification(f"{message}\n\n{condition_message}")

                        # 下限价单并设置止盈止损
                        place_order_with_tp_sl(symbol, 'buy', contract_amount, current_price, leverage, posSide='long')
                        long_position = 'long'

                    # 开空单条件：K线跌破MA60
                    elif (current_kline['close'] < df['MA60'].iloc[-2] and
                          current_kline['high'] < df['MA60'].iloc[-2] and
                          prev_kline['high'] >= df['MA60'].iloc[-3]):
                        # 获取当前价格
                        current_price = get_current_price(symbol)
                        message = f"### 开空单\n时间: {pd.Timestamp.now()}\n价格: {current_price}\n数量: {contract_amount}张"
                        logger.critical(message)

                        # 详细日志记录开单条件
                        condition_message = (
                            f"开空单条件满足，详细条件如下：\n"
                            f"前一根K线收盘价: {prev_kline['close']} > MA60: {df['MA60'].iloc[-2]}\n"
                            f"当前K线收盘价: {current_kline['close']} < MA60: {df['MA60'].iloc[-1]}\n"
                            f"前一根K线信息: 开盘价={prev_kline['open']}, 最高价={prev_kline['high']}, 最低价={prev_kline['low']}, 收盘价={prev_kline['close']}"
                        )
                        logger.info(condition_message)

                        # 发送飞书通知
                        send_feishu_notification(f"{message}\n\n{condition_message}")

                        # 下限价单并设置止盈止损
                        place_order_with_tp_sl(symbol, 'sell', contract_amount, current_price, leverage, posSide='short')
                        short_position = 'short'
            time.sleep(5)
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            time.sleep(5)

if __name__ == '__main__':
    main()
