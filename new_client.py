import ccxt.async_support as ccxt
import time
import pandas as pd
import toml
from loguru import logger
import requests
import pandas_ta as ta
import asyncio

# 配置loguru日志记录
logger.add("strategy_new_client.log", rotation="500MB", retention=3, level="INFO")
logger.add(lambda msg: send_feishu_notification(msg), level="CRITICAL")

# 加载配置文件
config = toml.load('config_new_client.toml')
okx_config = config['okx']
feishu_config = config['feishu']
trading_config = config['trading']

def send_feishu_notification(message):
    """Send a notification to Feishu (Lark) webhook."""
    try:
        headers = {
            'Content-Type': 'application/json',
        }
        payload = {
            "msg_type": "text",
            "content": {
                "text": message
            }
        }
        response = requests.post(FEISHU_WEBHOOK, headers=headers, json=payload)
        response.raise_for_status()
        logger.info("Feishu notification sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send Feishu notification: {e}")

# 获取飞书 Webhook URL
FEISHU_WEBHOOK = feishu_config['webhook_url']

# 获取交易参数
leverage = trading_config['leverage']
contract_amount = trading_config['contract_amount']

# 初始化交易所实例
exchange = ccxt.okx({
    'apiKey': okx_config['api_key'],
    'secret': okx_config['api_secret'],
    'password': okx_config['passphrase'],
})

# 定义时间间隔和K线数量
interval = '5m'
limit = 200  # 减少K线数量

# 全局变量
positions = {}  # 当前各币种持仓状态
entry_prices = {}  # 记录开仓价格
strategy_types = {}  # 记录开仓使用的策略类型

def check_original_entry_conditions(df):
    """检查原有的开仓条件"""
    try:
        # 获取倒数第二根K线
        current_kline = df.iloc[-2]
        
        # 检查条件1：EMA150是否大于其他所有均线和当前K线
        ema_columns = ['EMA5', 'EMA10', 'EMA24', 'EMA50']
        min_ema = min(current_kline[ema_columns])
        max_ema = max(current_kline[ema_columns])
        
        condition1 = all([
            current_kline['EMA150'] > current_kline[col] for col in ema_columns
        ]) and current_kline['high'] < current_kline['EMA150']
        
        # 检查条件2：开盘价小于最小EMA，收盘价大于最大EMA
        condition2 = (current_kline['open'] < min_ema and 
                     current_kline['close'] > max_ema)
        
        # 检查条件3：过去50根K线EMA150都大于其他均线
        condition3 = True
        for i in range(3, 54):  # 从倒数第三根到第53根
            historical_kline = df.iloc[-i]
            if not all([historical_kline['EMA150'] > historical_kline[col] 
                       for col in ema_columns]):
                condition3 = False
                break

        return all([condition1, condition2, condition3])
    except Exception as e:
        logger.error(f"Error checking original entry conditions: {e}")
        return False

def check_bullish_alignment(df, row):
    """检查均线多头排列"""
    return (row['EMA5'] > row['EMA10'] > 
            row['EMA24'] > row['EMA150'])

def check_unique_pattern(df, current_index):
    """检查是否是唯一符合条件的K线"""
    current_kline = df.iloc[current_index]
    
    # 获取过去53根K线的最大值
    historical_high = df['close'][current_index-52:current_index+1].max()
    
    # 检查当前K线的条件
    basic_condition = (current_kline['open'] < current_kline['EMA5'] and 
                      current_kline['close'] == historical_high)
    
    if not basic_condition:
        return False
        
    # 检查过去53根K线中是否有其他K线符合相同条件
    for i in range(current_index-52, current_index):
        check_kline = df.iloc[i]
        historical_high_for_check = df['close'][i-52:i+1].max()
        if (check_kline['open'] < check_kline['EMA5'] and 
            check_kline['close'] == historical_high_for_check):
            return False
            
    return True

def check_historical_below_ema150(df, current_index):
    """检查过去53根K线是否曾经出现过小于EMA150的情况"""
    for i in range(current_index-52, current_index+1):
        if df.iloc[i]['low'] < df.iloc[i]['EMA150']:
            return True
    return False

def check_new_entry_conditions(df):
    """检查新的开仓条件"""
    try:
        # 检查倒数第二根K线
        current_index = len(df) - 2
        current_kline = df.iloc[current_index]
        
        conditions = [
            # 1. 均线多头排列
            check_bullish_alignment(df, current_kline),
            
            # 2&3. 唯一符合条件的K线
            check_unique_pattern(df, current_index),
            
            # 4. 历史上出现过价格小于EMA150的情况
            check_historical_below_ema150(df, current_index)
        ]
        
        return all(conditions)
    except Exception as e:
        logger.error(f"Error checking new entry conditions: {e}")
        return False

def check_take_profit_condition(df, entry_price, strategy_type):
    """根据不同策略检查止盈条件"""
    try:
        current_price = df.iloc[-1]['close']
        
        if strategy_type == 'original':
            # 原策略：4%止盈
            return (current_price - entry_price) / entry_price > 0.04
        else:  # strategy_type == 'new'
            # 新策略：20%+EMA5交叉
            price_change = (current_price - entry_price) / entry_price
            return (price_change > 0.20 and 
                    df.iloc[-1]['close'] < df.iloc[-1]['EMA5'])
    except Exception as e:
        logger.error(f"Error checking take profit condition: {e}")
        return False

async def place_order_with_tp_sl(symbol, side, amount, entry_price, df, strategy_type, leverage=10, posSide='long'):
    """下单并设置止损"""
    try:
        # 根据策略类型设置止损比例
        stop_loss_percent = 0.02 if strategy_type == 'original' else 0.05
        stop_loss_price = entry_price * (1 - stop_loss_percent)
        
        # 设置止损参数
        params = {
            'leverage': leverage,
            'posSide': posSide,
            'stopLoss': {
                'triggerPrice': stop_loss_price,
                'price': stop_loss_price,
                'type': 'market'
            }
        }
        
        # 如果是原策略，添加止盈设置
        if strategy_type == 'original':
            take_profit_price = entry_price * 1.04
            params['takeProfit'] = {
                'triggerPrice': take_profit_price,
                'price': take_profit_price,
                'type': 'market'
            }

        # 下单
        order = await exchange.create_order(symbol, 'market', side, amount, None, params)
        logger.info(f"Placed {side} order for {symbol} with stop loss at {stop_loss_price}")
        return order['id']
    except Exception as e:
        logger.error(f"Failed to place order with SL for {symbol}: {e}")
        return None

async def close_position(symbol, amount):
    """平仓"""
    try:
        params = {'posSide': 'long'}
        order = await exchange.create_order(symbol, 'market', 'sell', amount, None, params)
        logger.info(f"Closed position for {symbol}")
        return order['id']
    except Exception as e:
        logger.error(f"Failed to close position for {symbol}: {e}")
        return None

def calculate_indicators(df):
    """计算各种均线指标"""
    # 计算EMA均线
    df['EMA5'] = ta.ema(df['close'], length=5)
    df['EMA10'] = ta.ema(df['close'], length=10)
    df['EMA24'] = ta.ema(df['close'], length=24)
    df['EMA50'] = ta.ema(df['close'], length=50)
    df['EMA150'] = ta.ema(df['close'], length=150)
    df.fillna(0, inplace=True)
    return df

import re

async def get_tradeable_symbols():
    """获取所有可交易的永续合约交易对，并排除带有数字的交易对"""
    markets = await exchange.load_markets()  # 使用 await 调用异步函数
    symbols = []
    
    # 正则表达式匹配带有数字的交易对（例如 BTC/USDT:USDT-250117）
    pattern = re.compile(r':USDT-\d+')
    
    for symbol in markets:
        # 只选择USDT永续合约，并排除带有数字的交易对
        if ':USDT' in symbol and not pattern.search(symbol):
            symbols.append(symbol)
    
    return symbols

async def process_symbol(symbol):
    """处理单个交易对的逻辑"""
    try:
        # 获取K线数据
        logger.info(f"Fetching OHLCV data for {symbol}...")
        klines = await exchange.fetch_ohlcv(symbol, interval, limit=limit)
        logger.info(f"Successfully fetched OHLCV data for {symbol}")

        df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)

        # 计算指标
        df = calculate_indicators(df)

        current_position = positions[symbol]
        
        # 检查止盈条件
        if current_position == 'long':
            strategy = strategy_types[symbol]
            if check_take_profit_condition(df, entry_prices[symbol], strategy):
                # 发送止盈通知
                message = f"### 止盈平仓\n币对: {symbol}\n策略: {strategy}\n时间: {pd.Timestamp.now()}\n入场价: {entry_prices[symbol]}\n当前价: {df.iloc[-1]['close']}"
                logger.critical(message)
                send_feishu_notification(message)
                
                # 平仓
                if await close_position(symbol, contract_amount):
                    positions[symbol] = None
                    entry_prices[symbol] = None
                    strategy_types[symbol] = None
        
        # 检查开仓条件
        elif current_position is None:
            # 检查原策略条件
            if check_original_entry_conditions(df):
                current_price = float((await exchange.fetch_ticker(symbol))['last'])
                
                message = f"### 开多单(原策略)\n币对: {symbol}\n时间: {pd.Timestamp.now()}\n价格: {current_price}\n数量: {contract_amount}张"
                logger.critical(message)
                send_feishu_notification(message)

                order_id = await place_order_with_tp_sl(symbol, 'buy', contract_amount, current_price, df, 'original', leverage, 'long')
                if order_id:
                    positions[symbol] = 'long'
                    entry_prices[symbol] = current_price
                    strategy_types[symbol] = 'original'
            
            # 检查新策略条件
            elif check_new_entry_conditions(df):
                current_price = float((await exchange.fetch_ticker(symbol))['last'])
                
                message = f"### 开多单(新策略)\n币对: {symbol}\n时间: {pd.Timestamp.now()}\n价格: {current_price}\n数量: {contract_amount}张"
                logger.critical(message)
                send_feishu_notification(message)

                order_id = await place_order_with_tp_sl(symbol, 'buy', contract_amount, current_price, df, 'new', leverage, 'long')
                if order_id:
                    positions[symbol] = 'long'
                    entry_prices[symbol] = current_price
                    strategy_types[symbol] = 'new'

    except Exception as e:
        logger.error(f"Error processing {symbol}: {e}")

async def main():
    global positions, entry_prices, strategy_types
    
    symbols = await get_tradeable_symbols()  # 使用 await 调用异步函数
    logger.info(f"Trading on {len(symbols)} symbols")
    logger.info(f"Symbols: {symbols}")  # 打印所有币对的名字

    # 初始化状态
    for symbol in symbols:
        positions[symbol] = None
        entry_prices[symbol] = None
        strategy_types[symbol] = None

    while True:
        try:
            # 读取控制信号
            with open('control_signal_new_client.txt', 'r') as f:
                signal = f.read().strip()

            if signal == 'stop':
                logger.critical("Strategy stopped by control signal")
                while True:
                    with open('control_signal_new_client.txt', 'r') as f:
                        if f.read().strip() == 'start':
                            break
                    await asyncio.sleep(2)
                continue

            # 分批处理交易对
            batch_size = 10
            for i in range(0, len(symbols), batch_size):
                batch_symbols = symbols[i:i + batch_size]
                tasks = [process_symbol(symbol) for symbol in batch_symbols]
                await asyncio.gather(*tasks)
                await asyncio.sleep(1)  # 增加请求间隔

            await asyncio.sleep(5)

        except Exception as e:
            logger.error(f"Main loop error: {e}")
            await asyncio.sleep(5)

if __name__ == '__main__':
    asyncio.run(main())
