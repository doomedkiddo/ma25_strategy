from flask import Flask, render_template, request, jsonify, send_from_directory, Response
import subprocess
import os
import signal
import time
import psutil  # 用于查找和终止子进程
import requests  # 用于发送飞书消息
import json
import toml  # 用于加载 TOML 配置文件

app = Flask(__name__)

# 加载配置文件
config = toml.load('config.toml')
feishu_config = config['feishu']

# 获取飞书 Webhook URL
FEISHU_WEBHOOK_URL = feishu_config['webhook_url']

# 全局变量，用于存储交易程序的进程
trading_process = None

def send_feishu_message(message):
    """发送飞书消息"""
    headers = {"Content-Type": "application/json"}
    payload = {
        "msg_type": "text",
        "content": {
            "text": message
        }
    }
    try:
        response = requests.post(FEISHU_WEBHOOK_URL, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        print("飞书消息发送成功")
    except requests.exceptions.RequestException as e:
        print(f"飞书消息发送失败: {e}")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/start', methods=['POST'])
def start_trading():
    try:
        # 写入开始信号到文件
        with open('control_signal.txt', 'w') as f:
            f.write('start')
        
        # 发送飞书消息
        start_time = time.strftime("%Y-%m-%d %H:%M:%S")
        message = f"交易策略已启动\n时间: {start_time}\n状态: 成功写入开始信号"
        send_feishu_message(message)

        return jsonify({'status': 'success', 'message': 'Trading strategy start signal sent'})
    except Exception as e:
        # 发送飞书消息（异常情况）
        start_time = time.strftime("%Y-%m-%d %H:%M:%S")
        message = f"交易策略启动失败\n时间: {start_time}\n错误: {str(e)}"
        send_feishu_message(message)

        return jsonify({'status': 'error', 'message': f'Failed to send start signal: {str(e)}'})


@app.route('/stop', methods=['POST'])
def stop_trading():
    try:
        # 写入停止信号到文件
        with open('control_signal.txt', 'w') as f:
            f.write('stop')
        
        # 发送飞书消息
        stop_time = time.strftime("%Y-%m-%d %H:%M:%S")
        message = f"交易策略已停止\n时间: {stop_time}\n状态: 成功写入停止信号"
        send_feishu_message(message)

        return jsonify({'status': 'success', 'message': 'Trading strategy stop signal sent'})
    except Exception as e:
        # 发送飞书消息（异常情况）
        stop_time = time.strftime("%Y-%m-%d %H:%M:%S")
        message = f"交易策略停止失败\n时间: {stop_time}\n错误: {str(e)}"
        send_feishu_message(message)

        return jsonify({'status': 'error', 'message': f'Failed to send stop signal: {str(e)}'})

@app.route('/test', methods=['POST'])
def test():
    try:
        # 运行测试程序
        subprocess.run(['python3', 'test_ma.py'], check=True)
        # 返回图片的URL
        return jsonify({'status': 'success', 'message': 'Test completed', 'image_url': '/static/test_image.png'})
    except subprocess.CalledProcessError as e:
        return jsonify({'status': 'error', 'message': f'Test failed: {str(e)}'})

@app.route('/static/<filename>')
def serve_static(filename):
    return send_from_directory('static', filename)

@app.route('/stream_logs')
def stream_logs():
    def generate():
        try:
            with open('strategy.log', 'r') as f:
                f.seek(0, 2)  # 移动到文件末尾
                while True:
                    line = f.readline()
                    if not line:
                        try:
                            time.sleep(0.1)  # 等待新日志
                        except (SystemExit, KeyboardInterrupt):
                            print("Log streaming interrupted. Exiting gracefully.")
                            break
                        continue
                    yield f"data: {line}\n\n"
        except FileNotFoundError:
            yield "data: Log file not found.\n\n"
        except Exception as e:
            yield f"data: Error reading log file: {str(e)}\n\n"
    return Response(generate(), mimetype='text/event-stream')

@app.route('/run_ma60', methods=['POST'])
def run_ma60():
    try:
        # 运行 ma60.py
        subprocess.Popen(['python3', 'ma60.py'])
        
        # 发送飞书消息
        start_time = time.strftime("%Y-%m-%d %H:%M:%S")
        message = f"MA60 策略已启动\n时间: {start_time}\n状态: 成功运行"
        send_feishu_message(message)

        return jsonify({'status': 'success', 'message': 'MA60 strategy started'})
    except Exception as e:
        # 发送飞书消息（异常情况）
        start_time = time.strftime("%Y-%m-%d %H:%M:%S")
        message = f"MA60 策略启动失败\n时间: {start_time}\n错误: {str(e)}"
        send_feishu_message(message)

        return jsonify({'status': 'error', 'message': f'Failed to start MA60 strategy: {str(e)}'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
