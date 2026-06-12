import websocket
import json
import requests
import threading  

TOKEN = "d8ldudhr01qtamgu9it0d8ldudhr01qtamgu9itg"
FLUME_URL = "http://localhost:44444"

def on_open(ws):
    print("CONNECTED TO FINNHUB")

    ws.send(json.dumps({
        "type": "subscribe",
        "symbol": "BINANCE:BTCUSDT"
    }))

def send_to_flume_async(message):
    flume_event = [{"body": message}]
    try:
        response = requests.post(FLUME_URL, json=flume_event, timeout=2) 
        if response.status_code == 200:
            print("--> Successfully sent to Flume!")
        else:
            print(f"--> Flume responded with error status: {response.status_code}")
    except Exception as e:
        print("--> Failed to connect to Flume Agent:", e)

def on_message(ws, message):
    print("RECEIVED FROM FINNHUB:", message)
    
    data = json.loads(message)
    if data.get("type") == "ping":
        return 
        
    threading.Thread(target=send_to_flume_async, args=(message,), daemon=True).start()

def on_error(ws, error):
    print("ERROR:", error)

def on_close(ws, close_status_code, close_msg):
    print(f"CLOSED | Status: {close_status_code} | Msg: {close_msg}")

ws = websocket.WebSocketApp(
    "wss://ws.finnhub.io?token=" + TOKEN,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

print("STARTING PYTHON PRODUCER...")
ws.run_forever()
print("FINISHED")
