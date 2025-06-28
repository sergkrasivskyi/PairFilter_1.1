import asyncio, json, time
import websockets           # pip install websockets

STREAM = "btcusdt@kline_1m"  # символ і таймфрейм

async def main() -> None:
    url = f"wss://stream.binance.com:9443/ws/{STREAM}"
    async with websockets.connect(url) as ws:
        print("WS open", url)
        for _ in range(10):                 # 3 повідомлення достатньо
            raw = await ws.recv()
            pkt = json.loads(raw)
            k = pkt["k"]                   # сам kline-об’єкт
            print(time.ctime(k["T"]//1000),
                  k["s"], "close=", k["c"], "final=", k["x"])

if __name__ == "__main__":
    asyncio.run(main())
