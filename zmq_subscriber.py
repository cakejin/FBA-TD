import asyncio
import json
import zmq
import zmq.asyncio


async def run_subscriber(endpoint: str = "tcp://127.0.0.1:5556"):
    ctx = zmq.asyncio.Context()
    sock = ctx.socket(zmq.SUB)
    sock.connect(endpoint)
    sock.setsockopt_string(zmq.SUBSCRIBE, "")
    print(f"[ZMQ SUB] Connected to {endpoint}")

    try:
        while True:
            msg = await sock.recv_string()
            try:
                data = json.loads(msg)
                print("Received:", data)
            except Exception:
                print("Received raw:", msg)
    except asyncio.CancelledError:
        pass
    finally:
        sock.close()


if __name__ == "__main__":
    asyncio.run(run_subscriber())
