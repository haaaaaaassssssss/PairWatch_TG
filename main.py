import asyncio
import base64
import json
import logging
import os
import random
import traceback
import time
import urllib
from asyncio import WindowsSelectorEventLoopPolicy
from datetime import datetime
import websockets
from telegram.ext import Application, CommandHandler
from telegram.helpers import escape_markdown
from commands import add, delete, list_links, get_redis
from utils import setup_logging, convert_to_hashable
from urllib.parse import urlparse
from websockets_proxy import Proxy, proxy_connect

# os.environ["PYTHONASYNCIODEBUG"] = "1"

prod_chat = '@pairpeekbot'
test_chat = '@pairpeektest'
telegram_bot_logger = logging.getLogger('telegram')
telegram_bot_logger.setLevel(logging.WARNING + 1)


def format_age(timestamp_str):
    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
    now = datetime.utcnow()
    delta = now - timestamp
    total_seconds = int(delta.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    if hours > 0:
        return f"{hours}h {minutes}m"
    elif minutes > 0:
        return f"{minutes}m"
    else:
        return f"{seconds}s"


async def parse(json_data):
    try:
        pairs = json_data.get("pairs", [])
        parsed_pairs = []

        for pair in pairs:
            try:
                base_token = pair.get("baseToken", {})
                token_name = base_token.get("name", "")
                pair_address = pair.get("pairAddress", "")
                link = f"https://dexscreener.com/solana/{pair_address}"
                created_at = pair.get("pairCreatedAt", 0)
                age = datetime.utcfromtimestamp(created_at / 1000).strftime(
                    '%Y-%m-%d %H:%M:%S') if created_at else "N/A"
                makers = pair.get("makers", {}).get("h24", 0)
                volume = pair.get("volume", {}).get("h24", 0.0)
                fdv = pair.get("marketCap", 0)
                price_changes = [pair.get("priceChange", {}).get(interval, 0) for interval in ['m5', 'h1', 'h6', 'h24']]

                parsed_pair = {
                    "token_name": token_name,
                    "link": link,
                    "age": age,
                    "makers": makers,
                    "volume": volume,
                    "fdv": fdv,
                    "price_changes": price_changes
                }
                parsed_pairs.append(parsed_pair)
            except Exception as e:
                logging.debug(f"Error processing pair: {e}")

        return parsed_pairs
    except Exception as e:
        # print(f"Error parsing JSON data: {e}  {json_data}")
        return []


def http_to_websocket(http_url):
    parsed_url = urllib.parse.urlparse(http_url)
    query_params = urllib.parse.parse_qs(parsed_url.query)

    websocket_base = "wss://io.dexscreener.com/dex/screener/pairs/h24/1?"

    conversion_map = {
        'chainIds': ('filters', 'chainIds', 0),
        'dexIds': ('filters', 'dexIds', 0),
        'minLiq': ('filters', 'liquidity', 'min'),
        'maxLiq': ('filters', 'liquidity', 'max'),
        'minFdv': ('filters', 'marketCap', 'min'),
        'maxFdv': ('filters', 'marketCap', 'max'),
        'minAge': ('filters', 'pairAge', 'min'),
        'maxAge': ('filters', 'pairAge', 'max'),
        'min24HTxns': ('filters', 'txns', 'h24', 'min'),
        'max24HTxns': ('filters', 'txns', 'h24', 'max'),
        'min6HTxns': ('filters', 'txns', 'h6', 'min'),
        'max6HTxns': ('filters', 'txns', 'h6', 'max'),
        'min1HTxns': ('filters', 'txns', 'h1', 'min'),
        'max1HTxns': ('filters', 'txns', 'h1', 'max'),
        'min5MTxns': ('filters', 'txns', 'm5', 'min'),
        'max5MTxns': ('filters', 'txns', 'm5', 'max'),
        'min24HBuys': ('filters', 'buys', 'h24', 'min'),
        'max24HBuys': ('filters', 'buys', 'h24', 'max'),
        'min6HBuys': ('filters', 'buys', 'h6', 'min'),
        'max6HBuys': ('filters', 'buys', 'h6', 'max'),
        'min1HBuys': ('filters', 'buys', 'h1', 'min'),
        'max1HBuys': ('filters', 'buys', 'h1', 'max'),
        'min5MBuys': ('filters', 'buys', 'm5', 'min'),
        'max5MBuys': ('filters', 'buys', 'm5', 'max'),
        'min24HSells': ('filters', 'sells', 'h24', 'min'),
        'max24HSells': ('filters', 'sells', 'h24', 'max'),
        'min6HSells': ('filters', 'sells', 'h6', 'min'),
        'max6HSells': ('filters', 'sells', 'h6', 'max'),
        'min1HSells': ('filters', 'sells', 'h1', 'min'),
        'max1HSells': ('filters', 'sells', 'h1', 'max'),
        'min5MSells': ('filters', 'sells', 'm5', 'min'),
        'max5MSells': ('filters', 'sells', 'm5', 'max'),
        'min24HVol': ('filters', 'volume', 'h24', 'min'),
        'max24HVol': ('filters', 'volume', 'h24', 'max'),
        'min6HVol': ('filters', 'volume', 'h6', 'min'),
        'max6HVol': ('filters', 'volume', 'h6', 'max'),
        'min1HVol': ('filters', 'volume', 'h1', 'min'),
        'max1HVol': ('filters', 'volume', 'h1', 'max'),
        'min5MVol': ('filters', 'volume', 'm5', 'min'),
        'max5MVol': ('filters', 'volume', 'm5', 'max'),
        'min24HChg': ('filters', 'priceChange', 'h24', 'min'),
        'max24HChg': ('filters', 'priceChange', 'h24', 'max'),
        'min6HChg': ('filters', 'priceChange', 'h6', 'min'),
        'max6HChg': ('filters', 'priceChange', 'h6', 'max'),
        'min1HChg': ('filters', 'priceChange', 'h1', 'min'),
        'max1HChg': ('filters', 'priceChange', 'h1', 'max'),
        'min5MChg': ('filters', 'priceChange', 'm5', 'min'),
        'max5MChg': ('filters', 'priceChange', 'm5', 'max')
    }
    ws_params = {'rankBy': {'key': 'pairAge', 'order': query_params.get('order', ['asc'])[0]}, 'filters': {}}

    for http_param, ws_keys in conversion_map.items():
        value = query_params.get(http_param)
        if value:
            current_dict = ws_params
            for key in ws_keys[:-1]:
                if isinstance(key, int):
                    current_dict = current_dict.setdefault(ws_keys[-2], [None] * (key + 1))
                else:
                    current_dict = current_dict.setdefault(key, {})
            current_dict[ws_keys[-1]] = value[0]

    def build_query(prefix, item):
        if isinstance(item, dict):
            return "&".join(build_query(f"{prefix}[{k}]", v) for k, v in item.items())
        elif isinstance(item, list):
            return "&".join(
                f"{prefix}[{index}]={urllib.parse.quote(str(v), safe='/:')}" for index, v in enumerate(item) if
                v is not None)
        else:
            return f"{prefix}={urllib.parse.quote(str(item), safe='/:')}"

    ws_query = build_query('rankBy', ws_params['rankBy']) + '&' + build_query('filters', ws_params['filters'])
    return websocket_base + ws_query


# async def keep_connection_alive(websocket):
#     try:
#         while True:
#             await websocket.ping()
#             await asyncio.sleep(5)
#     except websockets.exceptions.ConnectionClosed:
#         logging.error("Connection closed while trying to send ping")
#         return

def generate_sec_websocket_key():
    random_bytes = os.urandom(16)
    key = base64.b64encode(random_bytes).decode('utf-8')
    return key

async def fetch_links():
    while True:
        await asyncio.sleep(1)
        redis = await get_redis()
        keys = await redis.keys('link:*')
        current_links = {}
        for key in keys:
            url = await redis.hget(key, 'url')
            if url is not None:
                link_data = await redis.hgetall(key)
                current_links[url] = link_data
        return current_links

async def process_link(application, link):
    removal_timestamps = {}
    redis = await get_redis()

    cooldown_seconds = 300
    try:
        while True:
            # proxies = {
            #     "https": f"http://{link['proxy']['username']}:{link['proxy']['password']}@{link['proxy']['host']}:{link['proxy']['port']}",
            #     "http": f"http://{link['proxy']['username']}:{link['proxy']['password']}@{link['proxy']['host']}:{link['proxy']['port']}"
            # }
            headers = {
                'Pragma': 'no-cache',
                'Origin': 'https://dexscreener.com',
                'Accept-Language': 'en-US,en;q=0.9,fr;q=0.8',
                'Sec-WebSocket-Key': f'{generate_sec_websocket_key()}',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
                'Upgrade': 'websocket',
                'Cache-Control': 'no-cache',
                'Connection': 'Upgrade',
                'Sec-WebSocket-Version': '13',
                'Sec-WebSocket-Extensions': 'permessage-deflate; client_max_window_bits'
            }

            try:
                url = http_to_websocket(link['url'])
                # print(f"going with url {url}")
                proxy = Proxy.from_url(f"http://{link['username']}:{link['password']}@{link['host']}:{link['port']}")
                link_fetch_task = asyncio.create_task(fetch_links())

                async with proxy_connect(url, extra_headers=headers, open_timeout=54,ping_interval=None, ping_timeout=None, proxy=proxy,) as websocket:
                    current_links = await link_fetch_task
                    logging.info(f"Scraping {link['title']}")
                    while True:
                        if link['url'] not in current_links:
                            logging.info(f"Link {link['title']} is no longer in the database, stopping task.")
                            break

                        message = await websocket.recv()
                        data = json.loads(message)
                        logging.info(f"Got data from {link['title']}")
                        print(data)
                        try:
                            if message == "ping":
                                continue

                            if data.get("pairs") if isinstance(data, dict) else []:
                                logging.info(f"Processing {link['title']}")
                                current_pairs = await parse(data)
                                logging.debug(current_pairs)

                                if not current_pairs:
                                    logging.info(f"No pairs found during parsing {link['title']}.")
                                    continue

                                stored_pairs_dict = await redis.hgetall(f"pairs:{link['url']}")
                                # print(stored_pairs_dict)
                                stored_pairs_json = stored_pairs_dict.get('pairs', '[]')
                                stored_pairs = json.loads(stored_pairs_json)
                                stored_pairs_keys = {convert_to_hashable(pair): pair for pair in stored_pairs}
                                current_pairs_keys = {convert_to_hashable(pair): pair for pair in current_pairs}
                                added_pairs_keys = set(current_pairs_keys.keys()) - set(stored_pairs_keys.keys())
                                removed_pairs_keys = set(stored_pairs_keys.keys()) - set(current_pairs_keys.keys())
                                await redis.hset(f"pairs:{link['url']}", 'pairs', json.dumps(current_pairs))
                                for key in removed_pairs_keys:
                                    logging.info(f"Pair removed: {stored_pairs_keys[key]} from {link['title']}")

                                for key in added_pairs_keys:
                                    current_time = time.time()
                                    if key in removal_timestamps and (
                                            current_time - removal_timestamps[key] < cooldown_seconds):
                                        logging.info(
                                            f"Skipping {current_pairs_keys[key]} due to cooldown {link['title']}.")
                                        continue

                                    logging.info(
                                        f"New pair added: {current_pairs_keys[key]['token_name']} to {link['title']}")
                                    pair = current_pairs_keys[key]
                                    pair_dict = dict(pair)
                                    token_pair_address = pair_dict['link'].split('/')[-1]
                                    if link['url'] not in current_links:
                                        logging.info(f"Link {link['url']} is no longer in the database, stopping task.")
                                        break

                                    message = (
                                        f"[{escape_markdown(link['title'], version=2)}]({link['url']}): "
                                        f"[{escape_markdown(pair_dict['token_name'], version=2)}](https://photon-sol.tinyastro.io/en/lp/{token_pair_address})\n"
                                        f"Age: {escape_markdown(str(format_age(pair_dict['age'])), version=2)}\n"
                                        f"Makers: {escape_markdown(str(pair_dict['makers']), version=2)}\n"
                                        f"Volume: {escape_markdown(str(pair_dict['volume']), version=2)}\n"
                                        f"FDV: {escape_markdown(str(pair_dict['fdv']), version=2)}\n"
                                        f"{escape_markdown(str(pair_dict['price_changes'][0]) + ' %', version=2)} \\| {escape_markdown(str(pair_dict['price_changes'][1]) + ' %', version=2)} \\| "
                                        f"{escape_markdown(str(pair_dict['price_changes'][2]) + ' %', version=2)} \\| {escape_markdown(str(pair_dict['price_changes'][3]) + ' %', version=2)}\n"
                                        f"{token_pair_address}"
                                    )
                                    await application.bot.send_message(chat_id=prod_chat, text=message,
                                                                       parse_mode='MarkdownV2',
                                                                       disable_web_page_preview=True)
                                    removal_timestamps[key] = current_time
                                if not added_pairs_keys and not removed_pairs_keys:
                                    logging.info(f"{link['title']} No new pairs added or removed.")

                        except Exception as e:
                            tb = traceback.format_exc()
                            logging.error(f"{link['title']} Unhandled error in process_link: {e}\nTraceback: {tb}")
                            pass
            except websockets.exceptions.ConnectionClosed as e:
                logging.warning(f"Connection closed for {link['title']}, will attempt to reconnect: {e}")
            except Exception as e:
                tb = traceback.format_exc()

                logging.error(f" {link['title']} Unhandled exception: {e} , {tb}")
    except asyncio.CancelledError:
        logging.error(f"{link['title']} Task was cancelled during execution")


async def MeatofTheWork(application):
    tasks = {}
    current_links = {}
    while True:
        redis = await get_redis()
        keys = await redis.keys('link:*')
        for key in keys:
            link_data = await redis.hgetall(key)
            if 'url' in link_data and 'title' in link_data:
                current_links[link_data['url']] = link_data
            else:
                logging.error(f"Link data incomplete for key {key}")

        for url, link in current_links.items():
            if 'host' in link and 'port' in link and 'username' in link and 'password' in link:
                if url not in tasks or tasks[url].done():
                    try:
                        tasks[url] = asyncio.create_task(process_link(application, link))
                        logging.info(f"Started task for {url}")
                    except Exception as e:
                        logging.error(f"Failed to start task for {url}: {e}")
            else:
                logging.debug(f"Skipping task for {url} due to stale cookies.")
        else:
            logging.debug(f"No valid cookies found for {url}, skipping task start.")
        await asyncio.sleep(1)

        for url in list(tasks.keys()):
            if url not in current_links:
                tasks[url].cancel()
                logging.info(f"Cancelling task for {url} as it has been removed from the database.")
                del tasks[url]


async def run_bot():
    test = "7192206326:AAFrOt5vXOS-_kIIHHsIv-ekGajhP73zFH4"
    prod = '7001922941:AAGgFLgcUup4hSrNWeQ3mrYofKnp72JPHeM'
    application = (Application.builder().token(prod).concurrent_updates(
        True).connect_timeout(
        90).read_timeout(90).write_timeout(90).build())
    application.add_handler(CommandHandler("add", add, block=False))
    application.add_handler(CommandHandler("delete", delete, block=False))
    application.add_handler(CommandHandler("list", list_links, block=False))
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    await MeatofTheWork(application)


if __name__ == "__main__":
    asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())
    setup_logging()
    asyncio.run(run_bot())
