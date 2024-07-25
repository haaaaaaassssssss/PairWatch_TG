import asyncio
import base64
import json
import logging
import os
import sys
import traceback
import httpx
import websockets
from telegram.error import TelegramError
import time
import urllib
from asyncio import WindowsSelectorEventLoopPolicy
from datetime import datetime
from websockets_proxy import Proxy
from telegram.ext import Application, CommandHandler
from telegram.helpers import escape_markdown
from commands import add, delete, list_links, get_redis
from utils import setup_logging, convert_to_hashable
from urllib.parse import urlparse

chat_ids = ['@pairpeekbot', -1002249608086]
# chat_ids = ['@pairpeektest']
telegram_bot_logger = logging.getLogger('telegram')
telegram_bot_logger.setLevel(logging.WARNING + 1)
links_lock = asyncio.Lock()


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


def format_number(num):
    if num >= 1000000000:
        return str(num)
    elif num >= 1000000:
        return str(round(num / 1000000, 1)) + "m"
    elif num >= 1000:
        return str(round(num / 1000, 1)) + "k"
    else:
        return str(num)


async def AuthorityCheck(pair_dict):
    address = pair_dict['mint_address']
    print(f'Authority check address {address}')
    try:

        headers = {
            'accept': 'application/json',
        }
        async with httpx.AsyncClient() as client:

            response = await client.get(
                f'https://api.rugcheck.xyz/v1/tokens/{address.strip()}/report',
                headers=headers,
            )
            if response.status_code == 200:
                json = response.json()
                authority_status = {
                    "freezeAuthority": True if json["token"]["freezeAuthority"] else False,
                    "mintAuthority": True if json["token"]["mintAuthority"] else False,
                    "SpecialOwnership": True if ("Top 10 holders high ownership" in str(json) or
                                                 "Single holder ownership" in str(json) or
                                                 "High ownership" in str(json)) else False,
                    "LP": True if "Large amount of LP Unlocked" in str(json) else False,
                    "LinkCount": pair_dict.get("linkCount", False)
                }
                logging.info(authority_status)
                return authority_status
            else:
                logging.info(f"Authority check returned an odd response {response.status_code, response.text}")
                return None

    except Exception as e:
        logging.info(f"An error occurred: {e}")
        return None


async def parse(json_data):
    logging.info(json_data)
    try:
        pairs = json_data.get("pairs", [])
        parsed_pairs = []

        for pair in pairs:
            try:
                link_count = True if pair.get("profile", {}).get("linkCount", False) < 1 or None or "" else False
                base_token = pair.get("baseToken", {})
                symbol = base_token.get("symbol", "")
                mint_address = base_token.get("address", "")
                token_name = base_token.get("name", "")
                pair_address = pair.get("pairAddress", "")
                link = f"https://dexscreener.com/solana/{pair_address}"
                created_at = pair.get("pairCreatedAt", 0)
                age = datetime.utcfromtimestamp(created_at / 1000).strftime(
                    '%Y-%m-%d %H:%M:%S') if created_at else "N/A"
                makers = pair.get("makers", {}).get("h24", 0)
                volume = [format_number(pair.get("volume", {}).get(interval, 0)) for interval in
                          ['m5', 'h1', 'h6', 'h24']]
                fdv = pair.get("marketCap", 0)
                price_changes = [pair.get("priceChange", {}).get(interval, 0) for interval in ['m5', 'h1', 'h6', 'h24']]
                liquidity = pair.get("liquidity", 0).get("usd", 0)
                buys = [pair.get("buyers", {}).get(interval, 0) for interval in ['m5', 'h1', 'h6', 'h24']]
                sales = [pair.get("sellers", {}).get(interval, 0) for interval in ['m5', 'h1', 'h6', 'h24']]
                parsed_pair = {
                    "mint_address": mint_address,
                    "address": pair_address,
                    "token_name": token_name,
                    "link": link,
                    "age": age,
                    "makers": format_number(makers),
                    "volume": volume,
                    "liquidity": format_number(liquidity),
                    "fdv": format_number(fdv),
                    "buys_changes": buys,
                    "sales_changes": sales,
                    "price_changes": price_changes,
                    "symbol": symbol,
                    "linkCount": link_count,

                }
                logging.info(f"parsed pair {parsed_pair}")
                parsed_pairs.append(parsed_pair)
            except Exception as e:
                logging.info(f"Error processing pair: {e}")

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
            if http_param == 'chainIds':
                # Split the chainIds and enumerate for correct index mapping
                value = value[0].split(',')
                for i, val in enumerate(value):
                    current_dict = ws_params['filters'].setdefault(ws_keys[1], {})
                    current_dict[i] = val
            else:
                for key in ws_keys:
                    if key == ws_keys[-1]:
                        current_dict[key] = value[0]
                    else:
                        current_dict = current_dict.setdefault(key, {})

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


def generate_sec_websocket_key():
    random_bytes = os.urandom(16)
    key = base64.b64encode(random_bytes).decode('utf-8')
    return key


current_links = {}


async def fetch_links():
    while True:
        try:
            await asyncio.sleep(0.5)
            redis = await get_redis()
            keys = await redis.keys('link:*')
            new_links = {}
            if keys:
                for key in keys:
                    try:
                        link_data = await redis.hgetall(key)
                        if 'url' in link_data:
                            new_links[link_data['url']] = link_data
                    except Exception as e:
                        logging.error(f"Error processing key {key}: {str(e)}")
            else:
                continue

            try:
                async with links_lock:
                    if new_links != current_links:
                        current_links.clear()
                        current_links.update(new_links)
                        logging.info("Updated current_links with new data from Redis.")
            except Exception as e:
                logging.error(f"Error updating current_links: {str(e)}")

        except Exception as e:
            logging.error(f"Error in fetch_links: {str(e)}")
            await asyncio.sleep(5)


async def process_link(application, link):
    removal_timestamps = {}
    redis = await get_redis()
    cooldown_seconds = 3600
    while True:
        headers = {
            'Pragma': 'no-cache',
            'Origin': 'https://dexscreener.com',
            'Accept-Language': 'en-US,en;q=0.9',
            'Sec-WebSocket-Key': f'{generate_sec_websocket_key()}',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
            'Upgrade': 'websocket',
            'Cache-Control': 'no-cache',
            'Connection': 'Upgrade',
            'Sec-WebSocket-Version': '13',
            'Sec-WebSocket-Extensions': 'permessage-deflate; client_max_window_bits'
        }
        try:
            url = http_to_websocket(link['url'])

            proxy = Proxy.from_url(f"http://ecayyngo:h9e81ku5p0jj@179.61.172.89:6640")

            async with websockets.connect(url, extra_headers=headers, open_timeout=54, ping_interval=None,
                                          ping_timeout=None) as websocket:

                logging.info(f"Scraping {link['title']}")
                while True:
                    async with links_lock:
                        current_linksc = current_links.copy()
                        if link['url'] not in current_linksc:
                            logging.info(f"Link {link['title']} is no longer in the database, stopping task.")
                            logging.info("Raising error for task cancellation")
                            logging.info(f"During cancelling task the current links are {current_linksc}")
                            raise OverflowError

                    message = await websocket.recv()
                    data = json.loads(message)
                    # logging.info(f"Got data from {link['title']}")
                    #print(data)
                    if message == "ping":
                        continue

                    if data.get("pairs") if isinstance(data, dict) else []:
                        logging.info(f"Found pairs key for  {link['title']}")
                        current_pairs = await parse(data)
                        #logging.debug(current_pairs)

                        if not current_pairs:
                            logging.info(f"No pairs found during parsing {link['title']}.")
                            continue

                        stored_pairs_dict = await redis.hgetall(f"pairs:{link['url']}")
                        stored_pairs_json = stored_pairs_dict.get('pairs', '[]')
                        stored_pairs = json.loads(stored_pairs_json)
                        stored_pairs_keys = {convert_to_hashable(pair): pair for pair in stored_pairs}
                        current_pairs_keys = {convert_to_hashable(pair): pair for pair in current_pairs}
                        added_pairs_keys = frozenset(current_pairs_keys.keys()) - frozenset(stored_pairs_keys.keys())
                        removed_pairs_keys = frozenset(stored_pairs_keys.keys()) - frozenset(current_pairs_keys.keys())

                        await redis.hset(f"pairs:{link['url']}", 'pairs', json.dumps(current_pairs))

                        for key in removed_pairs_keys.copy():
                            logging.info(f"Pair removed: {stored_pairs_keys[key]} from {link['title']}")

                        for key in added_pairs_keys:
                            current_time = time.time()
                            redis_key = f"{key[1]}"
                            print(f"PRINTING KEY {redis_key}")
                            last_removed_time = await redis.hget(f"removal_timestamps:{link['url']}", redis_key)
                            await redis.hset(f"removal_timestamps:{str(link['url'])}", str(redis_key),
                                             str(current_time))
                            if last_removed_time and (current_time - float(last_removed_time) < cooldown_seconds):
                                logging.info(f"Skipping {current_pairs_keys[key]} due to cooldown {link['title']}.")
                                continue

                            logging.info(f"New pair added: {current_pairs_keys[key]['token_name']} to {link['title']}")
                            pair = current_pairs_keys[key]
                            pair_dict = dict(pair)
                            token_pair_address = pair_dict['link'].split('/')[-1]
                            async with links_lock:
                                current_linksc = current_links.copy()
                                if link['url'] not in current_linksc:
                                    logging.info(f"Link {link['url']} is no longer in the database, stopping task.")
                                    logging.info("Raising error for task cancellation")
                                    logging.info(f"During cancelling task the current links are {current_linksc}")

                                    raise OverflowError
                            authoritycheck = await AuthorityCheck(pair_dict)
                            authority_symbols = ''
                            if authoritycheck is not None:
                                authority_symbols = (
                                        ('❄️FA' if authoritycheck.get('freezeAuthority', False) else '') +
                                        ('♾️MA' if authoritycheck.get('mintAuthority', False) else '') +
                                        ('🔓LU' if authoritycheck.get('LP', False) else '') +
                                        ('⚠️SO' if authoritycheck.get('SpecialOwnership', False) else '') +
                                        ('0️⃣NU' if authoritycheck.get('LinkCount', False) else '')
                                )
                                if authority_symbols:
                                    authority_symbols = '⛔️⛔' + authority_symbols
                            # logging.info(f"DEBUG PAIR {pair}")
                            # logging.info(f"AUTHORITY DEBUG {authority_symbols}")
                            message = (
                                f"{authority_symbols} {escape_markdown(pair_dict['token_name'], version=2)} \\| {escape_markdown(pair_dict['symbol'], version=2)}\n\n"
                                f"⏱️ Age: {escape_markdown(format_age(pair_dict['age']), version=2)}\n"
                                f"💹 Market cap: {escape_markdown(pair_dict['fdv'], version=2)}\n"
                                f"📊 Volume: {escape_markdown(str(pair_dict['volume'][0]), version=2)}\n"
                                f"💧 Liquidity: {escape_markdown(str(pair_dict['liquidity']), version=2)}\n"
                                f"🧮 Change: {escape_markdown(str(pair_dict['price_changes'][0]) + '%', version=2)}\n"
                                f"💻 Traders: {escape_markdown(str(pair_dict.get('makers', 'N/A')), version=2)}\n\n"
                                f"[Photon](https://photon\\-sol\\.tinyastro\\.io/en/lp/{token_pair_address}) \\| "
                                f"[BirdEye](https://birdeye\\.so/token/So11111111111111111111111111111111111111112/{pair_dict['mint_address']}?chain=solana) \\| "
                                f"[DexS](https://dexscreener\\.com/solana/{token_pair_address}) \\| "
                                f"[Solscan](https://solscan\\.io/token/{pair_dict['mint_address']}) \\| "
                                f"[Twitter](https://twitter\\.com/search?q={escape_markdown(pair_dict['mint_address'], version=2)}&f=live)\n\n"
                                f"`{token_pair_address}`\\|"
                                f"[{link['title']}]\n"
                            )

                            for chat_id in chat_ids:
                                try:
                                    await application.bot.send_message(chat_id=chat_id, text=message,
                                                                       parse_mode='MarkdownV2',
                                                                       disable_web_page_preview=True)
                                except TelegramError as e:
                                    logging.error(f"Failed to send message to chat ID {chat_id}: {e}")
                            print(f"PRINTING KEY {key}")

                            await redis.hset(f"removal_timestamps:{link['url']}", str(key[1]), str(current_time))

                        if not added_pairs_keys and not removed_pairs_keys:
                            logging.info(f"{link['title']} No new pairs added or removed.")
                    logging.info(f"No Pairs Data {link['title']} but instead we got {data}")

        except websockets.exceptions.ConnectionClosed as e:
            logging.warning(f"Connection closed for {link['title']}, will attempt to reconnect: {e}")
            logging.warning(message)
            continue


        except RuntimeError as e:
            if "set changed size during iteration" in str(e):
                logging.error("Caught 'set changed size during iteration'. Restarting process_link.")
                continue
            else:
                logging.error("Unhandled runtime error: {}".format(e))
                break


        except asyncio.CancelledError:
            logging.warning(message)
            tb = traceback.format_exc()
            logging.error(f"{link['title']} Task was cancelled during execution with traceback {tb}")
            exc_type, exc_value, exc_traceback = sys.exc_info()
            logging.error(f"Exception type: {exc_type.__name__}, Value: {exc_value}")
            for frame, line in traceback.walk_tb(exc_traceback):
                filename = frame.f_code.co_filename
                funcname = frame.f_code.co_name
                locals_at_frame = frame.f_locals
                logging.error(f"Error at file {filename}, line {line}, in function {funcname}.")
                logging.error(f"Local variables at this step: {locals_at_frame}")

        except OverflowError:
            logging.info(f"Overflow error called for {link['title']} stopping task!")
            logging.warning(message)
            return


async def MeatofTheWork(application):
    tasks = {}
    fetch_task = asyncio.create_task(fetch_links())
    global current_links

    while True:
        try:
            redis = await get_redis()
            keys = await redis.keys('link:*')

            if keys:
                for key in keys:
                    try:
                        link_data = await redis.hgetall(key)
                        if 'url' in link_data and 'title' in link_data:
                            async with links_lock:
                                current_links[link_data['url']] = link_data
                        else:
                            logging.error(f"Link data incomplete for key {key}. Data: {link_data}")
                    except Exception as e:
                        logging.error(f"Error processing key {key}: {str(e)}\n{traceback.format_exc()}")
            else:
                logging.debug("No keys found in Redis. Sleeping for 1 second.")
                await asyncio.sleep(1)
                continue

            try:
                async with links_lock:
                    current_links_snapshot = current_links.copy()

                    for url, link in current_links.items():
                        if all(k in link for k in ['host', 'port', 'username', 'password']):
                            if url not in tasks or tasks[url].done():
                                try:
                                    tasks[url] = asyncio.create_task(process_link(application, link))
                                    logging.info(f"Started task for {url}")
                                except Exception as e:
                                    logging.error(f"Failed to start task for {url}: {str(e)}\n{traceback.format_exc()}")
                        else:
                            logging.debug(f"Skipping task for {url} due to missing required fields. Link data: {link}")
            except Exception as e:
                logging.error(f"Error processing links: {str(e)}\n{traceback.format_exc()}")

            await asyncio.sleep(1)

            try:
                async with links_lock:
                    for url in list(tasks.keys()):
                        if url not in current_links:
                            try:
                                tasks[url].cancel()
                                logging.info(f"Cancelling task for {url} as it has been removed from the database.")
                                del tasks[url]
                            except Exception as e:
                                logging.error(f"Error cancelling task for {url}: {str(e)}\n{traceback.format_exc()}")
            except Exception as e:
                logging.error(f"Error cleaning up tasks: {str(e)}\n{traceback.format_exc()}")

        except Exception as e:
            logging.error(f"Unexpected error in MeatofTheWork: {str(e)}\n{traceback.format_exc()}")
            await asyncio.sleep(5)


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
