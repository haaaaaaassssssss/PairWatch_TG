import asyncio
import logging
import time
from asyncio import WindowsSelectorEventLoopPolicy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from DrissionPage import ChromiumOptions
from DrissionPage import ChromiumPage
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
from telegram.ext import Application, CommandHandler
from telegram.helpers import escape_markdown
from tinydb import Query

from commands import add, delete, list_links
from extensions import ProxyAuthExtension
from resources import dblock, cookies_dblock, CookiesDB, PairsDB, link_db
from utils import setup_logging, convert_to_hashable


# os.environ["PYTHONASYNCIODEBUG"] = "1"


def getcookies_of_proxy(proxy_ip, proxy_port, proxy_user, proxy_password, return_expiry_only=False):
    Q = Query()
    result = CookiesDB.search(
        (Q.proxy.host == proxy_ip) &
        (Q.proxy.port == proxy_port) &
        (Q.proxy.username == proxy_user) &
        (Q.proxy.password == proxy_password)
    )

    if result:
        if return_expiry_only:
            return result[0]['last_refreshed']
        else:
            return result[0]['cookies']
    else:
        return None


async def parse(html):
    souped = BeautifulSoup(html, 'html.parser')
    rows = souped.find_all(class_="ds-dex-table-row ds-dex-table-row-new")
    pairs = []
    for row in rows:
        token_name = row.find(class_="ds-dex-table-row-base-token-name").text
        link = "https://dexscreener.com" + row.get('href')
        age = row.find(class_="ds-dex-table-row-col-pair-age").text.strip()
        makers = row.find_all(class_="ds-table-data-cell")[6].text.strip()
        volume = row.find_all(class_="ds-table-data-cell")[5].text.strip()
        fdv = row.find_all(class_="ds-table-data-cell")[-1].text.strip()
        price_changes = [change.text.strip() for change in row.find_all(class_="ds-dex-table-row-col-price-change")]
        pair = {
            "token_name": token_name,
            "link": link,
            "age": age,
            "makers": makers,
            "volume": volume,
            "fdv": fdv,
            "price_changes": price_changes
        }
        pairs.append(pair)
    return pairs


def blocking_browser_interaction():
    while True:
        links = link_db.all()
        current_time = datetime.now(timezone.utc).timestamp()

        for link_detail in links:
            link = link_detail.get('url')
            proxy_info = link_detail.get('proxy', {})
            cookies_info = getcookies_of_proxy(proxy_info['host'], proxy_info['port'],
                                               proxy_info['username'], proxy_info['password'], return_expiry_only=True)

            if cookies_info is None or (current_time - cookies_info > 1200):
                prx = (proxy_info['host'], proxy_info['port'], proxy_info['username'], proxy_info['password'],
                       proxy_info['dirname'])
                proxy = ProxyAuthExtension(*prx)
                options = ChromiumOptions().auto_port(True)
                options.set_argument('--no-sandbox')
                options.set_argument('--disable-crash-reporter')
                options.set_argument("--window-size=1920,1080")
                options.set_argument("--start-maximized")
                options.add_extension(proxy.directory)
                driver = ChromiumPage(options)
                try:
                    driver.timeout = 60
                    driver.get("http://nowsecure.nl/")
                    time.sleep(3)
                    driver.get(link)
                    result = driver.wait.eles_loaded("xpath:/html/body/div[1]/div/div[1]/div/div/iframe")
                    if result:
                        time.sleep(1)
                        iframe = driver.get_frame(1, timeout=60)
                        if iframe:
                            iframe.ele('xpath:/html/body/div/div/div[1]/div/label/input', timeout=60).click()
                            time.sleep(2)
                            driver.wait.ele_displayed("xpath:/html/body/div[1]/div/nav/div[1]", timeout=60)

                            cookies = driver.cookies(as_dict=True)
                            last_refreshed = datetime.now(timezone.utc).timestamp()
                            ProxyQuery = Query()
                            existing_entry = CookiesDB.search(ProxyQuery.proxy.host == proxy_info['host'])
                            if existing_entry:
                                CookiesDB.update({'cookies': cookies, 'last_refreshed': last_refreshed},
                                                 ProxyQuery.proxy.host == proxy_info['host'])
                                if driver:
                                    driver.quit()
                            else:
                                CookiesDB.insert(
                                    {'proxy': proxy_info, 'cookies': cookies, 'last_refreshed': last_refreshed})
                                if driver:
                                    driver.quit()
                except Exception as e:
                    print(f"Error during browsing: {e}")
                finally:
                    if driver:
                        driver.quit()
        time.sleep(3)


async def refreshCookies():
    await run_in_executor()


async def run_in_executor():
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as executor:
        await loop.run_in_executor(executor, blocking_browser_interaction)


async def process_link(application, link, PairsDB):
    removal_timestamps = {}
    max_retries = 6
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'max-age=0',
        'if-modified-since': 'Sun, 14 Apr 2024 17:15:39 GMT',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-arch': '"x86"',
        'sec-ch-ua-bitness': '"64"',
        'sec-ch-ua-full-version': '"123.0.6312.123"',
        'sec-ch-ua-full-version-list': '"Google Chrome";v="123.0.6312.123", "Not:A-Brand";v="8.0.0.0", "Chromium";v="123.0.6312.123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"12.0.0"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    }
    proxies = {
        "https": f"http://{link['proxy']['username']}:{link['proxy']['password']}@{link['proxy']['host']}:{link['proxy']['port']}",
        "http": f"http://{link['proxy']['username']}:{link['proxy']['password']}@{link['proxy']['host']}:{link['proxy']['port']}"
    }

    cooldown_seconds = 90  # Cooldown period in seconds

    try:
        while True:
            current_links = {link_details['url']: link_details for link_details in link_db.all()}

            if link['url'] not in current_links:
                logging.info(f"Link {link['url']} is no longer in the database, stopping task.")
                break

            async with cookies_dblock:
                cookies = getcookies_of_proxy(link['proxy']['host'], link['proxy']['port'], link['proxy']['username'],
                                              link['proxy']['password'])

            success = False
            for attempt in range(max_retries):
                try:
                    async with AsyncSession(impersonate="chrome") as s:
                        response = await s.get(link['url'], headers=headers, proxies=proxies, cookies=cookies)
                        if response.status_code == 200:
                            success = True
                            break
                        else:
                            logging.error(
                                f"Attempt {attempt + 1}: Failed to fetch data with status code {response.status_code}")
                except Exception as e:
                    logging.error(f"Attempt {attempt + 1}: Request failed with error {e}")

                await asyncio.sleep(1)

            if success:
                current_pairs = await parse(response.text)
                async with dblock:
                    q = Query()
                    stored_pairs_dict = PairsDB.get(q.url == link['url']) or {'pairs': []}
                    stored_pairs_keys = {convert_to_hashable(pair): pair for pair in stored_pairs_dict['pairs']}
                    current_pairs_keys = {convert_to_hashable(pair): pair for pair in current_pairs}

                    added_pairs_keys = set(current_pairs_keys.keys()) - set(stored_pairs_keys.keys())
                    removed_pairs_keys = set(stored_pairs_keys.keys()) - set(current_pairs_keys.keys())
                    PairsDB.upsert({'url': link['url'], 'pairs': list(current_pairs)}, q.url == link['url'])

                    current_time = time.time()
                    for key in removed_pairs_keys:
                        logging.info(f"Pair removed: {stored_pairs_keys[key]}")
                        removal_timestamps[key] = current_time

                    for key in added_pairs_keys:
                        if key in removal_timestamps:
                            time_since_removal = current_time - removal_timestamps[key]
                            if time_since_removal < cooldown_seconds:
                                continue

                        logging.info(f"New pair added: {current_pairs_keys[key]}")
                        pair = current_pairs_keys[key]
                        pair_dict = dict(pair)
                        token_pair_address = pair_dict['link'].split('/')[-1]
                        message = (
                            f"[{escape_markdown(link['title'], version=2)}]({link['url']}): "
                            f"[{escape_markdown(pair_dict['token_name'], version=2)}](https://photon-sol.tinyastro.io/en/lp/{token_pair_address})\n"
                            f"Age: {escape_markdown(pair_dict['age'], version=2)}\n"
                            f"Makers: {escape_markdown(pair_dict['makers'], version=2)}\n"
                            f"Volume: {escape_markdown(pair_dict['volume'], version=2)}\n"
                            f"FDV: {escape_markdown(pair_dict['fdv'], version=2)}\n"
                            f"{escape_markdown(pair_dict['price_changes'][0], version=2)} \\| {escape_markdown(pair_dict['price_changes'][1], version=2)} \\| "
                            f"{escape_markdown(pair_dict['price_changes'][2], version=2)} \\| {escape_markdown(pair_dict['price_changes'][3], version=2)}\n"
                            f"{token_pair_address}"
                        )
                        await application.bot.send_message(chat_id='@pairpeekbot', text=message,
                                                           parse_mode='MarkdownV2',
                                                           disable_web_page_preview=True)

            else:
                logging.error("All retry attempts failed. Moving to next task.")

            await asyncio.sleep(2)
    except asyncio.CancelledError:
        logging.error("Task was cancelled during execution")
    except Exception as e:
        logging.error(f"Unhandled error in process_link: {e}")


async def MeatofTheWork(application):
    refresh = asyncio.create_task(refreshCookies())
    tasks = {}
    while True:
        current_links = {link['url']: link for link in link_db.all()}
        for url, link in current_links.items():
            cookies_info = getcookies_of_proxy(link['proxy']['host'], link['proxy']['port'],
                                               link['proxy']['username'], link['proxy']['password'],
                                               return_expiry_only=True)
            if cookies_info:
                current_time = time.time()
                if current_time - cookies_info <= 1800:
                    if url not in tasks or tasks[url].done():
                        try:
                            tasks[url] = asyncio.create_task(process_link(application, link, PairsDB))
                            print(f"Started task for {url}")
                        except Exception as e:
                            print(f"Failed to start task for {url}: {e}")
                else:
                    print(f"Skipping task for {url} due to stale cookies.")

            else:
                print(f"No valid cookies found for {url}, skipping task start.")

        await asyncio.sleep(2)


async def run_bot():
    application = (Application.builder().token('7001922941:AAGgFLgcUup4hSrNWeQ3mrYofKnp72JPHeM').concurrent_updates(
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
