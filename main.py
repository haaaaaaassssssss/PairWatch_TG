import asyncio
import logging
from datetime import datetime

import pytz
from DrissionPage import ChromiumOptions
from DrissionPage._pages.chromium_page import ChromiumPage
from bs4 import BeautifulSoup
from curl_cffi import requests
from telegram.ext import Application, CommandHandler
from telegram.helpers import escape_markdown
from tinydb import TinyDB, Query

import commands
from extensions import ProxyAuthExtension
from utils import setup_logging, convert_to_hashable
dblock = asyncio.Lock()
cookies_dblock = asyncio.Lock()
pairs_dblock = asyncio.Lock()
CookiesDB = TinyDB('cookies.json')
PairsDB = TinyDB('pairs.json')
link_db = TinyDB('links.json')
Link = Query()

def getcookies_of_proxy(proxy_ip, proxy_port, proxy_user, proxy_password):
    Q = Query()
    result = CookiesDB.search(
        (Q.proxy.host == proxy_ip) &
        (Q.proxy.port == proxy_port) &
        (Q.proxy.user == proxy_user) &
        (Q.proxy.password == proxy_password)
    )

    if result:
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


async def refreshCookies():
    links = link_db.all()
    for link_detail in links:
        link = link_detail.get('url')
        proxy_info = link_detail.get('proxy', {})
        max_retries = 5
        retries = 0
        driver = None
        while retries < max_retries:
            try:
                prx = (
                    proxy_info['host'], proxy_info['port'], proxy_info['username'], proxy_info['password'],
                    proxy_info['dirname'])
                proxy = ProxyAuthExtension(*prx)
                options = ChromiumOptions().auto_port(True)
                options.set_argument('--no-sandbox')
                options.set_argument('--disable-crash-reporter')
                options.set_argument("--window-size=1920,1080")
                options.set_argument("--start-maximized")
                options.add_extension(proxy.directory)
                driver = ChromiumPage(options)
                driver.timeout = 60
                driver.get("http://nowsecure.nl/")
                driver.wait.doc_loaded()
                await asyncio.sleep(3)
                driver.get(link)  # Use the actual link instead of hardcoded "https://dexscreener.com/"
                driver.wait.doc_loaded()
                result = driver.wait.eles_loaded("xpath:/html/body/div[1]/div/div[1]/div/div/iframe")

                if result:
                    logging.info("Page is loaded, proceeding")
                    await asyncio.sleep(1)
                    bFrame = driver.get_frame(1)
                    if bFrame:
                        iframe = driver.get_frame(1, timeout=60)
                        logging.info("Found the iframe, proceeding for click")
                        iframe.ele('xpath:/html/body/div/div/div[1]/div/label/input', timeout=60).click()
                        logging.info("Iframe clicked")
                        driver.wait.ele_displayed("xpath:/html/body/div[1]/div/nav/div[1]", timeout=60)
                        cookies = driver.cookies(as_dict=True)
                        timezone = pytz.timezone('UTC')
                        last_refreshed = datetime.now(timezone).timestamp()
                        async with cookies_dblock:
                            ProxyQuery = Query()
                            existing_entry = CookiesDB.search(ProxyQuery.proxy.host == proxy_info['host'])
                            if existing_entry:
                                CookiesDB.update({'cookies': cookies, 'last_refreshed': last_refreshed}, ProxyQuery.proxy.host == proxy_info['host'])
                            else:
                                CookiesDB.insert({'proxy': proxy_info, 'cookies': cookies, 'last_refreshed': last_refreshed})
                            logging.info(f"Cookies refreshed for proxy {proxy_info['host']} at directory {proxy_info['dirname']}")
                            logging.info(f"Saved/Updated cookies and proxy details in the database with timestamp: {last_refreshed}")
                if driver:
                    driver.quit()
                break
            except Exception as e:
                logging.error(f"Error encountered: {e}. Retrying...")
                if driver:
                    driver.quit(force=True)
                retries += 1
                await asyncio.sleep(1)


async def schedule_refresh_cookies(interval=1200):
    while True:
        await refreshCookies()
        await asyncio.sleep(interval)


async def process_link(application, link, PairsDB):
    while True:
        max_retries = 3
        headers = {
            'authority': 'dexscreener.com',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-language': 'en-US,en;q=0.9',
            'sec-ch-ua': '"(Not(A:Brand";v="8", "Chromium";v="101"',
            'sec-ch-ua-arch': '"x86"',
            'sec-ch-ua-bitness': '"64"',
            'sec-ch-ua-full-version': '"101.0.4937.0"',
            'sec-ch-ua-full-version-list': '"(Not(A:Brand";v="8.0.0.0", "Chromium";v="101.0.4937.0"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-model': '""',
            'sec-ch-ua-platform': '"Windows"',
            'sec-ch-ua-platform-version': '"15.0.0"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4937.0 Safari/537.36',
        }
        proxies = {
            "https": f"http://{link['proxy']['username']}:{link['proxy']['password']}@{link['proxy']['host']}:{link['proxy']['port']}",
            "http": f"http://{link['proxy']['username']}:{link['proxy']['password']}@{link['proxy']['host']}:{link['proxy']['port']}"
        }

        async with cookies_dblock:
            cookies = getcookies_of_proxy(link['proxy']['host'], link['proxy']['port'], link['proxy']['username'],
                                          link['proxy']['password'])

        success = False
        for attempt in range(max_retries):
            try:
                response = requests.get(link['url'], headers=headers, proxies=proxies, cookies=cookies)
                if response.status_code == 200:
                    success = True
                    break
                else:
                    logging.error(
                        f"Attempt {attempt + 1}: Failed to fetch data with status code {response.status_code}")
            except BaseException as e:
                logging.error(f"Attempt {attempt + 1}: Request failed with error {e}")

            await asyncio.sleep(2)

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

                for key in added_pairs_keys:
                    logging.info(f"New pair added: {current_pairs_keys[key]}")
                for key in removed_pairs_keys:
                    logging.info(f"Pair removed: {stored_pairs_keys[key]}")

            for key in added_pairs_keys:
                pair = current_pairs_keys[key]
                pair_dict = dict(pair)
                token_pair_address = pair_dict['link'].split('/')[-1]
                message = (
                    f"[{escape_markdown(link['title'], version=2)}](https://dexscreener.com/{token_pair_address}): "
                    f"[{escape_markdown(pair_dict['token_name'], version=2)}](https://photon-sol.tinyastro.io/en/lp/{token_pair_address})\n"
                    f"Age: {escape_markdown(pair_dict['age'], version=2)}\n"
                    f"Makers: {escape_markdown(pair_dict['makers'], version=2)}\n"
                    f"Volume: {escape_markdown(pair_dict['volume'], version=2)}\n"
                    f"FDV: {escape_markdown(pair_dict['fdv'], version=2)}\n"
                    f"{escape_markdown(pair_dict['price_changes'][0], version=2)} \\| {escape_markdown(pair_dict['price_changes'][1], version=2)} \\| "
                    f"{escape_markdown(pair_dict['price_changes'][2], version=2)} \\| {escape_markdown(pair_dict['price_changes'][3], version=2)}\n"
                    f"Token Pair Address: {token_pair_address}"
                )
                await application.bot.send_message(chat_id='@pairpeekbot', text=message, parse_mode='MarkdownV2')

        else:
            logging.error("All retry attempts failed. Moving to next task.")

        await asyncio.sleep(3)


async def MeatofTheWork(application):
    # Cold start here.
    # while True:
    #     print("a")
    #     await asyncio.sleep(1)
    # await refreshCookies()
    # refresh_task = asyncio.create_task(schedule_refresh_cookies(interval=1200))
    # tasks = [process_link(application, link, PairsDB) for link in config]
    # await asyncio.gather(*tasks)
    tasks = {}
    try:
        while True:
            current_links = {link['url']: link for link in link_db.all()}
            for url, link in current_links.items():
                if url not in tasks or tasks[url].done():
                    tasks[url] = asyncio.create_task(process_link(application, link, PairsDB))
                    print(f"Started task for {url}")

            for url in list(tasks.keys()):
                if url not in current_links:
                    tasks[url].cancel()
                    print(f"Cancelling task for {url} as it has gotten removed!")
                    del tasks[url]

            await asyncio.sleep(5)
    except asyncio.CancelledError:
        print("Manage links task cancelled")
    finally:
        for task in tasks.values():
            task.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)


async def run_bot():
    application = (Application.builder().token('7001922941:AAGgFLgcUup4hSrNWeQ3mrYofKnp72JPHeM').connect_timeout(
        90).read_timeout(90).write_timeout(90).build())
    application.add_handler(CommandHandler("add", commands.add))
    application.add_handler(CommandHandler("delete", commands.delete))
    application.add_handler(CommandHandler("list", commands.list_links))
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    await MeatofTheWork(application)


if __name__ == "__main__":
    setup_logging()
    asyncio.run(run_bot())
