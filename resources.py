import asyncio
from tinydb import TinyDB, Query

dblock = asyncio.Lock()
cookies_dblock = asyncio.Lock()
pairs_dblock = asyncio.Lock()
CookiesDB = TinyDB('cookies.json')
PairsDB = TinyDB('pairs.json')
link_db = TinyDB('links.json')
Link = Query()
