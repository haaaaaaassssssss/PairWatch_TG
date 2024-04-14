import json
from telegram import Update
from telegram.ext import ContextTypes
from tinydb import TinyDB, Query
from resources import dblock, link_db
from models import ValidateLink

Link = Query()


async def add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with dblock:
        args_str = ' '.join(context.args)
        if args_str.startswith('"'):
            end_quote_index = args_str.find('"', 1)
            if end_quote_index == -1:
                await update.message.reply_text('Invalid input. Make sure the title is enclosed in quotes.')
                return
            title = args_str[1:end_quote_index]
            url = args_str[end_quote_index + 1:].strip()
        else:
            await update.message.reply_text('Usage: /add "<title>" <link>')
            return

        try:
            link = ValidateLink(title=title, url=url)
            existing_link = link_db.search(Query().url == link.url)
            if existing_link:
                await update.message.reply_text(f'Link already exists at: {link.url}')
                return

            with open('proxies.json', 'r') as f:
                proxies = json.load(f)
            available_proxies = [proxy for proxy in proxies['proxies'] if not link_db.search(Link.proxy.host == proxy['host'])]

            if not available_proxies:
                await update.message.reply_text('No available proxies to assign.')
                return

            selected_proxy = available_proxies[0]
            link_db.insert({'title': link.title, 'url': link.url, 'proxy': selected_proxy})
            await update.message.reply_text(f'Added link: "{link.title}" at {link.url} with proxy {selected_proxy["host"]}')
        except Exception as e:
            await update.message.reply_text(str(e))


async def delete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with dblock:
        if context.args:
            title = ' '.join(context.args)

            found_links = link_db.search(Query().title == title)
            if found_links:
                link_db.remove(Query().title == title)
                await update.message.reply_text(f'Deleted links with title: "{title}"')
            else:
                await update.message.reply_text(f'No links found with title: "{title}"')
        else:
            await update.message.reply_text('Usage: /delete <title>')


async def list_links(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with dblock:
        links = link_db.all()
        message = 'Links:\n' + '\n'.join([f"{link.doc_id}: {link['title']} - {link['url']}" for link in links])
        await update.message.reply_text(message)
