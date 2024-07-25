import json
import logging
import traceback

import redis.asyncio as aioredis
from telegram import Update
from telegram.ext import ContextTypes

WHITELISTED_USERS = ['belgxz', 'roidv']

with open('proxies.json', 'r') as f:
    proxies = json.load(f)


async def get_redis():
    redis = await aioredis.from_url("redis://localhost", decode_responses=True)
    return redis


async def add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        user_id = update.message.from_user.username
        if user_id not in WHITELISTED_USERS:
            await update.message.reply_text('You are not authorized to use this command.')
            logging.warning(f"Unauthorized access attempt by user: {user_id}")
            return

        redis = await get_redis()
        try:
            args_str = ' '.join(context.args)
            if args_str.startswith('"'):
                end_quote_index = args_str.find('"', 1)
                if end_quote_index == -1:
                    await update.message.reply_text('Invalid input. Make sure the title is enclosed in quotes.')
                    logging.warning(f"Invalid input format from user {user_id}: {args_str}")
                    return
                title = args_str[1:end_quote_index]
                url = args_str[end_quote_index + 2:].strip()
                if not url.startswith('http://') and not url.startswith('https://'):
                    await update.message.reply_text('Invalid URL. Make sure the URL is correct.')
                    logging.warning(f"Invalid URL from user {user_id}: {url}")
                    return
            else:
                await update.message.reply_text('Usage: /add "<title>" <link>')
                logging.info(f"Usage information sent to user {user_id}")
                return

            existing_keys = await redis.keys("link:*")
            duplicate_found = False
            for key in existing_keys:
                existing_title = await redis.hget(key, 'title')
                existing_url = await redis.hget(key, 'url')
                if title == existing_title and url == existing_url:
                    duplicate_found = True
                    break

            if duplicate_found:
                await update.message.reply_text(f'Link with title "{title}" and URL "{url}" already exists.')
                logging.info(f"Duplicate link attempt by user {user_id}: {title} - {url}")
                return

            available_proxies = [proxy for proxy in proxies['proxies'] if
                                 not await redis.hget(f"proxy:{proxy['host']}", "in_use")]

            if not available_proxies:
                await update.message.reply_text('No available proxies to assign.')
                logging.warning("No available proxies for new link")
                return

            selected_proxy = available_proxies[0]
            proxy_data = {
                'host': selected_proxy['host'],
                'port': selected_proxy['port'],
                'username': selected_proxy['username'],
                'password': selected_proxy['password'],
                'dirname': selected_proxy['dirname']
            }

            await redis.hset(f"link:{url}", mapping={
                'title': title,
                'url': url,
                **proxy_data
            })
            await redis.hset(f"proxy:{selected_proxy['host']}", "in_use", "true")
            await update.message.reply_text(f'Added link: "{title}" at {url} with proxy {selected_proxy["host"]}')
            logging.info(f"Link added by user {user_id}: {title} at {url} with proxy {selected_proxy['host']}")

        except Exception as e:
            await update.message.reply_text('An error occurred while processing your request. Please try again later.')
            logging.error(f"Error in add command for user {user_id}: {str(e)}\n{traceback.format_exc()}")
        finally:
            await redis.close()

    except Exception as e:
        await update.message.reply_text('An unexpected error occurred. Please try again later.')
        logging.error(f"Unexpected error in add command: {str(e)}\n{traceback.format_exc()}")


async def delete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.username
    if user_id not in WHITELISTED_USERS:
        await update.message.reply_text('You do not have permission to execute this command.')
        return

    if not context.args:
        await update.message.reply_text('Usage: /delete "<title>" — Ensure the title is enclosed in quotes.')
        return

    title = ' '.join(context.args).strip()
    redis = await get_redis()

    keys = await redis.keys("link:*")
    if not keys:
        await update.message.reply_text(f'No links with the title "{title}" found to delete.')
        await redis.close()
        return

    deleted_count = 0
    for key in keys:
        link_title = await redis.hget(key, 'title')
        if title == link_title:
            proxy_host = await redis.hget(key, 'host')
            if proxy_host:
                await redis.delete(f"proxy:{proxy_host}")

            await redis.delete(key)
            deleted_count += 1

    await redis.close()

    if deleted_count > 0:
        await update.message.reply_text(f'Successfully deleted {deleted_count} link(s) titled "{title}".')
        logging.info(f"Deleted {deleted_count} links titled: {title}")
    else:
        await update.message.reply_text(f'No links found with the exact title "{title}" to delete.')


async def list_links(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.username
    if user_id not in WHITELISTED_USERS:
        await update.message.reply_text('You are not authorized to use this command.')
        return

    redis = await get_redis()
    keys = await redis.keys('link:*')
    if not keys:
        await update.message.reply_text('No links found.')
        await redis.close()
        return

    links_info = []
    for key in keys:
        link_data = await redis.hgetall(key)
        if 'title' in link_data and 'url' in link_data:
            links_info.append(f"Title: {link_data['title']} - URL: {link_data['url']}")
        else:
            links_info.append(f"Data missing for {key}")

    message = 'Links:\n' + '\n'.join(links_info)
    await update.message.reply_text(message)
    await redis.close()
