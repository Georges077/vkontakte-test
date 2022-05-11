from ibex.models.account import Account
from ibex.search_telegram import TelegramCollector
from ibex.search_vkontakte import VKCollector
from ibex.search_youtube import YoutubeCollector
from ibex.search_facebook import FacebookCollector
from ibex.search_twitter import TwitterCollector
from typing import List
import asyncio

platform_classes = dict(
    youtube=YoutubeCollector(),
    vkontakte=VKCollector(),
    telegram=TelegramCollector(),
    facebook=FacebookCollector(),
    twitter=TwitterCollector(),
)


async def search_accounts(query):
    accounts: List[Account] = []
    get_acc = [accounts.append(await platform_classes[platform_class].get_accounts(query)) for platform_class in platform_classes]
    asyncio.wait(get_acc)
    return accounts

if __name__ == '__main__':
    result = asyncio.run(search_accounts('Ukraine AND Russia'))
    print("Accounts: ", result)
