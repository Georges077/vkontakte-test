from __future__ import annotations
from datetime import datetime
import requests
import pandas as pd
import os
from uuid import UUID

from typing import List, Dict

from ibex.models.account import Account
from ibex.models.search_term import SearchTerm
from ibex.models.post import Post, Scores
from ibex.models.platform import Platform
from ibex.models.collect_task import CollectTask


# from app.core.datasources.facebook.helper import split_to_chunks, needs_download
# from app.core.datasources.utils import update_hits_count

class FacebookCollector:
    def __init__(self, *args, **kwargs):
        self.token = os.getenv('CROWDTANGLE_TOKEN')

        # TODO: double check the limit per post
        self.max_posts_per_call = 100
        self.max_requests = 20

        self.max_posts_per_call_sample = 50
        self.max_requests_sample = 1

    @staticmethod
    def _collect_posts_by_param(params):
        res = requests.get("https://api.crowdtangle.com/posts", params=params).json()
        return res

    def generate_request_params(self, collect_task: CollectTask):
        self.max_requests_ = self.max_requests_sample if collect_task.sample else self.max_requests
        self.max_posts_per_call_ = self.max_posts_per_call_sample if collect_task.sample else self.max_posts_per_call

        params = dict(
            token=self.token,
            startDate=collect_task.date_from.isoformat(),
            endDate=collect_task.date_to.isoformat(),
            count=self.max_posts_per_call_
        )

        if collect_task.query is not None and len(collect_task.query) > 0:
            params['searchTerm'] = collect_task.query
        if collect_task.accounts is not None and len(collect_task.accounts) > 0:
            params['accounts'] = ','.join([account.platform_id for account in collect_task.accounts])

        return params

    async def collect(self, collect_task: CollectTask) -> List[Post]:
        params = self.generate_request_params(collect_task)

        results: List[any] = self._collect(params)
        posts = self._map_to_posts(results, params)

        return posts

    def _collect(self, params) -> List[Dict]:
        results = []
        res = {"result": {"pagination": {"nextPage": None}}}
        offset = 0

        while 'nextPage' in res["result"]["pagination"]:
            params["offset"] = self.max_posts_per_call_ * offset
            offset += 1
            res = self._collect_posts_by_param(params)

            if "result" not in res or "posts" not in res["result"]:
                self.log.warn(f'[Facebook] result not present in api response, breaking loop..')
                break

            results += res["result"]["posts"]

            if offset >= self.max_requests_:
                self.log.success(f'[Facebook] limit of {self.max_requests}')
                #  f' requests has been reached for params: {params}.')
                break

        if not len(results):
            self.log.warn('[Facebook] No data collected')
            return results

        self.log.success(f'[Facebook] {len(results)} posts collected')

        return results

    async def get_hits_count(self, collect_task: CollectTask) -> int:
        params = self.generate_request_params(collect_task)
        params['count'] = 0

        hits_count = requests.get("https://api.crowdtangle.com/posts/search", params=params).content

        self.log.info(f'[Facebook] Hits count - {hits_count}')

        return hits_count

    @staticmethod
    def map_to_post(api_post: Dict, collect_task: CollectTask) -> Post:
        # create scores class
        scores = None
        if 'statistics' in api_post and 'actual' in api_post['statistics']:
            actual_statistics = api_post['statistics']['actual']
            likes = actual_statistics['likeCount'] if 'likeCount' in actual_statistics else None
            shares = actual_statistics['shareCount'] if 'shareCount' in actual_statistics else None
            love_count = actual_statistics['loveCount'] if 'loveCount' in actual_statistics else None
            wow_count = actual_statistics['wowCount'] if 'wowCount' in actual_statistics else None
            sad_count = actual_statistics['sadCount'] if 'sadCount' in actual_statistics else None
            angry_count = actual_statistics['angryCount'] if 'angryCount' in actual_statistics else None
            engagement = actual_statistics['commentCount'] if 'commentCount' in actual_statistics else None
            scores = Scores(likes=likes,
                            shares=shares,
                            love=love_count,
                            wow=wow_count,
                            sad=sad_count,
                            angry=angry_count,
                            engagement=engagement)

        # create post class
        title = ""
        if 'title' in api_post:
            title = api_post['title']
        elif 'message' in api_post:
            title = api_post['message']

        url = api_post['postUrl'] if 'postUrl' in api_post.keys() else None

        post_doc = Post(title=api_post['message'] if 'message' in api_post else "",
                        text=api_post['description'] if 'description' in api_post else "",
                        created_at=api_post['date'] if 'date' in api_post else datetime.now(),
                        platform=Platform.facebook,
                        platform_id=api_post['platformId'],
                        author_platform_id=api_post['account']['id'] if 'account' in api_post else None,
                        scores=scores,
                        api_dump=api_post,
                        monitor_id=collect_task.monitor_id,
                        url=url
                        )
        return post_doc

    def _map_to_posts(self, posts: List[Dict], collect_task: CollectTask):
        res: List[Post] = []
        for post in posts:
            try:
                post = self.map_to_post(post, collect_task)
                res.append(post)
            except ValueError as e:
                self.log.error(f'[{collect_task.platform}] {e}')
        return res

    async def get_accounts(self, query) -> List[Account]:
        params = self.generate_acc_req_params(query)
        res = requests.get("https://graph.facebook.com/pages/search", params)
        acc = res.json()['items']
        accounts = self.map_to_accounts(acc, query)
        return accounts

    def generate_acc_req_params(self, query: str):
        params = dict(
            q=query,
            fields=['id', 'name', 'location', 'link'],
            access_token=self.token,
        )
        return params

    def map_to_accounts(self, accounts: List) -> List[Account]:
        result: List[Account] = []
        for account in accounts:
            try:
                account = self.map_to_acc(account)
                result.append(account)
            except ValueError as e:
                print('Facebook', e)
        return result

    def map_to_acc(self, acc: Account) -> Account:
        mapped_acc = Account(
            title=acc['name'],
            url=acc['link'],
            platform=Platform.facebook,
            platform_id=acc['id'],
        )
        return mapped_acc

# async def test():
#     ibex_models.platform import Platform
#     from app.config.mongo_config import init_mongo
#     await init_mongo()
#     date_from = datetime.now() - timedelta(days=5)
#     date_to = datetime.now() - timedelta(days=1)
#     accounts = await Account.find(Account.platform == Platform.facebook).to_list()
#     fb = FacebookCollector()
#     res = fb.collect_curated_batch(date_from=date_from.isoformat(),
#                                     date_to=date_to.isoformat(),
#                                     accounts=accounts)
#     print(res)
#
#
# if __name__ == "__main__":
#     import asyncio
#
#     asyncio.run(test())


# os('python3 sample.py monitor_id=128376-81723618-087186238712 sample=True')
