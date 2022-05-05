from __future__ import annotations
import logging
import os
from typing import List, Dict

import requests
import pandas as pd
from datetime import datetime, timedelta

from ibex.models.account import Account
from ibex.models.collect_task import CollectTask
from ibex.models.platform import Platform
from ibex.models.post import Post, Scores
from ibex.models.media_status import MediaStatus
from ibex.models.search_term import SearchTerm
from ibex.models.monitor import Monitor


class YoutubeCollector:

    def __init__(self, *args, **kwargs):
        self.token = os.getenv('YOUTUBE_TOKEN')
        self.max_posts_per_call = 100 #TODO Exact max videos limit
        self.max_requests = 1 #20

        self.max_posts_per_call_sample = 50
        self.max_requests_sample = 1


    def generate_request_params(self, collect_task: CollectTask):
        self.max_requests_ = self.max_requests_sample if collect_task.sample else self.max_requests
        self.max_posts_per_call_ = self.max_posts_per_call_sample if collect_task.sample else self.max_posts_per_call

        if collect_task.accounts is not None and len(collect_task.accounts) > 1:
            self.log.error('[YouTube] Can not collect data from mode than one channel per call!')

        params = dict(
            part='snippet',
            maxResults=self.max_posts_per_call_,
            publishedAfter=f'{collect_task.date_from.isoformat()[:19]}Z',
            publishedBefore=f'{collect_task.date_to.isoformat()[:19]}Z',
            key=self.token,
            order='relevance',
            type='video'
        )

        if collect_task.query is not None and len(collect_task.query) > 0:
            params['q'] = collect_task.query

        if collect_task.accounts is not None and len(collect_task.accounts) == 1:
            params['channelId'] = collect_task.accounts[0].platform_id

        return params

    async def collect(self, collect_task: CollectTask):
        params = self.generate_request_params(collect_task)

        posts_from_api = self._collect(params)
        posts = self.map_to_posts(posts_from_api, collect_task)

        self.log.success(f'[YouTube] {len(posts)} posts collected')

        return posts


    async def get_hits_count(self, collect_task: CollectTask) -> int:
        params = self.generate_request_params(collect_task)

        res = self._youtube_search(params).json()

        hits_count = res['pageInfo']['totalResults']
        self.log.info(f'[YouTube] Hits count - {hits_count}')

        return hits_count


    def _collect(self, params):
        ids = self.collect_ids(params)
        collected_posts = self._get_video_details(ids)

        if len(collected_posts) == 0:
            self.log.warn('[YouTube] No data collected.')

        return collected_posts


    def collect_ids(self, params) -> List[str]:
        ids = []
        for i in range(self.max_requests_):
            res = self._youtube_search(params)
            res_dict = res.json()

            if 'items' not in res_dict or not len(res_dict['items']):
                self.log.error('[YouTube] no items!')
            else:
                for i in res_dict["items"]:
                    try:
                        ids.append(i["id"]["videoId"])
                    except Exception as ex:
                        self.log.error(f'[YouTube] {i} {str(ex)}')

            if "nextPageToken" not in res_dict:
                self.log.warn(f'[YouTube] nextPageToken not present in api response, breaking loop..')
                break

            params["pageToken"] = res_dict["nextPageToken"]

        return ids


    def _get_video_details(self, ids):
        results = []
        ids_chunks = [ids[i:i + self.max_posts_per_call_-1] for i in range(0, len(ids), self.max_posts_per_call_-1)]

        for ids_chunk in ids_chunks:
            params = dict(
                part='contentDetails,id,liveStreamingDetails,localizations,recordingDetails,snippet,statistics,status,topicDetails',
                id=','.join(ids_chunk),
                key=self.token,
            )

            res = self._youtube_details(params)
            results += res.json()["items"]

        return results

    @staticmethod
    # @sleep_after(tag='YouTube')
    def _youtube_search(params):
        res = requests.get("https://youtube.googleapis.com/youtube/v3/search",
            params=params)
        return res

    @staticmethod
    def _youtube_details(params):
        res = requests.get("https://www.googleapis.com/youtube/v3/videos", params=params)
        return res

    @staticmethod
    def map_to_post(api_post: Dict, collect_task: CollectTask) -> Post:
        # create scores class
        scores = None
        if 'statistics' in api_post:
            stats = api_post['statistics']
            likes = stats['likeCount'] if 'likeCount' in stats else None
            views = stats['viewCount'] if 'viewCount' in stats else None
            love = stats['favoriteCount'] if 'favoriteCount' in stats else None
            engagement = stats['commentCount'] if 'commentCount' in stats else None
            scores = Scores(likes=likes,
                            views=views,
                            love=love,
                            engagement=engagement)

        post_doc = Post(
            platform_id=api_post['id'],
            title=api_post['snippet']['title'],
            text=api_post['snippet']['description'],
            created_at=api_post['snippet']['publishedAt'],
            author_platform_id=api_post['snippet']['channelId'],
            image_url=api_post['snippet']['thumbnails']['default']['url'],
            url=f'https://www.youtube.com/watch?v={api_post["id"]}',
            platform=Platform.youtube,
            monitor_ids=[collect_task.monitor_id],
            api_dump=dict(**api_post),
            scores=scores,
            media_status=MediaStatus.to_be_downloaded
        )

        return post_doc

    def map_to_posts(self, posts: List[Dict], collect_task: CollectTask):
        res: List[Post] = []
        for post in posts:
            try:
                post = self.map_to_post(post, collect_task)
                res.append(post)
            except ValueError as e:
                self.log.error(f'[{collect_task.platform}] {e}')
        return res

    def generate_account_request_params(self, collect_task: CollectTask):
        if collect_task.accounts is not None and len(collect_task.accounts) > 1:
            self.log.error('[YouTube] Can not collect data from mode than one channel per call!')

        params = dict(
            part='snippet',
            maxResults=5,
            publishedAfter=f'{collect_task.date_from.isoformat()[:19]}Z',
            publishedBefore=f'{collect_task.date_to.isoformat()[:19]}Z',
            key=self.token,
            order='relevance',
            type='channel'
        )
        if collect_task.query is not None and len(collect_task.query) > 0:
            params['q'] = collect_task.query
        if collect_task.accounts is not None and len(collect_task.accounts) == 1:
            params['channelId'] = collect_task.accounts[0].platform_id
        return params

    async def get_accounts(self, collect_task: CollectTask) -> List[Account]:
        params = self.generate_account_request_params(collect_task)
        req_url = "https://www.googleapis.com/youtube/v3/search"
        res = requests.get(req_url, params)
        acc = res.json()['items']
        accounts = self.map_to_accounts(acc, collect_task)

        return accounts

    def map_to_accounts(self, accounts: List, collect_task: CollectTask) -> List[Account]:
        result: List[Account] = []
        for account in accounts:
            try:
                account = self.map_to_acc(account, collect_task)
                result.append(account)
            except ValueError as e:
                print({collect_task.platform}, e)
        return result

    def map_to_acc(self, acc: Account, collect_task: CollectTask) -> Account:
        mapped_account = Account(
            id=acc['id']['channelId'],
            title=acc['snippet']['channelTitle'],
            tags=[acc['etag']],
            img=acc['snippet']['thumbnails']['default']['url'],
            url='',
            platform=Platform.youtube,
            platform_id=acc['id']['channelId'],
            broadcasting_start_time=acc['snippet']['publishTime'],
        )
        return mapped_account

# async def test():
#     ibex_models.platform import Platform
#     from app.config.mongo_config import init_mongo
#     await init_mongo()
#     date_from = datetime.now() - timedelta(days=5)
#     date_to = datetime.now() - timedelta(days=1)
#     accounts = await Account.find(Account.platform == Platform.youtube).to_list()
#     yt = YoutubeCollector()
#     res = yt.collect_curated_single(date_from=date_from,
#                                     date_to=date_to,
#                                     account=accounts[0])
#     print(res)
#
#
# if __name__ == "__main__":
#     import asyncio
#     asyncio.run(test())
