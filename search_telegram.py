import json
import os
import requests
from datetime import datetime
from telethon.sync import TelegramClient, events
from typing import List, Dict
from ibex.models.post import Post, Scores
from ibex.models.collect_task import CollectTask
from ibex.models.platform import Platform
from abc import ABC, abstractmethod


class TelegramCollector():
    """The abstract class for data collectors.

        All data collectors/data sources implement
        methods described below.
    """
    def __init__(self, *args, **kwargs):
        self.hash = os.getenv('TELEGRAM_HASH')
        self.id = 11607483
        self.groups = []

        # TODO: double check the limit per post
        self.max_posts_per_call = 100

    def generate_req_params(self, collect_task: CollectTask):
        params = dict(
            id=self.id,
            hash=self.hash,
            max_posts=self.max_posts_per_call,
            groups=self.groups,
        )

        if collect_task.query is not None and len(collect_task.query) > 0:
            params['searchTerm'] = collect_task.query
        if collect_task.accounts is not None and len(collect_task.accounts) > 0:
            params['accounts'] = ','.join([account.platform_id for account in collect_task.accounts])

        return params

    # @abstractmethod
    async def collect(self, collect_task: CollectTask) -> List[Post]:
        """The method is responsible for collecting posts
            from platforms.

        Args:
            collect_action(CollectTask): CollectTask object holds
                all the metadata needed for data collection.

        Returns:
            (List[Post]): List of collected posts.
        """
        params = self.generate_req_params(collect_task)
        client = TelegramClient('name', params['id'], params['hash'])
        await client.start()
        messages = await client.get_messages(params['searchTerm'][:7], limit=params['max_posts'])
        posts = self._map_to_posts(messages, params)

        return posts

    # @abstractmethod
    async def get_hits_count(self, collect_task: CollectTask) -> int:
        """The method is responsible for collecting the number of posts,
            that satisfy all criterias in CollectTask object.

        Note:
            Do not collect actual posts here, this method is only
            applicable to platforms that provide this kind of information.

        Args:
            collect_action(CollectTask): CollectTask object holds
                all the metadata needed for data collection.

        Returns:
            (int): Number of posts existing on the platform.
        """
        params = self.generate_req_params(collect_task)
        client = TelegramClient('name', params['id'], params['hash'])
        await client.start()
        messages = await client.get_messages(params['searchTerm'][:7], limit=params['max_posts'])
        hits_per_msg = []
        for msg in messages:
            hits_per_msg.append(msg.views)
        hits_count = sum(hits_per_msg)

        return hits_count

    # @abstractmethod
    @staticmethod
    def map_to_post(api_post: Dict, collect_task: CollectTask) -> Post:
        """The method is responsible for mapping data redudned by plarform api
            into Post class.

        Args:
            api_post: responce from platform API.
            collect_action(CollectTask): the metadata used for data collection task.

        Returns:
            (Post): class derived from API data.
        """
        scores = Scores(
            likes=api_post.replies,
            shares=api_post.forwards,
        )
        post_doc = ''
        try:
            post_doc = Post(title="post" if 'post' in str(api_post) else "",
                            text=api_post.message if 'message' in str(api_post) else "",
                            created_at=api_post.date if 'date' in str(api_post) else datetime.now(),
                            platform=Platform.telegram,
                            platform_id=api_post.from_id if api_post.from_id is not None else '',
                            author_platform_id=api_post.peer_id.channel_id if 'channel_id' in str(api_post) else None,
                            scores=scores,
                            api_dump=dict({"dump": str(api_post)}),
                            monitor_id=api_post.id,
                            # url=url,
                            )
        except Exception as exc:
            print(exc)
        return post_doc

    @abstractmethod
    def _map_to_posts(self, posts: List[Dict], collect_task: CollectTask):
        res: List[Post] = []
        for post in posts:
            try:
                post = self.map_to_post(post, collect_task)
                res.append(post)
            except ValueError as e:
                self.log.error(f'[{collect_task.platform}] {e}')
        return res

