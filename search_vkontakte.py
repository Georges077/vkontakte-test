import os
import requests
import vk_api
from typing import List, Dict
from ibex.models.post import Post, Scores
from ibex.models.collect_task import CollectTask
from ibex.models.platform import Platform
from datetime import datetime
from abc import ABC, abstractmethod


class VKCollector(ABC):
    """The abstract class for data collectors.

        All data collectors/data sources implement
        methods described below.
    """

    def __init__(self, *args, **kwargs):
        self.token = os.getenv('VK_TOKEN')
        # TODO: double check the limit per post
        self.max_posts_per_call = 1
        self.max_requests = 20
        self.groups = []

    def get_posts_by_params(self, params):
        url = f"https://api.vk.com/method/newsfeed.search?q={params['searchTerm']}&count={params['max_posts']}&access_token={params['token']}&v=5.81"
        req = requests.get(url)
        return req.json()['response']['items']

    def generate_req_params(self, collect_task: CollectTask):
        params = dict(
            token=self.token,
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
        results: List[any] = self.get_posts_by_params(params)
        posts = self._map_to_posts(results, params)

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
        pass

    # @abstractmethod
    # @staticmethod
    def map_to_post(self, api_post: Dict, collect_task: CollectTask) -> Post:
        """The method is responsible for mapping data redudned by plarform api
            into Post class.

        Args:
            api_post: responce from platform API.
            collect_action(CollectTask): the metadata used for data collection task.

        Returns:
            (Post): class derived from API data.
        """
        scores = Scores(
            likes=api_post['likes']['count'],
            shares=api_post['reposts']['count'],
        )
        post_doc = Post(title=api_post['post_type'] if 'post_type' in api_post else "",
                        text=api_post['text'] if 'text' in api_post else "",
                        created_at=api_post['date'] if 'date' in api_post else datetime.now(),
                        platform=Platform.vkontakte,
                        platform_id=api_post['from_id'],
                        author_platform_id=api_post['owner_id'] if 'owner_id' in api_post else None,
                        scores=scores,
                        api_dump=api_post,
                        monitor_id=api_post['id'],
                        # url=url,
                        )
        return post_doc

    # @abstractmethod
    def _map_to_posts(self, posts: List[Dict], collect_task: CollectTask):
        res: List[Post] = []
        for post in posts:
            try:
                post = self.map_to_post(post, collect_task)
                res.append(post)
            except ValueError as e:
                self.log.error(f'[{collect_task.platform}] {e}')
        return res
