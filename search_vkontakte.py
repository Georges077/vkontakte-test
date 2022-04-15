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
        self.max_posts_per_call = 10  # Variable for maximum number of posts per request
        self.max_requests = 20  # Variable for maximum number of requests

    def get_posts_by_params(self, params: Dict):
        # Get next page
        data = self.get_posts(params)  # Returned data for call of get_posts method.
        has_next = True  # Variable for terminating while loop if posts are not left.
        posts = []  # List for posts to collect all posts data.
        post_iterator = 0  # iterator for checking maximum call of requests.
        while has_next:
            if 'next_from' not in data.keys():
                has_next = False
                posts = posts + data['items']
                return posts
            else:
                post_iterator += 1
                data = self.get_posts(params, data['next_from'])
                posts = posts + data['items']
            if post_iterator > 20:
                break
        return posts

    def get_posts(self, params: Dict,  next_from=None):
        """ The method is responsible for actual get of data

        Args:
            params - Generated dictionary with all needed metadata
            next_from - Parameter for checking next portion of posts existence
        """
        if next_from is None:
            params['start_from'] = None
        else:
            params['start_from'] = next_from

        url = f"https://api.vk.com/method/newsfeed.search"  # Url string for request
        req = requests.get(url, params)  # Variable for data returned from request
        return req.json()['response']

    def generate_req_params(self, collect_task: CollectTask):
        """ The method is responsible for generating params

        Args:
            collect_action(CollectTask): CollectTask object holds
                all the metadata.
        """
        params = dict(
            access_token=self.token,
            count=self.max_posts_per_call,
            fields=['city', 'connections', 'counters', 'country', 'domain', 'exports', 'followers_count', 'has_photo', 'home_town', 'interests', 'is_no_index', 'first_name','last_name', 'deactivated', 'is_closed', 'military','nickname', 'personal', 'photo_50','relatives', 'schools','screen_name', 'sex', 'timezone', 'verified', 'wall_default', 'next_from'],
            start_time=int(collect_task.date_from.strftime('%s')),
            end_time=int(collect_task.date_to.strftime('%s')),
            v=5.81,
        )

        if collect_task.query is not None and len(collect_task.query) > 0:
            params['q'] = collect_task.query
        if collect_task.accounts is not None and len(collect_task.accounts) > 0:
            params['groups'] = ','.join([account.platform_id for account in collect_task.accounts])

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
        params = self.generate_req_params(collect_task)  # parameter for generated metadata
        results: List[any] = self.get_posts_by_params(params)  # list of posts returned by method
        posts = self._map_to_posts(results, params)  # list of posts with type of Post for every element

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
        params = self.generate_req_params(collect_task)  # parameter for generated metadata
        return self.get_posts(params)['total_count']

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
        post_doc = Post(title=api_post['title'] if 'title' in api_post else "",
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
        res: List[Post] = []  # Variable for collecting posts in one List with type of Post for each element.
        for post in posts:
            try:
                post = self.map_to_post(post, collect_task)  # Variable of type Post for adding post to posts list.
                res.append(post)
            except ValueError as e:
                self.log.error(f'[{collect_task.platform}] {e}')
        return res
