import asyncio
import os
import requests
from eldar import Query
from typing import List, Dict
from ibex.models.post import Post, Scores
from ibex.models.collect_task import CollectTask
from ibex.models.platform import Platform
from ibex.models.account import Account
from datetime import datetime
from ibex.split import split_complex_query
from ibex.datasource import Datasource
import vk_api


class VKCollector(Datasource):
    """The class for data collection from VKontakte.

        All data collectors/data sources implement
        methods described below.
    """

    def __init__(self, *args, **kwargs):
        self.token = os.getenv('VK_TOKEN')
        # TODO: double check the limit per post

        # Variable for maximum number of posts per request
        self.max_posts_per_call = 50
        self.max_posts_per_call_sample = 20

        # Variable for maximum number of requests
        self.max_requests = 50
        self.max_requests_sample = 50
        self.operators = dict(or_=' OR ', and_=' AND ', not_=' NOT ')

    async def get_keyword_with_least_posts(self, collect_task: CollectTask) -> str:
        # split_complex_query splits query into words and statements
        # keyword_1,           keyword_2,          keyword_3,      keyword_4
        #        statement_1,         statement_2,       statement_3
        keywords, statements = split_complex_query(collect_task.query, self.operators)

        tmp_query = collect_task.query
        keywords_with_hits_counts = []
        for i, keyword in enumerate(keywords):
            if statements[i - 1] == '_NOT': continue
            collect_task.query = keyword
            hits_count = await self.get_hits_count(collect_task)
            keywords_with_hits_counts.append((keyword, hits_count))

        keywords_with_hits_counts.sort(key=lambda tup: tup[1])

        keyword_with_least_posts = keywords_with_hits_counts[0]
        collect_task.query = tmp_query
        return keyword_with_least_posts[0]

    def get_posts_by_params(self, params: Dict, init_query: str, last_query: str):

        params['q'] = init_query
        # Returned data for call of get_posts method.
        data = self.get_posts(params)

        # Variable for terminating while loop if posts are not left.
        has_next = True

        # List for posts to collect all posts data.
        posts = []

        # iterator for checking maximum call of requests.
        post_iterator = 0
        eldar = Query(last_query)
        while has_next:
            posts += [d for d in data['items'] if len(eldar.filter([d['text']])) > 0]
            post_iterator += 1

            if 'next_from' not in data.keys():
                print(f'[VKontakte] all end of a list been reached')
                break

            if post_iterator > self.max_requests_:
                print(f'[VKontakte] limit of {self.max_requests_} have been reached')
                break

            data = self.get_posts(params, data['next_from'])

        return posts

    def get_posts(self, params: Dict, next_from=None):
        """ The method is responsible for actual get of data
        Args:
            params - Generated dictionary with all needed metadata
            next_from - Parameter for checking next portion of posts existence
        """
        if next_from is None:
            params['start_from'] = None
        else:
            params['start_from'] = next_from

        # Url string for request
        url = f"https://api.vk.com/method/newsfeed.search"

        # Variable for data returned from request
        req = requests.get(url, params)
        if 'response' not in str(req.json()):
            self.generate_token()
            params['access_token'] = self.token
            req = requests.get(url, params)
        return req.json()['response']

    def generate_token(self):
        vk_session = vk_api.VkApi('+995595912192', 'gio51319702')
        vk_session.auth()
        self.token = vk_session.token['access_token']

    def generate_req_params(self, collect_task: CollectTask):
        """ The method is responsible for generating params
        Args:
            collect_action(CollectTask): CollectTask object holds
                all the metadata.
        """
        params = dict(
            access_token=self.token,
            count=self.max_posts_per_call_,
            fields=['city', 'connections', 'counters', 'country', 'domain', 'exports', 'followers_count', 'has_photo',
                    'home_town', 'interests', 'is_no_index', 'first_name', 'last_name', 'deactivated', 'is_closed',
                    'military', 'nickname', 'personal', 'photo_50', 'relatives', 'schools', 'screen_name', 'sex',
                    'timezone', 'verified', 'wall_default', 'next_from'],
            start_time=int(collect_task.date_from.strftime('%s')),
            end_time=int(collect_task.date_to.strftime('%s')),
            v=5.81,
        )

        if collect_task.query is not None and len(collect_task.query) > 0:
            params['q'] = collect_task.query
        if collect_task.accounts is not None and len(collect_task.accounts) > 0:
            params['groups'] = ','.join([account.platform_id for account in collect_task.accounts])
        return params

    async def collect(self, collect_task: CollectTask) -> List[Post]:
        """The method is responsible for collecting posts
            from platforms.
        Args:
            collect_action(CollectTask): CollectTask object holds
                all the metadata needed for data collection.
        Returns:
            (List[Post]): List of collected posts.
        """
        self.max_requests_ = self.max_requests_sample if collect_task.sample else self.max_requests
        self.max_posts_per_call_ = self.max_posts_per_call_sample if collect_task.sample else self.max_posts_per_call

        # parameter for generated metadata
        params = self.generate_req_params(collect_task)

        # list of posts returned by method
        init_query = ' '
        if ' OR ' not in collect_task.query:
            init_query = await self.get_keyword_with_least_posts(collect_task)
        results: List[any] = self.get_posts_by_params(params, init_query, collect_task.query)

        # list of posts with type of Post for every element
        posts = self.map_to_posts(results, params)
        return posts


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
        params = self.generate_req_params(collect_task)  # parameters for generated metadata
        return self.get_posts(params)['total_count']

    def map_to_post(self, api_post: Dict, collect_task: CollectTask) -> Post:
        """The method is responsible for mapping data redudned by plarform api
            into Post class.
        Args:
            api_post: responce from platform API.
            collect_action(CollectTask): the metadata used for data collection task.
        Returns:
            (Post): class derived from API data.
        """
        # print(api_post)
        scores = Scores(
            likes=0 if not 'likes' in api_post else api_post['likes']['count'],
            shares=0 if not 'reposts' in api_post else api_post['reposts']['count'],
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

    def map_to_posts(self, posts: List[Dict], collect_task: CollectTask):

        # Variable for collecting posts in one List with type of Post for each element.
        res: List[Post] = []
        for post in posts:
            try:
                # Variable of type Post for adding post to posts list.
                post = self.map_to_post(post, collect_task)
                res.append(post)
            except ValueError as e:
                self.log.error(f'[{collect_task.platform}] {e}')
        return res

    def get_acc(self, params: Dict):
        """ The method is responsible for actual get of data
        Args:
            params - Generated dictionary with all needed metadata.
        """

        # Url string for request
        url = f"https://api.vk.com/method/search.getHints"

        # Variable for data returned from request
        req = requests.get(url, params)
        return req.json()['response']['items']

    # @abstractmethod
    async def get_accounts(self, query) -> List[Account]:
        """The method is responsible for collecting Accounts
              from platforms.
          Args:
              collect_action(CollectTask): CollectTask object holds
                  all the metadata needed for data collection.
          Returns:
              (List[Account]): List of collected accounts.
          """
        # parameter for generated metadata
        params = dict(
            access_token=self.token,
            limit=5,
            fields=[''],
            v=5.82,
            q=query
        )

        # list of posts returned by method
        results: List[any] = self.get_acc(params)

        # list of accounts with type of Account for every element
        accounts = self.map_to_accounts(results, params)
        return accounts

    def map_to_accounts(self, accounts: List, collect_task: CollectTask) -> List[Account]:
        """The method is responsible for mapping data redudned by plarform api
                   into Account class.
               Args:
                   accounts: responce from platform API.
                   collect_action(CollectTask): the metadata used for data collection task.
               Returns:
                   (Account): class derived from API data.
               """
        result: List[Account] = []
        for account in accounts:
            try:
                account = self.map_to_acc(account, collect_task)
                result.append(account)
            except ValueError as e:
                print({collect_task.platform}, e)
        return result

    def map_to_acc(self, acc: Account) -> Account:
        group_id = ''
        group_name = ''
        group_photo = ''
        group_url = ''
        if 'group' in str(acc):
            group_id = acc['group']['id']
            group_photo = acc['group']['photo_100']
            group_name = acc['group']['name']
            group_url = acc['group']['screen_name']
        if 'profile' in str(acc):
            group_id = acc['profile']['id']
            group_photo = acc['profile']['first_name']
            group_name = ''
            group_url = ''
        mapped_account = Account(
            title=group_name,
            url='vk.com/' + group_url,
            platform=Platform.vkontakte,
            platform_id=group_id,
            img=group_photo
        )
        return mapped_account