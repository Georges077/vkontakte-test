import asyncio

from beanie import init_beanie
import motor
import os

from ibex_models import Monitor, Platform, Account, SearchTerm, CollectAction
from datetime import datetime

from typing import Optional
from typing import List
from pydantic import Field, BaseModel, validator
from uuid import UUID, uuid4
from datetime import datetime
from ibex_models import Platform
from beanie.odm.operators.find.comparison import In

os.environ['MONGO_CS'] = 'mongodb+srv://[user]:[password]@ibexcluster.otmko.mongodb.net/ibex?retryWrites=true&w=majority'

class AccountReques(BaseModel):
    title: str
    platform: Platform
    platform_id: str

class PostMonitor(BaseModel):
    title: str
    descr: str
    date_from: datetime
    date_to: Optional[datetime]
    search_terms: List[str]
    accounts: List[AccountReques]
    platforms: Optional[List[Platform]]
    languages: Optional[List[str]]


class PostMonitorEdit(BaseModel):
    id: UUID
    date_from: Optional[datetime]
    date_to: Optional[datetime]
    search_terms: List[str]
    accounts: List[AccountReques]
    platforms: Optional[List[Platform]]
    languages: Optional[List[str]]


class SearchTermInit(BaseModel):
    id: UUID = Field(default_factory=uuid4, alias='_id')
    tags: List[str] = []
    term: str = ''


async def mongo(classes):
    mongodb_connection_string = os.getenv('MONGO_CS')
    client = motor.motor_asyncio.AsyncIOMotorClient(mongodb_connection_string)
    await init_beanie(database=client.ibex, document_models=classes)


async def create_monitor(postMonitor: PostMonitor) -> Monitor:
    await mongo([Monitor, Account, SearchTerm, CollectAction])

    monitor = Monitor(
        title=postMonitor.title,
        descr=postMonitor.descr,
        collect_actions = [],
        date_from=postMonitor.date_from,
        date_to=postMonitor.date_to
    )
    # print(monitor.id, type(monitor.id), type(str(monitor.id)))
    search_terms = [SearchTerm(
            term=search_term,
            tags=[str(monitor.id)]
        ) for search_term in postMonitor.search_terms]

    accounts = [Account(
            title=account.title,
            platform=account.platform,
            platform_id=account.platform_id,
            tags=[str(monitor.id)],
            url=''
        ) for account in postMonitor.accounts]

    platforms = postMonitor.platforms if postMonitor.platforms and len(postMonitor.platforms) else [account.platform for account in postMonitor.accounts]

    # Create single CollectAction per platform
    collect_actions = [CollectAction(
            monitor_id=monitor.id,
            platform=platform,
            search_term_tags = [str(monitor.id)],
            account_tags=[str(monitor.id)],
            tags=[],
        ) for platform in platforms]

    monitor.collect_actions = [collect_action.id for collect_action in collect_actions]

    if len(search_terms): await SearchTerm.insert_many(search_terms)
    if len(accounts): await Account.insert_many(accounts)
    await CollectAction.insert_many(collect_actions)
    await monitor.save()

    return monitor


async def edit_monitor(postMonitor: PostMonitorEdit) -> Monitor:
    # the method modifies the monitor in databes and related records
    await mongo([Monitor, Account, SearchTerm, CollectAction])
    monitor = await Monitor.get(postMonitor.id)

    # if date_from and date_to exists in postMonitor, it is updated
    if postMonitor.date_from: monitor.date_from = postMonitor.date_from
    if postMonitor.date_to: monitor.date_to = postMonitor.date_to

    # if search terms are passed, it needs to be compared to existing list and
    # and if changes are made, existing records needs to be modified
    # search_terms: List[str]
    search_result = await SearchTerm.find(In(SearchTerm.tags, [postMonitor.id])).to_list()
    for res in search_result:
        if res.term not in postMonitor.search_terms:
            while postMonitor.id in res.tags:
                res.tags.remove(postMonitor.id)
            st = SearchTerm.find(SearchTerm.id == res.id)
            await st.set({SearchTerm.tags: res.tags})
    for row in postMonitor.search_terms:
        if row not in search_result:
            term = SearchTerm.find(In(SearchTerm.term, [row]))
            for t in await term.to_list():
                if postMonitor.id not in t.tags:
                    t.tags.append(postMonitor.id)
                await t.set({SearchTerm.tags: t.tags})
            if not await term.to_list():
                search_term = SearchTerm(term=row, tags=[str(postMonitor.id)])
                await SearchTerm.insert_one(search_term)



    # if accounts are passed, it needs to be compared to existing list and 
    # and if changes are made, existing records needs to be modified
    # accounts: List[AccountReques]




    # if platforms are passed, it needs to be compared to existing list and 
    # and if changes are made, existing records needs to be modified
    # platforms: Optional[List[Platform]]

    # if languages are passed, it needs to be compared to existing list and 
    # and if changes are made, existing records needs to be modified
    # languages: Optional[List[str]]


post_mon_edit = PostMonitorEdit(
    id=UUID('b1936d71-542f-441f-b535-2344ab1a463d'),
    search_terms=["Ukraine", "Russia", "USA", "BBC","CNN", "Alia"],
    accounts=[AccountReques(
        title="Ukraine",
        platform="youtube",
        platform_id='flkjfogjfe2542342352'
    )]
)

asyncio.run(edit_monitor(post_mon_edit))
