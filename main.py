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

os.environ[
    'MONGO_CS'] = 'mongodb+srv://giorgi:8TIjM5usTviU7QRf@ibexcluster.otmko.mongodb.net/ibex?retryWrites=true&w=majority'


class AccountReques(BaseModel):
    id: Optional[UUID]
    title: str
    platform: Platform
    platform_id: str
    url: str


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


async def mongo(classes):
    mongodb_connection_string = os.getenv('MONGO_CS')
    client = motor.motor_asyncio.AsyncIOMotorClient(mongodb_connection_string)
    await init_beanie(database=client.ibex, document_models=classes)


async def create_monitor(postMonitor: PostMonitor) -> Monitor:
    await mongo([Monitor, Account, SearchTerm, CollectAction])

    monitor = Monitor(
        title=postMonitor.title,
        descr=postMonitor.descr,
        collect_actions=[],
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

    platforms = postMonitor.platforms if postMonitor.platforms and len(postMonitor.platforms) else [account.platform for
                                                                                                    account in
                                                                                                    postMonitor.accounts]

    # Create single CollectAction per platform
    collect_actions = [CollectAction(
        monitor_id=monitor.id,
        platform=platform,
        search_term_tags=[str(monitor.id)],
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

    await modify_monitor_search_terms(postMonitor)
    await modify_monitor_accounts(postMonitor)

    # if platforms are passed, it needs to be compared to existing list and
    # and if changes are made, existing records needs to be modified
    # platforms: Optional[List[Platform]]

    # if languages are passed, it needs to be compared to existing list and
    # and if changes are made, existing records needs to be modified
    # languages: Optional[List[str]]


async def modify_monitor_search_terms(postMonitor):
    # if search terms are passed, it needs to be compared to existing list and
    # and if changes are made, existing records needs to be modified
    # finding search terms in db which are no longer preseng in the post request
    db_search_terms: List[SearchTerm] = await SearchTerm.find(In(SearchTerm.tags, [postMonitor.id])).to_list()
    db_search_terms_to_to_remove_from_db: List[SearchTerm] = [search_term for search_term in db_search_terms if
                                                              search_term not in postMonitor.search_terms]

    for search_term in db_search_terms_to_to_remove_from_db:
        search_term.tags = [tag for tag in search_term.tags if tag != postMonitor.id]
        await search_term.save()

    # finding search terms that are not taged in db
    db_search_terms_strs: List[str] = [search_term.term for search_term in db_search_terms]
    search_terms_to_add_to_db: List[str] = [search_term for search_term in postMonitor.search_terms if
                                            search_term not in db_search_terms_strs]

    searchs_to_insert = []
    for search_term_str in search_terms_to_add_to_db:
        db_search_term = await SearchTerm.find(SearchTerm.term == search_term_str).to_list()
        if db_search_term[0]:
            # If same keyword exists in db, monitor.id is added to it's tags list
            db_search_term[0].tags.append(str(postMonitor.id))
            await db_search_term[0].save()
        else:
            # If keyword does not exists in db, new keyword is created
            searchs_to_insert.append(SearchTerm(term=search_term_str, tags=[str(postMonitor.id)]))
    if len(searchs_to_insert): await SearchTerm.insert_many(searchs_to_insert)


async def modify_monitor_accounts(postMonitor):
    # if accounts are passed, it needs to be compared to existing list and
    # and if changes are made, existing records needs to be modified
    # accounts: List[AccountReques]
    db_accounts: List[SearchTerm] = await Account.find(In(Account.tags, [postMonitor.id])).to_list()
    account_not_in_monitor = lambda db_account: len(
        [account for account in postMonitor.accounts if account == db_account.id]) == 0
    db_accounts_terms_to_to_remove_from_db: List[Account] = [account for account in db_accounts if
                                                             account_not_in_monitor(account)]

    for account in db_accounts_terms_to_to_remove_from_db:
        account.tags = [tag for tag in account.tags if tag != postMonitor.id]
        await account.save()

    accounts_to_insert = [Account(
        title=account.title,
        platform=account.platform,
        platform_id=account.platform_id,
        tags=[str(postMonitor.id)],
        url='') for account in postMonitor.accounts if not account.id]

    if len(accounts_to_insert): await Account.insert_many(accounts_to_insert)

post_mon_edit = PostMonitorEdit(
    id=UUID('b1936d71-542f-441f-b535-2344ab1a463d'),
    search_terms=["Ukraine", "Russia", "USA", "BBC", "CNN", "Alia"],
    accounts=[AccountReques(
        title="USA",
        platform="youtube",
        url="www.youtube.com",
        platform_id='flkjfogjfe2542342344'
    )]
)

asyncio.run(edit_monitor(post_mon_edit))