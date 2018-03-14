#! /usr/bin/env python3

import asyncio
import time
import random

from . import debug
from . import asset
from . import records
from . import dispose
from . import source
from .configure import config
from .asset import arranger

log = config.log;

def testLock():
    print('testLock');
    config.nFetchLimit = 4;
    config.nLimitPerHost = 2;
    loop = asyncio.get_event_loop();
    lock = asset.PerHostLock();
    async def run(sUrl=None):
        print(time.asctime(), 'prepare: {}'.format(sUrl));
        await lock.acquire(sUrl);
        print(time.asctime(), 'begin: {}'.format(sUrl));
        await asyncio.sleep(3+random.random()*3);
        lock.release(sUrl);
        print(time.asctime(), 'end: {}'.format(sUrl));
    aTasks= [];
    for x in range(3):
        aTasks.append(arranger.task(run('//bai.cc/{}'.format(x))));
    for x in range(1):
        aTasks.append(arranger.task(run()));
    for x in range(4):
        s1 = chr(ord('a')+x);
        aTasks.append(arranger.task(run('//{0}.{0}/{1}'.format(s1, x))));
    loop.run_until_complete(asyncio.gather(*aTasks));
    print('testLock tested');

async def testSource():
    print('testSource');
    aUserUrls = [
        'https://weibo.com/n/马伯庸',
        'https://weibo.com/u/1444865141',
        'https://m.weibo.cn/u/1811696373'
    ];
    aPostUrls = [
        'https://weibo.com/1444865141/G2MA2rq4A?from=page_1005051444865141_profile&wvr=6&mod=weibotime',
        'https://m.weibo.cn/status/G3rdV10vb',
        'https://m.weibo.cn/status/4180192106660800'
    ];
    sou = source.WeiboSource();
    print(sou.UA);

    user = await sou.getUser(aUserUrls[0]);
    print()
    print(vars(user));
    await sou.getUser(user=user, isFull=True);
    print()
    print(vars(user));
    user = await sou.getUser(aUserUrls[1], isFull=True);
    print()
    print(vars(user));
    user = records.WeiboUser(sUrl=aUserUrls[2]);
    await sou.getUser(user=user, isFull=True);
    print()
    print(vars(user));
    user = records.WeiboUser(sId=aUserUrls[2].split('/')[-1]);
    await sou.getUser(user=user, isFull=True);
    print()
    print(vars(user));

    post = await sou.getPost(aPostUrls[0]);
    print()
    print(vars(post));
    await sou.getPost(aPostUrls[1], isWithComment=True);
    print(vars(post));
    post = records.WeiboPost(sUrl=aPostUrls[2]);
    await sou.getPost(post=post, isWithComment=True);
    print(vars(post));

    from pprint import pprint

    user = await sou.getUser(aUserUrls[0]);
    aWeibo1 = await sou.getWeibo(user=user, nPage=1, nPageCount=1, isFull=True);
    pprint(aWeibo1);

    aWeibo2 = await sou.getWeibo(sCid=user.sWeiboCid, nPage=3, nPageCount=2, isFull=False);
    pprint(aWeibo2);

    aArticleUrls = [
        'https://media.weibo.cn/article?id=2309404214301027136108&jumpfrom=weibocom&sudaref=www.v2ex.com&display=0&retcode=6102',
        'https://weibo.com/ttarticle/p/show?id=2309404214301027136108'
    ]
    article = await sou.getArticle(sUrl=aArticleUrls[0]);
    pprint(vars(article));
    article = records.WeiboArticle(sUrl=aArticleUrls[1]);
    await sou.getArticle(article=article, isWithComment=True);
    pprint(vars(article));

    print('weibo source tested');

    await sou.cleanup();
    return True;

async def testMakeData():
    print('testMakeData');

    def show(record):
        nonlocal cursor
        from pprint import pprint
        mData = dispose.makeData(record);
        pprint(mData);
        #[sName, sType, value for sName, sType, value in mData.values()];
        aData = [data for data in mData.values()];
        template = '\n'.join('{}-{}-%s'.format(data.field, data.type) for data in aData)
        print();
        print(cursor.mogrify(template, [data.value for data in aData]).decode());
        print();

    aUserUrls = [
        'https://weibo.com/n/马伯庸',
        'https://weibo.com/u/1444865141',
        'https://m.weibo.cn/u/1811696373'
    ];
    aPostUrls = [
        'https://weibo.com/1444865141/G2MA2rq4A?from=page_1005051444865141_profile&wvr=6&mod=weibotime',
        'https://m.weibo.cn/status/G3rdV10vb',
        'https://m.weibo.cn/status/4180192106660800'
    ];

    sou = source.WeiboSource();
    print(sou.UA);
    disposer = dispose.PostgresDisposer();
    disposer.prepare();
    cursor = disposer.conn.cursor();

    user = await sou.getUser(aUserUrls[0]);
    print(user.mVarCast);
    print(user.mVarCastRev);
    show(user);

    post = await sou.getPost(aPostUrls[0]);
    await sou.getPost(post=post, isWithComment=True);
    aWeibo1 = await sou.getWeibo(user=user, nPage=1, nPageCount=1, isFull=True);
    user.aPosts.extend(aWeibo1);
    show(user);

    post = records.WeiboPost(sUrl=aPostUrls[1]);
    await sou.getPost(post=post, isWithComment=True);
    print(post.mVarCast);
    print(post.mVarCastRev);
    show(post);

    await sou.getPost(aPostUrls[2]);

    await sou.cleanup();

    print('makeData tested');

async def testDispose():
    print('testDispose');
    aUserUrls = [
            'https://weibo.com/n/马伯庸',
            'https://m.weibo.cn/u/1811696373'
    ];
    sou = source.WeiboSource();
    user1 = await sou.getUser(aUserUrls[0]);
    user2 = await sou.getUser(aUserUrls[1], isFull=True);
    disposer = dispose.PostgresDisposer('test', 'postgres');
    try:
        disposer._execute('dsf');
    except Exception as e:
        log.info(e);
    disposer.runForever();
    disposer.prepare();
    disposer.dropTable(user1, False);
    disposer.createTable(user1, False);
    try:
        disposer.createTable(user2);
    except Exception as e:
        log.info(e);
    disposer.feed(user1);
    disposer.joinQueue();
    disposer.feed(user1);
    disposer.pause();
    disposer.feed(user2);
    time.sleep(2);
    print('user2 fed');
    disposer.feed(user2);
    disposer.resume();
    disposer.joinQueue();
    disposer.feedEnd();
    disposer.feed(user1);
    disposer.joinQueue();
    disposer.join();

    disposer.prepare();
    disposer.createTable(records.Media, False);
    disposer.cleanup();
    try:
        disposer._execute('ssasddsafdsaf');
    except Exception as e:
        log.info(e);

    disposer = dispose.PostgresDisposer('test', 'postgres', isSafe=True);
    disposer.prepare();
    print(vars(user1));
    print(disposer.fetchRecords(user1));
    print(disposer.fetchRecords(records.WeiboUser, sId=user2.sId));
    user3 = records.WeiboUser(sId = user1.sId);
    print(vars(user3));
    disposer.updateRecord(user3);
    print(vars(user3));
    disposer.close();

    print('postgres disposing tested');

    await sou.cleanup();

async def testTiebaSource():
    print('testTiebaSource');
    from pprint import pprint
    sou = source.TiebaSource();
    sForumUrl = 'http://tieba.baidu.com/f?kw=kokia&ie=utf-8&pn=7800';
    sPostUrl = 'http://tieba.baidu.com/p/5521603618?red_tag=m1841643583';
    sUserName = 'clamp疯子';
    sUserUrl = 'http://tieba.baidu.com/home/main/?un=clamp%E7%96%AF%E5%AD%90&ie=utf-8&fr=frs';

    post = await sou.getPost(sPostUrl, nPageCount=2);
    print(post);
    pprint(vars(post));
    pprint(vars(post.aComments[0]));
    print(post.htmlBytes().decode());
    print('post tested\n');

    user = await sou.getUser(sUserName);
    print(user);
    user = await sou.getUser(sUrl=sUserUrl);
    pprint(vars(user));
    print('user tested\n');

    forum = await sou.getForum(sForumUrl, nPage=7, nPageCount=3);
    print(forum);
    pprint(vars(forum));
    pprint(vars(forum.aPosts[0]));
    print(forum.allUser());
    print('forum tested\n');

    forum = await sou.getForum(sForumUrl)
    await sou.getForumDetail(forum);
    print(forum);
    pprint(vars(forum));
    pprint(vars(forum.aPosts[0]));
    print(forum.allUser());
    print('detailed forum tested\n');

    await sou.cleanup();
    print('testTiebaSource tested')

async def testRegulatedTask():
    print('testRegulatedTask');
    loop = asyncio.get_event_loop();
    arranger = asset.TaskArranger(loop=loop, nMaxConcurrent=5);
    async def one(n):
        print('task {} started'.format(n));
        await asyncio.sleep(random.random()*7, loop=loop);
        print('task {} ended'.format(n));
    #tasks = [await arranger.regulatedTask(one(n)) for n in range(8)] # 'await' expressions in comprehensions are not supported before python3.6
    tasks = [];
    for n in range(8):
        tasks.append(await arranger.regulatedTask(one(n)));
    await asyncio.gather(*tasks);
    print('testRegulatedTask tested');
    await arranger.join();
    await arranger.close();

def test():
    loop = asyncio.get_event_loop();
    try:
        print('test start');
        arranger.task(testSource());
        arranger.task(testMakeData());
        asset.arun(arranger.task(testDispose()));
        testLock();
        arranger.task(testTiebaSource());
        asset.arun(testRegulatedTask());
        asset.arun(arranger.join(isGather=True));
        print('test end');
    except asset.DeadArrangerError as e:
        log.info(e)
    except KeyboardInterrupt as e:
        raise;
    except Exception as e:
        config.log.exception(e);
        debug.pm();
        raise;
    finally:
        #debug.setTrace();
        asset.arun(arranger.close());
        loop.run_until_complete(asset.cleanup());
        loop.close();

if __name__ == '__main__':
    test();


