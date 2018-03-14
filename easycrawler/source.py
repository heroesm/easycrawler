import logging
import re
import json
import asyncio
import datetime
import time
import html
import urllib.parse

import aiohttp
import lxml
import lxml.etree, lxml.html

from . import records
from . import asset
from .asset import fetchBytes, fetchJson, Response, mergeQuery, innerHtml, html2Unicode
from .configure import config


log = logging.getLogger(__name__);

def prepare():
    global log;
    log.setLevel(config.nLogLevel);
prepare();

class UrlUnmatchError(Exception):
    pass

class PostNotFoundError(Exception):
    pass

class ArticleNotFoundError(PostNotFoundError):
    pass

class Url(str):
    isResolved = False;

class Source():
    def __init__(self, sName=None, sUa=None, loop=None, arranger=None):
        self.sName = sName;
        self.UA = sUa or asset.UA;
        self.loop = loop or asyncio.get_event_loop();
        mHeaders = {'User-Agent': self.UA};
        connector = aiohttp.TCPConnector(force_close=True, enable_cleanup_closed=True);
        self.session = aiohttp.ClientSession(connector=connector, headers=mHeaders, trust_env=True, read_timeout=config.nReadTimeout);
        self.parser = lxml.html.HTMLParser(encoding='utf-8');
        self.arranger = arranger or asset.arranger;
    async def resolve(self, sUrl):
        assert isinstance(sUrl, str);
        if (getattr(sUrl, 'isResolved', None)):
            return sUrl;
        else:
            async with Response(sUrl, session=self.session) as res:
                sLastUrl = Url(res.url);
            sLastUrl.isResolved = True;
            return sLastUrl;
    async def queryJson(self, sUrl, mAssert=None, isTypeCheck=False):
        return await fetchJson(sUrl, session=self.session, mAssert=mAssert, isTypeCheck=isTypeCheck);
    async def queryBytes(self, sUrl):
        return await fetchBytes(sUrl, session=self.session);
    def parse(self, html, *arg, parser=None, **karg):
        if (lxml.etree.iselement(html)):
            return html;
        if (isinstance(html, bytes)):
            html = html2Unicode(html);
        parser = parser or self.parser;
        return lxml.html.fromstring(html, *arg, **karg, parser=parser);
    def grab(self):
        pass
    async def cleanup(self):
        await self.session.close();
        log.debug('{} closed'.format(type(self).__name__));

class HtmlSource(Source):
    pass

class JsonSource(Source):
    pass

class TextSource(Source):
    pass

class BlogSource(Source):
    pass

class QuestionSource(Source):
    pass

class DiscuzSource(Source):
    pass

class TiebaSource(Source):
    sApiPost = 'http://tieba.baidu.com/p/{}?pn={}'; # post.sId, nPage
    sApiComment = 'https://tieba.baidu.com/p/totalComment?tid={}&pn={}&t={}&see_lz=0' # post.sId, nPage, nTimestamp
    sApiUser = 'http://tieba.baidu.com/home/get/panel?ie=utf-8&un={}' # user.sName
    sApiAvatar = 'http://himg.bdimg.com/sys/portrait/item/{}.jpg' # avatar ID
    sApiForum = 'http://tieba.baidu.com/f?kw={}&ie=utf-8&pn={}'; # forum.sName, nPosts (50 * (nPage-1))
    sApiMobileForum = 'https://tieba.baidu.com/mo/q----,sz@320_240-1-3---2/m?kw={}&pn={}'; #forum.sName, nPosts (20 * (nPage-1))

    postPattern = re.compile(r'^https?://tieba\.baidu\.com/p/(\d+)(?:\?|$)');
    postPropPattern = re.compile(r'PageData\.thread\s*=\s*\{\s*author\s*:\s*"(.+?)"\s*,\s*thread_id\s*:\s*(\d+?)\s*,\s*title\s*:\s*"(.+?)"s*,\s*reply_num\s*:\s*(\d+?)\s*,');
    userPattern = re.compile(r'https?://tieba\.baidu\.com/home/main/?\?(?:[^#]+&)?un=([^&#]+)');
    forumPattern = re.compile(r'^https?://tieba\.baidu\.com/f\?(?:[^#]+&)?kw=([^&#]+)');
    forumPropPattern = re.compile(r'PageData\.forum\s*=\s*(\{[\s\S]+?\})\s*;');
    pagePropPattern = re.compile(r'PageData\.pager\s*=\s*(\{[\s\S]+?\})\s*;');
    quotePattern = re.compile(r'(?<!\\)\''); # replace ' with " to make it valid JSON string, but keep ' which following \
    commentedForumPattern = re.compile(rb'<!--\s*<ul id="thread_list"'); # post list in tieba forum page is commented out in some case

    commentPropPath = lxml.etree.XPath('//div[contains(@class, "l_post")]/@data-field');
    postPageDataPath = lxml.etree.XPath('.//div[@class="wrap1"]//script[1]');
    forumPageDataPath = lxml.etree.XPath('/html/head/script[contains(., "PageData.forum")][1]');
    titlePath = lxml.etree.XPath('/html/head/title/text()');
    forumLiPath = lxml.etree.XPath('//ul[@id="thread_list"]//li[contains(@class, "j_thread_list")]');
    postTitlePath = lxml.etree.XPath('.//div[contains(@class, "threadlist_title")]/a/@title');

    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs);
    async def getPost(self, sUrl=None, post=None, nPageCount=1):
        assert sUrl or post.sId;
        isReturn = False if post else True;
        post = post or records.TiebaPost();
        nPage = 1;
        nPageCount = float('inf') if nPageCount == 0 else nPageCount or 1;
        nMaxPage = nPage + nPageCount - 1;
        if (not post.sId):
            match = self.postPattern.search(sUrl);
            if (not match):
                sUrl = await self.resolve(sUrl);
                match = self.postPattern.search(sUrl);
            if (not match):
                raise UrlUnmatchError(sUrl, type(post));
            post.sId = match.group(1);
        post.sUrl = self.sApiPost.format(post.sId, 1);
        post.aComments = []
        while nPage <= nMaxPage:
            aComments = await self.getComments(sPostId=post.sId, nPage=nPage, nPageCount=1);
            if (aComments):
                aSubComments = await self.getSubComments(sPostId=post.sId, nPage=nPage, nPageCount=1);
                self.attachComments(aComments, aSubComments);
                post.aComments.extend(aComments);
                post.aComments.extend(aSubComments);
                nPage += 1;
            else:
                break;
        firstFloor = post.aComments[0];
        post.sName = firstFloor.sName;
        post.fetchTime = firstFloor.fetchTime;
        post.nComments = firstFloor.nComments;
        post.sForum = firstFloor.sForum;
        post.sForumId = firstFloor.sForumId;
        if (firstFloor.aIndices[0] == 1):
            post.sText = firstFloor.sText;
            post.sContent = innerHtml(firstFloor.sContent, isAggregate=True);
            post.date = firstFloor.date;
            post.aAttach = firstFloor.aAttach;
            post.author = firstFloor.author
            #post.sAuthorId = firstFloor.sAuthorId;
            #post.sAuthor = firstFloor.sAuthor;
        log.debug('post {} got'.format(post));
        if (isReturn):
            return post;
    async def getComments(self, sPostId, nPage=1, nPageCount=1):
        assert sPostId;
        nPage = nPage or 1;
        nPageCount = float('inf') if nPageCount == 0 else nPageCount or 1;
        nMaxPage = nPage + nPageCount - 1;
        aResult = []
        while nPage <= nMaxPage:
            sApi = self.sApiPost.format(sPostId, nPage);
            bData = await self.queryBytes(sApi);
            aComments = self.parseComments(data=bData, sPostId=sPostId, nPage=nPage);
            if (aComments):
                aResult.extend(aComments);
                nPage += 1;
            else:
                break;
        return aResult;
    def parseComments(self, data, sPostId, nPage):
        ele = self.parse(data);
        sTitle, nComments, sForumId, sForum, nMaxPage = self.parsePageData(ele);
        if (nPage > nMaxPage):
            return [];
        aDataFields = self.commentPropPath(ele);
        aComments = []
        for sData in aDataFields:
            mData = json.loads(html.unescape(sData));
            if not ('date' in mData['content']):
                continue; # advertisement
            comment = records.TiebaComment();
            comment.fetchTime = datetime.datetime.now();
            comment.sId = str(mData['content']['post_id']);
            comment.nPage = nPage;
            comment.sPostId = sPostId;
            sIdAttr = 'post_content_{}'.format(comment.sId);
            comment.sUrl = '{}#{}'.format(self.sApiPost.format(sPostId, nPage), sIdAttr);
            target = ele.xpath('//div[@id="{}"]'.format(sIdAttr))[0];
            #comment.sText = target.text_content();
            comment.sText = lxml.etree.tostring(target, method='text', encoding='utf-8').decode(errors='replace');
            comment.sContent = innerHtml(target);
            comment.date = datetime.datetime.strptime(mData['content']['date'], '%Y-%m-%d %H:%M');
            sAuthor = mData['author'].get('user_name');
            sAuthorId = str(mData['author'].get('user_id'));
            comment.author = records.TiebaUser(sId=sAuthorId, sName=sAuthor);
            if (comment.author.sName and comment.author.sName.endswith('.*')):
                comment.author.isAnonymous = True;
            comment.aIndices = [int(mData['content']['post_no'])];
            comment.sForumId = sForumId;
            comment.sForum = sForum;
            comment.sParentId = None;
            comment.aAttach = None;
            aComments.append(comment);
        if (nPage == 1):
            aComments[0].sName = sTitle or self.titlePath(ele)[0];
            aComments[0].nComments = nComments;
        return aComments;
    def parsePageData(self, data):
        ele = self.parse(data);
        sScript = self.postPageDataPath(ele)[0].text;
        match = self.postPropPattern.search(sScript);
        sAuthor, sPostId, sTitle, nComments = match.groups();
        match = self.forumPropPattern.search(sScript);
        #mData = json.loads(match.group(1).replace('\'', '"'));
        mData = json.loads(self.quotePattern.sub('"', match.group(1)));
        sForumId = str(mData.get('true_forum_id') or mData['forum_id']);
        sForum = mData['forum_name'];
        match = self.pagePropPattern.search(sScript);
        #mData = json.loads(match.group(1).replace('\'', '"'));
        mData = json.loads(self.quotePattern.sub('"', match.group(1)));
        nMaxPage = mData['total_page']
        return sTitle, nComments, sForumId, sForum, nMaxPage;
    async def getSubComments(self, sPostId, nPage=1, nPageCount=1):
        assert sPostId;
        nPage = nPage or 1;
        nPageCount = float('inf') if nPageCount == 0 else nPageCount or 1;
        nMaxPage = nPage + nPageCount - 1;
        aResult = []
        while nPage <= nMaxPage:
            sApi = self.sApiComment.format(sPostId, nPage, int(time.time()*1000));
            mData = await self.queryJson(sApi, mAssert={'errno': 0});
            aSubComments = self.parseSubComments(data=mData, sPostId=sPostId, nPage=nPage);
            if (aSubComments):
                aResult.extend(aSubComments);
                nPage += 1;
            else:
                break;
        return aResult;
    def parseSubComments(self, data, sPostId, nPage):
        mData = data if isinstance(data, dict) else json.loads(data);
        aRawComments = mData['data']['comment_list'];
        if (not aRawComments):
            return [];
        aSubComments = [];
        for sParentId, mComments in aRawComments.items():
            for nIndex, mComment in enumerate(mComments['comment_info'], 1):
                comment = records.TiebaComment();
                comment.fetchTime = datetime.datetime.now();
                comment.sId = str(mComment['comment_id']);
                comment.nPage = nPage;
                comment.sPostId = sPostId;
                comment.sContent = innerHtml(mComment['content'], isAggregate=True);
                comment.sText = self.parse(comment.sContent).text_content();
                comment.date = datetime.datetime.fromtimestamp(mComment['now_time']);
                comment.author = records.TiebaUser(sId=mComment.get('user_id'), sName=mComment.get('username'));
                if (comment.author.sName and comment.author.sName.endswith('.*')):
                    comment.author.isAnonymous = True;
                comment.aIndices = [None, nIndex]; # first element modified in attachComments method
                comment.sParentId = str(mComment['post_id']);
                aSubComments.append(comment);
        return aSubComments;
    def attachComments(self, aComments, aSubComments):
        mCommentIndex = {comment.sId: comment for comment in aComments};
        for subComment in aSubComments:
            parent = mCommentIndex[subComment.sParentId];
            subComment.parent = parent;
            subComment.aIndices[0] = parent.aIndices[0];
            subComment.sForumId = parent.sForumId;
            subComment.sForum = parent.sForum;
        return True;
    async def getUser(self, sName=None, user=None, sUrl=None):
        assert sUrl or sName or user.sName
        isReturn = False if user else True;
        if (not sName and user):
            sName = user.sName;
        if (not sName and sUrl):
            match = self.userPattern.search(sUrl);
            if (match):
                sName = urllib.parse.unquote(match.group(1));
            else:
                raise UrlUnmatchError(sUrl, records.TiebaUser);
        user = user or records.TiebaUser();
        if (sName and sName.endswith('.*')):
            # anonymous
            user.sName = sName;
            user.isAnonymous = True;
            return user;
        if (sName is None):
            return user;
        sApi = self.sApiUser.format(sName);
        mData = await self.queryJson(sApi, mAssert={'error': '成功'});
        mData = mData['data'];
        user.sId = str(mData['id']);
        user.sName = mData['name'];
        assert user.sName == sName;
        user.sNickName = mData.get('name_show');
        user.sAvatar = self.sApiAvatar.format(mData['portrait'].split('?')[0]);
        user.sGender = 'male' if mData['sex'] == 'male' else 'female' if mData['sex'] == 'female' else None;
        log.debug('user {} got'.format(user));
        if (isReturn):
            return user;
    async def getForum(self, sUrl=None, forum=None, nPage=1, nPageCount=1, isWithDetail=False):
        assert sUrl or forum.sName;
        isReturn = False if forum else True;
        forum = forum or records.TiebaForum();
        if not (forum and forum.sName):
            match = self.forumPattern.search(sUrl);
            if (not match):
                sUrl = await self.resolve(sUrl);
                match = self.forumPattern.search(sUrl);
            if (not match):
                raise UrlUnmatchError(sUrl, type(forum));
            forum.sName = urllib.parse.unquote(match.group(1));
        sApi = self.sApiForum.format(forum.sName, 0);
        forum.fetchTime = datetime.datetime.now();
        forum.aPosts = [];
        nPage = nPage or 1;
        nPageCount = float('inf') if nPageCount == 0 else nPageCount or 1;
        nMaxPage = nPage + nPageCount - 1;
        while nPage <= nMaxPage:
            nPosts = (nPage-1)*50;
            sApi = mergeQuery(sApi, {'pn': nPosts});
            bData = await self.queryBytes(sApi);
            #bData = re.sub(rb'<!--\s*<ul id="thread_list"', b'<ul id="thread_list"', bData);
            bData = self.commentedForumPattern.sub(b'<ul id="thread_list"', bData);
            ele = self.parse(bData);
            self.parseForum(ele, forum=forum);
            aPosts = self.parsePosts(data=ele, idSet=forum.postIdSet);
            if (aPosts):
                forum.aPosts.extend(aPosts);
                nPage += 1;
            else:
                break;
        if (isWithDetail):
            await self.getForumDetail(forum=forum);
        log.debug('forum {} got'.format(forum));
        if (isReturn):
            return forum;
    def parseForum(self, data, forum=None):
        isReturn = False if forum else True;
        ele = self.parse(data);
        forum = forum or records.TiebaForum();
        if not (forum.sId and forum.sName and forum.sUrl):
            sScript = self.forumPageDataPath(ele)[0].text;
            match = self.forumPropPattern.search(sScript);
            #mData = json.loads(match.group(1).replace('\'', '"'));
            mData = json.loads(self.quotePattern.sub('"', match.group(1)));
            forum.sId = str(mData['id']);
            forum.sName = mData['name'];
            forum.sUrl = self.sApiForum.format(forum.sName, 0);
        if (isReturn):
            return forum
    def parsePosts(self, data, idSet):
        ele = self.parse(data);
        aPosts = [];
        aLi = self.forumLiPath(ele);
        for li in aLi:
            mData = json.loads(html.unescape(li.get('data-field')));
            if (mData['id'] in idSet):
                continue;
            post = records.TiebaPost();
            post.sId = str(mData['id']);
            post.author = records.TiebaUser(sName=mData['author_name']);
            if (post.author.sName and post.author.sName.endswith('.*')):
                post.author.isAnonymous = True;
            post.sUrl = self.sApiPost.format(post.sId, 1);
            post.sName = self.postTitlePath(li)[0];
            idSet.add(post.sId);
            aPosts.append(post);
        return aPosts;
    async def getForumDetail(self, forum, nPostPages=0):
        if (not nPostPages): nPostPages = 0;
        aTasks = []
        for post in forum.aPosts:
            if (not post.fetchTime):
                aTasks.append(self.arranger.task(self.getPost(post=post, nPageCount=nPostPages)));
        for user in forum.allUser():
            if (not user.fetchTime):
                aTasks.append(self.arranger.task(self.getUser(user=user)));
        if (aTasks):
            await asyncio.gather(*aTasks, loop=self.loop);
        log.debug('detail of forum {} got'.format(forum));
        return True;

class WeiboSource(Source):
    sApiEntryUid = 'https://m.weibo.cn/api/container/getIndex?type=uid&value={}' # user.sId
    sApiEntryCid = 'https://m.weibo.cn/api/container/getIndex?containerid={}' # user.sEntryCid
    sApiUserInfo = 'https://m.weibo.cn/api/container/getIndex?containerid={}_-_INFO' # user.sProfileCid
    sApiUserWeibo = 'https://m.weibo.cn/api/container/getIndex?containerid={}&page={}' # user.sWeiboCid, nPage
    sApiPost = 'https://m.weibo.cn/status/{}'; # post.sId
    sApiComment = 'https://m.weibo.cn/api/comments/show?id={}&page={}' # post.sId, nPage
    sApiArticle = 'https://media.weibo.cn/article?id={}&display=0&retcode=6102' # article.sPageId

    UA = 'Android / Chrome 40: Mozilla/5.0 (Linux; Android 5.1.1; Nexus 4 Build/LMY48T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.89 Mobile Safari/537.36';

    postStatusPattern = re.compile(rb'<script>[^<>]+?var \$render_data = (\[\{[\s\S]+?\}\])\[0\] \|\| \{\};\s*</script>');
    userCidPattern = re.compile(r'^https?://m\.weibo\.cn/p/(\d+)');
    userUidPattern = re.compile(r'^https?://m\.weibo\.cn/u/(\d+)');
    postPattern = re.compile(r'^https?://m\.weibo\.cn/status/(\w+)');
    articlePattern = re.compile(r'https?://media\.weibo\.cn/article/?\?(?:[^#]+&)?id=(\d+)');

    
    def __init__(self, *args, sUa=None, **kargs):
        super().__init__(*args, **kargs, sUa=sUa or self.UA);

    async def getUser(self, sUrl=None, user=None, isFull=False):
        assert sUrl or user
        isReturn = False if user else True;
        user = user or records.WeiboUser();
        assert isinstance(user, records.WeiboUser);
        sCid = None;
        sApi = None;
        if (user.sEntryCid):
            sApi = self.sApiEntryCid.format(user.sEntryCid);
        elif (user.sId):
            sApi = self.sApiEntryUid.format(user.sId);
        if (not sApi):
            sUrl = sUrl or user.sUrl;
            assert sUrl;
            match = self.userCidPattern.search(sUrl);
            if (not match):
                sUrl = await self.resolve(sUrl);
            match = self.userCidPattern.search(sUrl);
            if (match):
                sCid = match.group(1);
                sApi = self.sApiEntryCid.format(sCid);
            else:
                match = self.userUidPattern.search(sUrl);
                if (match):
                    nUid = match.group(1);
                    sApi = self.sApiEntryUid.format(nUid);
                else:
                    raise UrlUnmatchError(sApi, type(user));
        if (sCid):
            user.sEntryCid = sCid;
        mData = await self.queryJson(sApi, mAssert={'ok': 1});
        self.parseUser(data=mData, user=user);
        if (isFull):
            assert user.sProfileCid;
            sApi = self.sApiUserInfo.format(user.sProfileCid);
            mData = await self.queryJson(sApi, mAssert={'ok': 1});
            self.parseInfo(mData, user=user);
        log.debug('user {} got'.format(user));
        if (isReturn):
            return user;
    def parseUser(self, data=None, mUser=None, user=None):
        assert data or mUser;
        isReturn = False if user else True;
        user = user or records.WeiboUser();
        assert isinstance(user, records.WeiboUser);
        if (mUser):
            mUserInfo = mUser;
        else:
            mData = data if isinstance(data, dict) else json.loads(data.decode());
            assert mData['ok'] == 1;
            mData = mData['data'];
            mUserInfo = mData['userInfo'];
            user.sProfileCid = mData['tabsInfo']['tabs'][0]['containerid'];
            user.sWeiboCid = mData['tabsInfo']['tabs'][1]['containerid'];
        user.fetchTime = datetime.datetime.now();
        user.sId = str(mUserInfo['id']);
        user.sName = mUserInfo['screen_name'];
        if (mUserInfo.get('profile_url')):
            user.sUrl = mUserInfo['profile_url'].split('?')[0];
        user.sAvatar = mUserInfo['profile_image_url'];
        user.sAvatarHd = mUserInfo.get('avatar_hd');
        user.sIntro = mUserInfo.get('description');
        user.sGender = None if 'gender' not in mUserInfo  else 'male' if mUserInfo['gender'].startswith('m') else 'female';
        user.nFans = mUserInfo.get('followers_count');
        user.nFollow = mUserInfo.get('follow_count');
        if (isReturn):
            return user;
    def parseInfo(self, data, user):
        assert isinstance(user, records.WeiboUser);
        mData = data if isinstance(data, dict) else json.loads(data.decode());
        assert mData['ok'] == 1;
        aCards = mData['data']['cards'];
        for mCard in aCards:
            for mItem in mCard['card_group']:
                if ('item_name' in mItem):
                    user.mProperties[mItem['item_name']] = mItem['item_content'];
    async def getPost(self, sUrl=None, post=None, isWithComment=False, isRaise=True):
        assert sUrl or post
        isReturn = False if post else True;
        post = post or records.WeiboPost();
        if (post and (post.sBid or post.sId)):
            sApi = self.sApiPost.format(post.sBid or post.sId);
        else:
            sUrl = sUrl or post.sUrl;
            match = self.postPattern.search(sUrl);
            if (not match):
                sUrl = await self.resolve(sUrl);
                match = self.postPattern.search(sUrl);
            if (not match):
                raise UrlUnmatchError(sUrl, type(post));
            sApi = sUrl;
        assert sApi;
        bData = await self.queryBytes(sApi);
        match = self.postStatusPattern.search(bData);
        if (not match):
            if (isRaise):
                raise PostNotFoundError(self);
            else:
                return False;
        mData = json.loads(match.group(1).decode())[0]['status'];
        self.parsePost(data=mData, post=post);
        post.sUrl = sApi.split('?')[0];
        if (isWithComment):
            post.aHotComments.extend(await self.getComments(post=post, isHot=True));
        log.debug('post {} got'.format(post));
        if (isReturn):
            return post;
    def parsePost(self, data, post=None):
        isReturn = False if post else True;
        post = post or records.WeiboPost();
        mBlog = data if isinstance(data, dict) else json.loads(data.decode());
        self.objectifyBlog(mBlog, post);
        if (isReturn):
            return post;
    def objectifyBlog(self, mBlog, post=None):
        isReturn = False if post else True;
        post = post or records.WeiboPost();
        post.fetchTime = datetime.datetime.now();
        post.author = self.parseUser(mUser=mBlog['user']);
        #post.sAuthorId = str(mBlog['user']['id']);
        #post.sAuthor = mBlog['user']['screen_name'];
        post.sId = str(mBlog['id']);
        post.sBid = mBlog['bid'];
        post.sContent = innerHtml(mBlog['text'], isAggregate=True);
        post.sText = mBlog.get('raw_text');
        post.sName = (post.sText or post.sContent or '')[:9];
        post.sSource = mBlog.get('source');
        try:
            sTime = mBlog['created_at'];
            post.date = datetime.datetime.strptime(sTime, '%a %b %d %H:%M:%S %z %Y');
        except ValueError:
            pass
        post.sThumb = mBlog.get('thumbnail_pic');
        post.nComments = mBlog['comments_count'];
        post.nLike = mBlog['attitudes_count'];
        mRepost = mBlog.get('retweeted_status');
        if (mRepost):
            post.repost = self.objectifyBlog(mBlog=mRepost);
            post.sQuoteId = post.repost.sId;
            post.sQuote = post.repost.sContent;
        aPicIds = [item.sId for item in post.aAttach];
        for i, mPic in enumerate(mBlog.get('pics', ()), 1):
            if (mPic['pid'] not in aPicIds):
                post.aAttach.append(records.Image(
                        sId = str(mPic['pid']),
                        sPostId = post.sId,
                        date=post.date,
                        sUrl=mPic['large']['url'],
                        author=post.author,
                        sPreview=mPic['url'],
                        sName='pic_{}'.format(i)
                ));
        if (isReturn):
            return post;
    async def getComments(self, post=None, sPostId=None, nPage=1, nPageCount=1, isHot=False):
        if (post):
            sPostId = post.sId;
        assert sPostId;
        aComments = [];
        if (isHot):
            sApi = self.sApiComment.format(sPostId, 1)
            mData = await self.queryJson(sApi);
            aComments = self.parseComments(data=mData, sPostId=sPostId, isHot=True);
        else:
            nPage = nPage or 1;
            nPageCount = float('inf') if nPageCount == 0 else nPageCount or 1;
            nMaxPage = nPage + nPageCount - 1;
            while True:
                sApi = self.sApiComment.format(sPostId, nPage)
                mData = await self.queryJson(sApi);
                aResult = self.parseComments(mData, sPostId=sPostId);
                aComments.extend(aResult);
                nPage += 1;
                if (nPage > nMaxPage):
                    break;
        return aComments;
    def parseComments(self, data, sPostId=None, isHot=False):
        mData = data if isinstance(data, dict) else json.loads(data.decode());
        if (mData['ok'] != 1):
            return [];
        aComments = [];
        mData = mData['data'];
        if (isHot and mData.get('hot_data')):
            for mComment in mData['hot_data']:
                comment = records.WeiboComment();
                comment.fetchTime = datetime.datetime.now();
                comment.sId = str(mComment['id']);
                comment.sType = 'hot';
                comment.sSource = mComment['source'];
                comment.sContent = innerHtml(mComment['text'], isAggregate=True);
                comment.sName = comment.sContent[:9];
                comment.nLike = mComment['like_counts'];
                comment.author = self.parseUser(mUser=mComment['user']);
                comment.sPostId = sPostId;
                aComments.append(comment);
            return aComments;
        else:
            for mComment in mData['data']:
                comment = records.WeiboComment();
                comment.fetchTime = datetime.datetime.now();
                comment.sId = str(mComment['id']);
                comment.sSource = mComment['source'];
                comment.sContent = innerHtml(mComment['text'], isAggregate=True);
                comment.sName = comment.sContent[:9];
                comment.nLike = mComment['like_counts'];
                comment.author = self.parseUser(mUser=mComment['user']);
                comment.sPostId = sPostId;
                aComments.append(comment);
            return aComments;
    async def getWeibo(self, user=None, sCid=None, nPage=1, nPageCount=1, isFull=False, isWithComment=False):
        if (user):
            sWeiboCid = user.sWeiboCid;
        else:
            sWeiboCid = sCid;
        assert sWeiboCid;
        nPage = nPage or 1;
        nPageCount = float('inf') if nPageCount == 0 else nPageCount or 1;
        nMaxPage = nPage + nPageCount - 1;
        aWeibo = [];
        aTasks = []
        while True:
            sApi = self.sApiUserWeibo.format(sWeiboCid, nPage);
            mData = await self.queryJson(sApi, mAssert={});
            aResult = self.parseWeibo(mData);
            if (isFull):
                for post in aResult:
                    aTasks.append(self.arranger.task(self.getPost(post=post, isWithComment=isWithComment, isRaise=False)));
            aWeibo.extend(aResult);
            nPage += 1;
            if (nPage > nMaxPage or not aResult):
                break;
        if (aTasks):
            await asyncio.gather(*aTasks);
        return aWeibo;
    def parseWeibo(self, data):
        mData = data if isinstance(data, dict) else json.loads(data.decode());
        if (mData['ok'] != 1):
            return [];
        aCards = mData['data']['cards']
        aPosts = [];
        for card in aCards:
            if (card['card_type'] != 9):
                continue;
            post = records.WeiboPost();
            post.sUrl = card['scheme'].split('?')[0];
            mBlog = card['mblog'];
            self.objectifyBlog(mBlog=mBlog, post=post);
            aPosts.append(post);
        return aPosts;
    async def getArticle(self, sUrl=None, article=None, isWithComment=False, isRaise=True):
        assert sUrl or article;
        isReturn = False if article else True;
        article = article or records.WeiboArticle();
        if (article and article.sPageId):
            sApi = self.sApiArticle.format(article.sPageId);
        else:
            sUrl = sUrl or article.sUrl;
            assert sUrl;
            match = self.articlePattern.search(sUrl);
            if (not match):
                sUrl = await self.resolve(sUrl);
                match = self.articlePattern.search(sUrl);
            if (not match):
                raise UrlUnmatchError(sUrl, type(article));
            sApi = sUrl;
        bData = await self.queryBytes(sApi);
        match = self.postStatusPattern.search(bData);
        if (not match):
            if (isRaise):
                raise ArticleNotFoundError(self);
            else:
                return False;
        mData = json.loads(match.group(1).decode())[0];
        self.parseArticle(mData=mData, article=article);
        article.sUrl = self.sApiArticle.format(article.sPageId);
        if (isWithComment):
            article.aHotComments.extend(await self.getComments(post=article, isHot=True));
        log.debug('article {} got'.format(article));
        if (isReturn):
            return article;
    def parseArticle(self, mData, article=None):
        isReturn = False if article else True;
        article = article or records.WeiboArticle();
        article.fetchTime = datetime.datetime.now();
        article.sName = mData['title'];
        article.sContent = innerHtml(mData['content'], isAggregate=True);
        article.sText = self.parse(mData['content']).text_content();
        article.user = self.parseUser(mUser=mData['userinfo']);
        article.sPageId = mData['page_id'];
        article.date = datetime.datetime.strptime(mData['created_time'][:19], '%Y-%m-%dT%H:%M:%S');
        mBlog = mData['mblog'];
        article.sId = str(mBlog['id']);
        article.nComments = mBlog['comments_count'];
        article.nLike = mBlog['attitudes_count'];
        if (isReturn):
            return article;

    async def getRecord(self, sUrl):
        sUrl = sUrl.split('#')[0];
        return await self.getUser(sUrl) or await self.getPost(sUrl);
