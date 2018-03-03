import logging
import asyncio
import os
import re
import socket
import urllib.parse
from concurrent.futures import CancelledError
import time
import json

import lxml.html
import aiohttp

from .configure import config

UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Safari/537.36 Edge/13.10586';

aiohttpSession = None
#pool = asyncio.BoundedSemaphore(config.nFetchLimit);

log = logging.getLogger(__name__);

def prepare():
    global log;
    log.setLevel(config.nLogLevel);
    socket.setdefaulttimeout(30);
prepare();

class DeadArrangerError(Exception):
    def __init__(self, *arg, arranger=None, **karg):
        super().__init__(*arg, **karg);
        self.arranger=arranger;

class TaskArranger():
    def __init__(self, loop=None, nLife=None, nVolume=None, nWidth=None):
        self.loop = loop or asyncio.get_event_loop();
        self.nLife = nLife or None;
        self.nVolume = nVolume or None;
        self.nWidth = nWidth or None;
        self.isClosed = False;
        self.aliveTask = set();
        self.exceptionTask = set();
        self.nBirth = time.time();
        self.nDone = 0;
        self.closeFuture = self.loop.create_future();
        self.doneFuture = self.loop.create_future();
    def done(self, task):
        self.aliveTask.discard(task);
        self.nDone += 1;
        if (task.exception()):
            self.exceptionTask.add(task);
        if (not self.aliveTask and not self.doneFuture.done()):
            self.doneFuture.set_result(None);
    def task(self, coro):
        # mimic loop.create_task
        async def wrapped():
            try:
                if (self.isClosed):
                    raise CancelledError;
                elif (self.nLife and time.time() > self.nBirth + self.nLife):
                    log.info('task arranger dead because of exhausted lifetime ({}s)'.format(self.nLife));
                    await self.loop.create_task(self.close());
                    raise CancelledError;
                elif (self.nVolume and self.nDone > self.nVolume):
                    log.info('task arranger dead because of overflowed volume ({})'.format(self.nVolume));
                    await self.loop.create_task(self.close());
                    raise CancelledError;
                elif (self.nWidth and len(self.aliveTask) > self.nWidth):
                    log.info('task arranger dead because of tight width ({})'.format(self.nWidth));
                    await self.loop.create_task(self.close());
                    raise CancelledError;
                else:
                    return await coro;
            except CancelledError:
                if (self.isClosed):
                    raise DeadArrangerError(arranger=self);
                else:
                    raise;
        task = self.loop.create_task(wrapped());
        self.aliveTask.add(task);
        if (self.doneFuture.done()):
            self.doneFuture = self.loop.create_future();
        task.add_done_callback(self.done);
        return task;
    async def close(self, nTimeout=None):
        if (self.isClosed):
            return False
        log.info('close task arranger...');
        self.isClosed = True;
        for task in self.aliveTask:
            task.cancel();
        if (self.aliveTask):
            await asyncio.wait(tuple(self.aliveTask), loop=self.loop, timeout=nTimeout);
        for task in self.aliveTask:
            if (not task.done()):
                task.set_exception(DeadArrangerError(arranger=self));
        if (not self.closeFuture.done()):
            self.closeFuture.set_result(None);
        if (not self.doneFuture.done()):
            self.doneFuture.set_result(None);
        log.info('task arranger closed');
    async def join(self, nTimeout=None, isGather=False):
        await asyncio.wait_for(self.doneFuture, timeout=nTimeout, loop=self.loop);
        if (isGather and self.exceptionTask):
            return await asyncio.gather(*self.exceptionTask);

arranger = TaskArranger();

def arun(task, loop=None):
    loop = loop or asyncio.get_event_loop();
    return loop.run_until_complete(task);

def discard(aTaget, x):
    nCount = 0;
    nPos = 0;
    try:
        while True:
            nPos = aTaget.index(x, nPos);
            del aTaget[nPos];
            nCount += 1;
    except ValueError:
        return nCount

def mergeQuery(sUrl, mQuery, sFragment=None):
    scheme, netloc, path, query, fragment = urllib.parse.urlsplit(sUrl);
    if (sFragment): fragment = sFragment;
    mQueryNew = urllib.parse.parse_qs(query);
    mQueryNew.update(mQuery);
    query = urllib.parse.urlencode(mQueryNew, doseq=True);
    return urllib.parse.urlunsplit((scheme, netloc, path, query, fragment));

utf8Parser = lxml.html.HTMLParser(encoding='utf-8');

def innerHtml(data):
    if (lxml.etree.iselement(data)):
        ele = data 
    else:
        if (isinstance(data, bytes)):
            data = html2Unicode(data);
        assert isinstance(data, str);
        ele = lxml.html.fragment_fromstring(data, parser=utf8Parser);
    try:
        sData = ele.text or '';
    except UnicodeDecodeError as e:
        log.warning(e);
        ele = lxml.html.fromstring(lxml.etree.tostring(ele, method='html', encoding='utf-8').decode(errors='replace'))
        sData = ele.text or '';
    sData += ''.join(lxml.etree.tostring(child, method='html', encoding='utf-8').decode(errors='replace') for child in ele);
    return sData;

def html2Unicode(bData):
    try:
        import cchardet as chardet
    except ImportError:
        try:
            import chardet
        except ImportError:
            return bData.decode('utf-8');
    mResult = chardet.detect(bData);
    if (mResult['confidence'] > 0.5):
        sEnc = mResult['encoding'];
        sData = bData.decode(sEnc, 'replace');
        return sData;
    else:
        raise UnicodeError('can not identify the encoding');

def prettyHtml(bData, sMethod='html'):
    try:
        bData = html2Unicode(bData);
    except UnicodeError as e:
        log.warning(e);
    ele = lxml.html.fromstring(bData, parser=utf8Parser);
    sMethod = sMethod or 'html';
    return lxml.etree.tostring(ele, encoding='utf-8', method='html', pretty_print=True).decode();

class PerHostLock():
    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop();
        self.fut = self.loop.create_future();
        self.nTotalLimit = config.nFetchLimit or float('inf');
        assert self.nTotalLimit > 0;
        self.nPerHostLimit = config.nLimitPerHost or float('inf');
        assert self.nPerHostLimit > 0;
        self.mHost = {};
        self.nFree = self.nTotalLimit;
    def _getHost(self, sUrl):
        sHost = urllib.parse.urlsplit(sUrl).netloc;
        assert sHost;
        return sHost;
    async def acquire(self, sUrl=None):
        if (sUrl):
            sHost = self._getHost(sUrl);
        while True:
            if (self.nFree > 0):
                if (not sUrl or self.mHost.setdefault(sHost, self.nPerHostLimit) > 0):
                    break;
            await self.fut;
        if (sUrl):
            self.mHost[sHost] -= 1;
        self.nFree -= 1;
    def release(self, sUrl=None):
        self.nFree += 1;
        if (self.nFree > self.nTotalLimit):
            log.error('excessive release regarding total limit!');
        if (sUrl):
            sHost = self._getHost(sUrl);
            self.mHost[sHost] += 1;
            if (self.mHost[sHost] > self.nPerHostLimit):
                log.error('excessive release regarding "{}"!'.format(sHost));
        self.fut.set_result(None);
        self.fut = self.loop.create_future();
perHostLock = PerHostLock();

def getDefaultSession():
    global aiohttpSession;
    if (aiohttpSession is None or aiohttpSession.closed):
        initSession();
    return aiohttpSession;

def initSession():
    global aiohttpSession;
    if (aiohttpSession and not aiohttpSession.closed):
        log.warning('aiohttp session is already alive.');
        return False;
    else:
        mHeaders = {'User-Agent': UA};
        connector = aiohttp.TCPConnector(force_close=True, enable_cleanup_closed=True);
        aiohttpSession = aiohttp.ClientSession(connector=connector, headers=mHeaders, trust_env=True, read_timeout=config.nReadTimeout);

async def cleanup():
    global aiohttpSession;
    if (aiohttpSession):
        await aiohttpSession.close();

class Response():
    def __init__(self, sUrl, mHeaders=None, session=None):
        self.sUrl = sUrl;
        self.mHeaders = mHeaders;
        self.session = session or getDefaultSession();
        self.res = None;
        self._closed = False;
        self._started = False;
    def __del__(self):
        self.close();
    async def __aenter__(self):
        return await self._get();
    async def __aexit__(self, *arg, **karg):
        self.close();
    def close(self):
        if (self._closed):
            return False;
        else:
            if (self.res): self.res.release()
            perHostLock.release(self.sUrl);
            self._closed = True;
            return True;
    async def _get(self):
        if (self.res):
            return self.res;
        if (self._started or self._closed):
            raise RuntimeError;
        self._started = True;
        await perHostLock.acquire(self.sUrl);
        nCount = 0;
        while nCount < config.nRetryCount:
            try:
                assert self._closed is False;
                self.res = await self.session.get(self.sUrl, headers=self.mHeaders)
                self.res.raise_for_status();
                return self.res;
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                info = self.res.request_info if self.res else self.sUrl;
                log.warning('failed to get response from {}: {}'.format(info, e));
                if (nCount+1 >= config.nRetryCount):
                    self.close();
                    raise;
            except:
                self.close();
                raise;
            nCount += 1
            asyncio.sleep(3*nCount);
        
async def fetchBytes(sUrl, mHeaders=None, session=None):
    await perHostLock.acquire(sUrl);
    session = session or getDefaultSession();
    try:
        nCount = 0;
        while nCount < config.nRetryCount:
            try:
                async with session.get(sUrl, headers=mHeaders) as res:
                    res.raise_for_status();
                    return await res.read();
            except aiohttp.ClientError as e:
                info = res.request_info if locals().get('res') else sUrl;
                log.warning('failed to read response {}: {}'.format(res.request_info, e));
                error = e;
            except asyncio.TimeoutError as e:
                info = res.request_info if locals().get('res') else sUrl;
                log.warning('timeout when getting {}: {}'.format(info, e));
                error = e;
            #except CancelledError as e:
            #    log.warning('unexpected CancelledError when getting {}: {}'.format(sUrl, e));
            #    error = e;
            nCount += 1;
            asyncio.sleep(3*nCount);
        raise error;
    finally:
        perHostLock.release(sUrl);

async def fetchJson(sUrl, mHeaders=None, session=None, mAssert=None, isTypeCheck=True):
    if (mAssert):
        assert isinstance(mAssert, dict);
    await perHostLock.acquire(sUrl);
    session = session or getDefaultSession();
    try:
        nCount = 0;
        while nCount < config.nRetryCount:
            try:
                async with session.get(sUrl, headers=mHeaders) as res:
                    res.raise_for_status();
                    typeCheck = 'application/json' if isTypeCheck else None;
                    mData = await res.json(content_type=typeCheck);
                    if (mData is None):
                        raise TypeError;
                    if (mAssert):
                        for key, value in mAssert.items():
                            assert mData.get(key) == value;
                    return mData;
            except TypeError as e:
                info = res.request_info if locals().get('res') else sUrl;
                log.warning('got empty JSON response {}: {}'.format(info, e));
                error = e;
            except AssertionError as e:
                info = res.request_info if locals().get('res') else sUrl;
                sMsg = 'assertion about JSON not satisfied: "{}" = "{}" (got "{}") - {}'.format(key, value, mData.get(key), info);
                log.warning(sMsg);
                error = AssertionError(sMsg)
            except aiohttp.ClientError as e:
                info = res.request_info if locals().get('res') else sUrl;
                log.warning('failed to get JSON response {}: {}'.format(info, e));
                error = e;
            except asyncio.TimeoutError as e:
                info = res.request_info if locals().get('res') else sUrl;
                log.warning('timeout when getting JSON response {}: {}'.format(info, e));
                error = e;
            except json.JSONDecodeError as e:
                info = res.request_info if locals().get('res') else sUrl;
                log.warning('invalid JSON response {}: {}'.format(info, e));
                error = e;
            #except CancelledError as e:
            #    log.warning('unexpected CancelledError when getting {}: {}'.format(sUrl, e));
            #    error = e;
            nCount += 1;
            asyncio.sleep(3*nCount);
        raise error;
    finally:
        perHostLock.release(sUrl);

async def fetchStream(sUrl, sDir=None, sFile=None, mHeaders=None, isDuplicate=True, session=None):
    await perHostLock.acquire(sUrl);
    session = session or getDefaultSession();
    try:
        sDir = sDir or config.sFileDir or '.';
        os.makedirs(sDir, exist_ok=True);
        if (not sFile):
            aUrl = sUrl.split('/');
            sFile = aUrl[-1] or aUrl[-2] or 'untitled';
        sFile = re.sub(r'[\\/:*?<>"|\t]', '_', sFile);
        sPath = os.path.join(sDir, sFile);
        if (os.path.exists(sPath)):
            if (not isDuplicate):
                log.debug('{} already existed, noop'.format(sPath));
                return (False, sPath);
            else:
                while (os.path.exists(sPath)):
                    sPath = re.sub(r'(\.\w+)?$', r'_\1', sPath);
                log.warning('file already existed, renaming new one to {}.'.format(sPath));
        try:
            res = await session.get(sUrl, headers=mHeaders);
            res.raise_for_status();
            file = open(sPath, 'wb');
            nBuffer = 1024*1024;
            nSize = 0;
            log.debug('downloading from {} to "{}"'.format(sUrl, sPath));
            bData = await res.content.read(nBuffer);
            while bData:
                nSize += file.write(bData);
                bData = await res.content.read(nBuffer);
        except Exception as e:
            log.warning('failed to download file {}: {}'.format(res.request_info, e));
            if (locals().get('file')):
                file.close();
                try:
                    os.remove(sPath);
                except FileNotFoundError:
                    pass
                file = None;
            raise;
        else:
            log.debug('downloaded: "{}"'.format(sPath));
            return True, sPath;
        finally:
            if (locals().get('file')):
                file.close();
    finally:
        perHostLock.release(sUrl);

