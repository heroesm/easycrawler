import logging
import queue
import threading
from collections import namedtuple

import psycopg2
import psycopg2.errorcodes
from psycopg2.sql import SQL, Identifier, Placeholder, Literal

from . import types
from .configure import config

Data = namedtuple('Data', ('field', 'type', 'value'));
log = logging.getLogger(__name__);

def prepare():
    global log;
    log.setLevel(config.nLogLevel);
prepare();

def makeData(record):
    mData = {};
    for (sAttrName, (sFieldName, sFieldType)) in record.mVarCast.items():
        value = record.varTrans(sAttrName);
        if (value is None or hasattr(value, '__len__') and len(value) == 0):
            continue;
        mData[sFieldName] = Data(sFieldName, sFieldType, value);
    return mData;

class Disposer():
    def __init__(self):
        self.queue = queue.Queue();
        self.thread = None;
        self.event = threading.Event();
        self.isRunning = False;
        self.isEndFed = False;
    def handleOneRecord(self, record):
        raise NotImplementedError;
    def prepare(self):
        log.debug('prepare disposer...');
        pass
    def cleanup(self):
        log.debug('cleanup disposer...');
        pass
    def feed(self, record):
        self.queue.put(record);
    def dispose(self):
        self.prepare();
        self.event.set();
        log.info('starting disposing');
        while self.isRunning:
            self.event.wait();
            try:
                record = self.queue.get();
                if (record is None):
                    continue;
                elif (record is False):
                    break;
                else:
                    self.handleOneRecord(record);
            finally:
                self.queue.task_done();
        self.cleanup();
        self.isRunning = False;
    def runForever(self):
        log.info('run until being stopped manually');
        self.isRunning = True;
        self.thread = threading.Thread(target=self.dispose, name='disposer');
        self.thread.start();
    def pause(self):
        self.event.clear();
        self.feed(None);
        log.debug('pause disposer...');
    def resume(self):
        self.event.set();
        log.debug('resume disposer...');
    def feedEnd(self):
        self.feed(False);
        self.isEndFed = True;
        log.debug('feed the end signal...');
    def stop(self):
        self.isRunning = False;
        self.feed(None);
        self.resume();
        log.debug('stop disposer...');
    def joinQueue(self):
        if (not self.isRunning or self.isEndFed):
            log.error('can not join queue of the disposer (running: {}; end fed: {})'.format(self.isRunning, self.isEndFed));
            return False;
        else:
            self.resume();
            log.debug('join input queue...');
            self.queue.join();
            return True;
    def join(self, nTimeout=None):
        self.resume();
        log.debug('join disposing thread...');
        if (self.thread):
            self.thread.join(timeout=nTimeout);
    def close(self):
        self.stop();
        self.join();
        self.cleanup();
    

class FileDisposer(Disposer):
    pass

class HtmlDisposer(Disposer):
    pass

class SqlDisposer(Disposer):
    pass

class SqliteDisposer(SqlDisposer):
    pass

class PostgresDisposer(SqlDisposer):
    def __init__(self, sDbName=None, sUser=None, sMisc=None, isSafe=False):
        super().__init__();
        self.sDbName = sDbName or config.sDbName;
        self.sUser = sUser or config.sDbUser;
        self.sMisc = sMisc or config.sDbOption;
        self.conn = None;
        self._lock = threading.Lock();
        self.isSafe = isSafe or False;
    def _execute(self, *arg, cursor=None, **karg):
        assert self.conn;
        if (not cursor or cursor.closed):
            cursor = self.conn.cursor();
            isClose = True;
        else:
            isClose = False;
        bQuery = cursor.mogrify(*arg, **karg);
        bQuery.replace(b'\0', b'');
        log.debug('execute: {}'.format(bQuery.decode()));
        cursor.execute(bQuery);
        if (isClose):
            cursor.close();
    def _connect(self, sDbName=None, sUser=None, sMisc=None):
        with self._lock:
            if (self.conn):
                log.debug('already connected, noop');
                return False;
            sDbName = sDbName or self.sDbName;
            sUser = sUser or self.sUser;
            sMisc = sMisc or self.sMisc or '';
            assert sDbName and sUser;
            self.conn = psycopg2.connect('dbname={} user={} {}'.format(sDbName, sUser, sMisc));
            log.info('connected to database "{}" as user "{}" with additional options "{}"'.format(sDbName, sUser, sMisc));
            return True;
    def _save(self, sPoint):
        if (self.isSafe):
            return False;
        query = SQL('SAVEPOINT {};').format(Identifier(sPoint));
        self._execute(query);
        #log.debug('savepoint created: "{}"'.format(sPoint));
        return True;
    def _back(self, sPoint):
        if (self.isSafe):
            return False;
        query = SQL('ROLLBACK TO {};').format(Identifier(sPoint));
        self._execute(query);
        #log.debug('savepoint rollbacked: "{}"'.format(sPoint));
        return True;
    def _release(self, sPoint):
        if (self.isSafe):
            return False;
        query = SQL('RELEASE SAVEPOINT {};').format(Identifier(sPoint));
        self._execute(query);
        #log.debug('savepoint released: "{}"'.format(sPoint));
        return True;
    def createTable(self, record, isRaise=True):
        assert self.conn;
        if (type(record) is type):
            record = record();
        assert isinstance(record, types.Record);
        sTable = type(record).__name__.lower();
        aFields = [SQL('{} {}').format(Identifier(sName), SQL(sType)) for sName, sType in sorted(record.mVarCast.values())];
        if (record.aPKs):
            aFields.append(SQL('PRIMARY KEY ({})').format(
                SQL(', ').join(Identifier(sPK) for sPK in record.aPKs)
            ));
        query = SQL('CREATE TABLE {} ({});').format(
                Identifier(sTable), 
                SQL(', ').join(aFields)
        );
        log.debug(query.as_string(self.conn));
        self._save('createtable');
        try:
            self._execute(query);
        except Exception as e:
            self._back('createtable');
            log.error(e);
            if (isRaise):
                raise;
            return False;
        else:
            log.info('"{}" table created'.format(sTable));
            return True;
        finally:
            self._release('createtable');
    def dropTable(self, record, isRaise=True):
        if (type(record) is type):
            record = record();
        assert isinstance(record, types.Record);
        sTable = type(record).__name__.lower();
        query = SQL('DROP TABLE {};').format(Identifier(sTable));
        self._save('droptable');
        try:
            self._execute(query);
        except Exception as e:
            self._back('droptable');
            log.error('can not drop not existing table {}'.format(sTable));
            if (isRaise):
                raise;
            return False;
        else:
            log.info('"{}" table dropped'.format(sTable));
            return True;
        finally:
            self._release('droptable');
    def upsertRecord(self, record):
        assert isinstance(record, types.Record);
        mData = makeData(record);
        table = Identifier(type(record).__name__.lower());
        aFields = [];
        mArgs = {};
        for sField, sType, value in mData.values():
            aFields.append(sField);
            mArgs[sField] = value;
        fields = SQL(', ' ).join(Identifier(sField) for sField in aFields);
        values = SQL(', ' ).join(Placeholder(sField) for sField in aFields);
        if (record.aPKs):
            action = (
                    SQL('DO UPDATE SET ({}) = ({})').format(fields, values)
                    if aFields
                    else SQL('DO NOTHING')
            )
            pks = SQL(', ').join(Identifier(sPK) for sPK in record.aPKs);
            conflict = SQL('ON CONFLICT ({}) {}').format(pks, action);
        else:
            conflict = SQL('');
        query = SQL('INSERT INTO {} ({}) VALUES ({}) {};').format(table, fields, values, conflict);
        self._execute(query, mArgs);
        log.debug('record "{}" upserted'.format(record));
    def fetchRecords(self, record, sId=None, mCondition=None):
        assert self.conn;
        if (isinstance(record, types.Record)):
            mReverse = record.mVarCastRev;
            cls = type(record);
            sId = sId or record.sId;
        else:
            assert type(record) is type;
            cls = record;
            mReverse = cls().mVarCastRev;
        assert sId or mCondition;
        aFields = tuple(mReverse.keys());
        fields = SQL(', ').join(Identifier(sField) for sField in aFields);
        aAttrs = tuple(mReverse.values());
        if (sId):
            condition = SQL('id = {}').format(Literal(sId));
        else:
            condition = SQL(' and ').join(SQL(
                '{} = {}'.format(Identifier(key), Literal(value))
                for key, value in mCondition.items()
            ))
        assert condition;
        table = Identifier(cls.__name__.lower());
        query = SQL('SELECT {} FROM {} WHERE ({});').format(fields, table, condition);
        with self.conn.cursor() as cursor:
            self._execute(query, cursor=cursor);
            aRawRecords = cursor.fetchall();
        aResult = [];
        for aRaw in aRawRecords:
            record = cls();
            for sAttr, value in zip(aAttrs, aRaw):
                if (value is not None):
                    value = record.revTrans(sAttr, value);
                    setattr(record, sAttr, value);
            aResult.append(record);
        return aResult;
    def updateRecord(self, record):
        mCondition = {
                sField: getattr(record, record.mVarCastRev[sField])
                for sField in record.aPKs
        };
        aResult = self.fetchRecords(record, mCondition=mCondition);
        if (aResult):
            assert len(aResult) == 1;
            record.__dict__.update(aResult[0].__dict__);
            return True;
        else:
            return False;
    def handleOneRecord(self, record):
        assert isinstance(record, types.Record);
        self._save('handleonerecord');
        try:
            self.upsertRecord(record);
            #log.debug('record "{}" handled'.format(record));
        except psycopg2.ProgrammingError as e:
            if (e.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE):
                self._back('handleonerecord');
                log.warning('table "{}" does not exist: {}'.format(type(record).__name__.lower(), e));
                self.createTable(record);
                self.upsertRecord(record);
            else:
                raise;
        finally:
            self._release('handleonerecord');
    def prepare(self):
        super().prepare();
        self._connect();
    def commit(self):
        self.conn.commit();
        log.info('transaction commited');
    def rollback(self):
        self.conn.rollback();
        log.info('transaction rollbacked');
    def cleanup(self):
        super().cleanup();
        with self._lock:
            if (self.conn):
                self.commit();
                self.conn.close();
                self.conn = None;
    def close(self):
        if (self.conn):
            self.rollback();
        super().close();
