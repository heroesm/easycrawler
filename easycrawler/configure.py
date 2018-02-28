import logging
import os
import sys

LIMITPERHOST = None;
RETRYCOUNT = 9;
READTIMEOUT = 90;
FETCHLIMIT = 20;
SUPPRESSFAILURE = False;
DBNAME = 'test';
DBUSER = 'postgres';
DBOPTION = '';
FILEDIR = os.path.expanduser('~/Downloads/deposit');
LOGLEVEL = logging.DEBUG;
#LOGLEVEL = logging.INFO;

class Config():
    sUserDir = os.path.expanduser('~');
    sDir = os.path.split(__file__)[0];
    nLogLevel = LOGLEVEL or 0;
    nDisplayLevel = 0;
    sHome = os.path.expanduser('~');
    nLimitPerHost = LIMITPERHOST or None;
    nRetryCount = RETRYCOUNT or 1;
    nReadTimeout = READTIMEOUT if READTIMEOUT is not None else 300;
    nFetchLimit = FETCHLIMIT or None;
    isSupressFailure = SUPPRESSFAILURE or False;
    sDbName = DBNAME or 'test';
    sDbUser = DBUSER or 'postgres';
    sDbOption = DBOPTION or '';
    sFileDir = FILEDIR or os.path.join(sDir, 'downloaded');
    log = None;
    def __init__(self):
        self.__dict__.update({
            key: value
            for (key, value) in type(self).__dict__.items()
            if not key.startswith('__') and not hasattr(value, '__call__')
        });
        self.reload();
    def reload(self):
        self.log = log = logging.getLogger(__package__);
        handler = logging.StreamHandler(sys.stdout);
        formatter = logging.Formatter(
                fmt='    %(asctime)s %(levelname)-7s :%(filename)-10s:%(lineno)-3d:\t%(message)s',
                datefmt='%m%d-%H:%M:%S'
        );
        handler.setFormatter(formatter);
        handler.setLevel(self.nDisplayLevel);
        #log.addHandler(handler);
        fileHandler = logging.FileHandler(os.path.join(self.sDir, 'debug.log'));
        fileHandler.setFormatter(formatter);
        fileHandler.setLevel(logging.DEBUG);
        #log.addHandler(fileHandler);
        log.handlers = [handler, fileHandler];

config = Config();
