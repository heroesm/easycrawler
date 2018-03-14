import os
import logging

from .asset import discard
from .configure import config

import lxml.html

log = logging.getLogger(__name__);

class Record():
    aPKs = ('id',);
    mVarCast = {
            'sId': ('id', 'TEXT'),
            'sType': ('type', 'TEXT'),
            'sName': ('name', 'TEXT'),
            'sUrl': ('url', 'TEXT'),
            'sParentId': ('parentid', 'TEXT'),
            'sText': ('text', 'TEXT'),
            'sContent': ('content', 'TEXT'),
            'fetchTime': ('fetchtime', 'TIMESTAMP'),
            'mProperties': ('property', 'JSONB')
    };
    @property
    def parent(self):
        return self._parent;
    @parent.setter
    def parent(self, value):
        if (value is None):
            del self.parent;
        else:
            assert isinstance(value, Record);
            if (self._parent != value):
                if (self._parent):
                    discard(self._parent._aChildren, self);
                value._aChildren.append(self);
                self._parent = value;
                self.sParentId = value.sId;
    @parent.deleter
    def parent(self):
        if (getattr(self, '_parent', None)):
            discard(self._parent._aChildren, self);
            self._parent = None;
            self.sParentId = None;
    @property
    def mVarCastRev(self):
        return {
                sFieldName: sAttrName
                for sAttrName, (sFieldName, sFieldType)
                in self.mVarCast.items()
        };

    def __init__(self, sType=None, sId=None, sName=None, sUrl=None):
        self.sId = sId;
        self.sType = sType;
        self.sName = sName;
        self.sUrl = sUrl;
        self._aChildren = [];
        #self.parent;
        self._parent = None;
        self.sParentId = None;
        self.sText = None;
        self.sContent = None;
        self.fetchTime = None;
        self.aAttach = [];
        self.mProperties = {};
        #self.mData = None;
    def __repr__(self):
        return '< record "{}": "{}"-"{}" >'.format(type(self).__name__ or '', self.sId or '', self.sName or '');
    def getAttr(self):
        return self.__dict__;
    def updateAttr(self, target):
        if (isinstance(target, type(self))):
            mTarget = target.__dict__;
        else:
            mTarget = dict(target);
        self.__dict__.update(mTarget);
    def clear(self):
        self.__dict__ = {};
    def htmlBytes(self):
        if (self.sContent):
            return lxml.etree.tostring(lxml.html.document_fromstring(self.sContent), method='html', encoding='utf-8');
    def save(self, sFile=None, isForce=False):
        if (not self.sContent):
            log.warning('no content available, noop');
        else:
            if (not sFile and self.sName):
                sFile = '{}.html'.format(self.sName);
                sFile = os.path.join(config.sDir, sFile);
            if (os.path.exists(sFile) and not isForce):
                raise FileExistsError;
            with open(sFile, 'wb') as f:
                f.write(self.htmlBytes());
    def varTrans(self, sKey):
        value = getattr(self, sKey);
        if (value is None):
            return None;
        elif (sKey == 'aAttach'):
            return ['{}-{}'.format(item.sType, item.sId) for item in value];
        else:
            return value;
    def revTrans(self, sKey, value):
        if (value is None):
            return None;
        elif (sKey == 'aAttach'):
            return [
                    (lambda a: Media(sType=a[0], sId=a[1]))(sSpecId.split('-', 1))
                    for sSpecId in value
            ];
        else:
            return value;
    
class ForumRecord(Record):
    mVarCast = {
            'sId': ('id', 'TEXT'),
            'sType': ('type', 'TEXT'),
            'sName': ('name', 'TEXT'),
            'sUrl': ('url', 'TEXT'),
            'sParentId': ('parentid', 'TEXT'),
            'sText': ('text', 'TEXT'),
            'sContent': ('content', 'TEXT'),
    };
    def __init__(self, *arg, **kargs):
        super().__init__(*arg, **kargs);
        self.aPosts = [];
        self.postIdSet = set();
    def allUser(self):
        result = set();
        for post in self.aPosts:
            for comment in post.aComments:
                result.add(comment.author);
        return result;

class UserRecord(Record):
    mVarCast = {
            'sId': ('id', 'TEXT'),
            'sType': ('type', 'TEXT'),
            'sName': ('name', 'TEXT'),
            'sUrl': ('url', 'TEXT'),
            'fetchTime': ('fetchtime', 'TIMESTAMP'),
            'birth': ('birth', 'TIMESTAMP'),
            'sAvatar': ('avatar', 'TEXT'),
            'sAvatarHd': ('avatarhd', 'TEXT'),
            'sIntro': ('intro', 'TEXT'),
            'sGender': ('gender', 'TEXT')
    };
    def __init__(self, *arg, **kargs):
        super().__init__(*arg, **kargs);
        self.aPosts = [];
        self.sAvatar = None;
        self.sAvatarHd = None;
        self.sIntro = None;
        self.mProfile = {};
        self.sGender = None;
        self.birth = None;
        self.isAnonymous = None;
    def varTrans(self, sKey):
        value = getattr(self, sKey);
        if (value is None):
            return None;
        elif (sKey == 'aPosts'):
            return [post.sId for post in value];
        else:
            return super().varTrans(sKey);
    def revTrans(self, sKey, value):
        if (value is None):
            return None;
        elif (sKey == 'aPosts'):
            return [PostRecord(sId=sId) for sId in value];
        else:
            return super().revTrans(sKey, value);

class PostRecord(Record):
    mVarCast = {
            'sId': ('id', 'TEXT'),
            'sType': ('type', 'TEXT'),
            'sName': ('title', 'TEXT'),
            'sUrl': ('url', 'TEXT'),
            'sParentId': ('parentid', 'TEXT'),
            'sText': ('text', 'TEXT'),
            'sContent': ('content', 'TEXT'),
            'fetchTime': ('fetchtime', 'TIMESTAMP'),
            'date': ('date', 'TIMESTAMP'),
            'sForumId': ('forumid', 'TEXT'),
            'sForum': ('forum', 'TEXT'),
            'sAuthorId': ('authorid', 'TEXT'),
            'sAuthor': ('author', 'TEXT'),
            'nComments': ('commentnum', 'INT'),
            'sQuoteId': ('quoteid', 'TEXT'),
            'sQuote': ('quote', 'TEXT')
    };
    @property
    def sAuthorId(self):
        return self.author and self.author.sId or self._sAuthorId or None;
    @sAuthorId.setter
    def sAuthorId(self, value):
        if (self.author and self.author.sId != str(value)):
            raise AttributeError('inconsistent sAuthorId');
        self._sAuthorId = value;
    @property
    def sAuthor(self):
        return self.author and self.author.sName or self._sAuthor or None;
    @sAuthor.setter
    def sAuthor(self, value):
        if (self.author and self.author.sName != str(value)):
            raise AttributeError('inconsistent sAuthor');
        else:
            self._sAuthor = value;
    @property
    def sTitle(self):
        return self.sName;
    @sTitle.setter
    def sTitle(self, value):
        self.sName = value;

    def __init__(self, *arg, sTitle=None, author=None, date=None, **kargs):
        super().__init__(*arg, **kargs);
        self.sTitle = sTitle or self.sName;
        self.sForumId = None;
        self.sForum = None;
        self.author = author;
        self.date = date;
        #self.sAuthor
        self._sAuthor = None;
        #self.sAuthorId
        self._sAuthorId = None;
        self.nComments = None;
        self.aComments = [];
        self.sQuoteId = None;
        self.sQuote = None;
    def __repr__(self):
        return '< record "{}": "{}"-"{}" >'.format(type(self).__name__ or '', self.sId or '', self.sTitle or '');
    def varTrans(self, sKey):
        value = getattr(self, sKey);
        if (value is None):
            return None;
        elif (sKey == 'aComments'):
            return [comment.sId for comment in value];
        else:
            return super().varTrans(sKey);
    def revTrans(self, sKey, value):
        if (value is None):
            return None;
        elif (sKey == 'aComments'):
            return [CommentRecord(sId=sId) for sId in value];
        else:
            return super().revTrans(sKey, value);

class CommentRecord(PostRecord):
    mVarCast = {
            'sId': ('id', 'TEXT'),
            'sType': ('type', 'TEXT'),
            'sName': ('title', 'TEXT'),
            'sUrl': ('url', 'TEXT'),
            'sParentId': ('parentid', 'TEXT'),
            'sText': ('text', 'TEXT'),
            'sContent': ('content', 'TEXT'),
            'fetchTime': ('fetchtime', 'TIMESTAMP'),
            'date': ('date', 'TIMESTAMP'),
            'sForumId': ('forumid', 'TEXT'),
            'sForum': ('forum', 'TEXT'),
            'sAuthorId': ('authorid', 'TEXT'),
            'sAuthor': ('author', 'TEXT'),
            'sQuoteId': ('quoteid', 'TEXT'),
            'sQuote': ('quote', 'TEXT'),
            'sPostId': ('postid', 'TEXT'),
            'aIndices': ('indices', 'INT[]')
    };
    def __init__(self, *arg, sPostId=None, **kargs):
        super().__init__(*arg, **kargs);
        self.sPostId = sPostId;
        self.aIndices = None;

class WeiboUser(UserRecord):
    mVarCast = {
            'sId': ('id', 'TEXT'),
            'sType': ('type', 'TEXT'),
            'sName': ('name', 'TEXT'),
            'sUrl': ('url', 'TEXT'),
            'fetchTime': ('fetchtime', 'TIMESTAMP'),
            'birth': ('birth', 'TIMESTAMP'),
            'sAvatar': ('avatar', 'TEXT'),
            'sAvatarHd': ('avatarhd', 'TEXT'),
            'sIntro': ('intro', 'TEXT'),
            'sGender': ('gender', 'TEXT'),
            'nFans': ('fannum', 'INT'),
            'nFollow': ('follownum', 'INT'),
            'sEntryCid': ('entrycid', 'TEXT'),
            'sProfileCid': ('profilecid', 'TEXT'),
            'sWeiboCid': ('weibocid', 'TEXT'),
            'mProperties': ('property', 'JSONB')

    };
    def __init__(self, *arg, **kargs):
        super().__init__(*arg, **kargs);
        self.nFans = None;
        self.nFollow = None;
        self.sEntryCid = None;
        self.sProfileCid = None;
        self.sWeiboCid = None;
    def revTrans(self, sKey, value):
        if (value is None):
            return None;
        elif (sKey == 'aPosts'):
            return [WeiboPost(sId=sId) for sId in value];
        else:
            return super().revTrans(sKey, value);

class WeiboPost(PostRecord):
    mVarCast = {
            'sId': ('id', 'TEXT'),
            'sType': ('type', 'TEXT'),
            'sUrl': ('url', 'TEXT'),
            'sText': ('text', 'TEXT'),
            'sContent': ('content', 'TEXT'),
            'fetchTime': ('fetchtime', 'TIMESTAMP'),
            'date': ('date', 'TIMESTAMP'),
            'sAuthorId': ('authorid', 'TEXT'),
            'sAuthor': ('author', 'TEXT'),
            'nComments': ('commentnum', 'INT'),
            'sQuoteId': ('quoteid', 'TEXT'),
            'sQuote': ('quote', 'TEXT'),
            'nLike': ('likenum', 'INT'),
            'sSource': ('source', 'TEXT'),
            'sBid': ('bid', 'TEXT')
    };
    def __init__(self, *arg, **kargs):
        super().__init__(*arg, **kargs);
        self.nLike = None;
        self.sSource = None;
        self.sBid = None;
        self.aHotComments = [];
        self.repost = None;
    def revTrans(self, sKey, value):
        if (value is None):
            return None;
        elif (sKey == 'aComments'):
            return [WeiboComment(sId=sId) for sId in value];
        else:
            return super().revTrans(sKey, value);

class WeiboComment(CommentRecord):
    mVarCast = {
            'sId': ('id', 'TEXT'),
            'sType': ('type', 'TEXT'),
            'sUrl': ('url', 'TEXT'),
            'sText': ('text', 'TEXT'),
            'sContent': ('content', 'TEXT'),
            'fetchTime': ('fetchtime', 'TIMESTAMP'),
            'date': ('date', 'TIMESTAMP'),
            'sAuthorId': ('authorid', 'TEXT'),
            'sAuthor': ('author', 'TEXT'),
            'sQuoteId': ('quoteid', 'TEXT'),
            'sQuote': ('quote', 'TEXT'),
            'sPostId': ('postid', 'TEXT'),
            'nLike': ('likenum', 'INT'),
            'sSource': ('source', 'TEXT')
    };
    def __init__(self, *arg, **kargs):
        super().__init__(*arg, **kargs);
        self.nLike = None;
        self.sSource = None;

class WeiboArticle(WeiboPost):
    mVarCast = {
            'sId': ('id', 'TEXT'),
            'sType': ('type', 'TEXT'),
            'sUrl': ('url', 'TEXT'),
            'sText': ('text', 'TEXT'),
            'sContent': ('content', 'TEXT'),
            'fetchTime': ('fetchtime', 'TIMESTAMP'),
            'date': ('date', 'TIMESTAMP'),
            'sAuthorId': ('authorid', 'TEXT'),
            'sAuthor': ('author', 'TEXT'),
            'nComments': ('commentnum', 'INT'),
            'nLike': ('likenum', 'INT'),
            'sSource': ('source', 'TEXT'),
            'sPageId': ('pageid', 'TEXT')
    };
    def __init__(self, *arg, **kargs):
        super().__init__(*arg, **kargs);
        self.sPageId = None;

class TiebaForum(ForumRecord):
    mVarCast = {
            'sId': ('id', 'TEXT'),
            'sName': ('name', 'TEXT'),
            'sUrl': ('url', 'TEXT'),
            'fetchTime': ('fetchtime', 'TIMESTAMP')
    };
    def __init__(self, *arg, **kargs):
        super().__init__(*arg, **kargs);

class TiebaUser(UserRecord):
    aPKs = (); # anonymous user with only IP subnet as user name have no id nor name
    mVarCast = {
            'sId': ('id', 'TEXT'),
            'sName': ('name', 'TEXT'),
            'sUrl': ('url', 'TEXT'),
            'fetchTime': ('fetchtime', 'TIMESTAMP'),
            'birth': ('birth', 'TIMESTAMP'),
            'sAvatar': ('avatar', 'TEXT'),
            'sIntro': ('intro', 'TEXT'),
            'sGender': ('gender', 'TEXT'),
            'sNickName': ('nickname', 'TEXT')
    };
    def __init__(self, *arg, **kargs):
        super().__init__(*arg, **kargs);
        self.sNickName = None;

class TiebaPost(PostRecord):
    mVarCast = {
            'sId': ('id', 'TEXT'),
            'sType': ('type', 'TEXT'),
            'sName': ('title', 'TEXT'),
            'sUrl': ('url', 'TEXT'),
            'sText': ('text', 'TEXT'),
            'sContent': ('content', 'TEXT'),
            'fetchTime': ('fetchtime', 'TIMESTAMP'),
            'date': ('date', 'TIMESTAMP'),
            'sForumId': ('forumid', 'TEXT'),
            'sForum': ('forum', 'TEXT'),
            'sAuthorId': ('authorid', 'TEXT'),
            'sAuthor': ('author', 'TEXT'),
            'nComments': ('commentnum', 'INT'),
    };
    def __init__(self, *arg, **kargs):
        super().__init__(*arg, **kargs);

class TiebaComment(CommentRecord):
    mVarCast = {
            'sId': ('id', 'TEXT'),
            'sType': ('type', 'TEXT'),
            'sName': ('title', 'TEXT'),
            'sUrl': ('url', 'TEXT'),
            'sParentId': ('parentid', 'TEXT'),
            'sText': ('text', 'TEXT'),
            'sContent': ('content', 'TEXT'),
            'fetchTime': ('fetchtime', 'TIMESTAMP'),
            'date': ('date', 'TIMESTAMP'),
            'sForumId': ('forumid', 'TEXT'),
            'sForum': ('forum', 'TEXT'),
            'sAuthorId': ('authorid', 'TEXT'),
            'sAuthor': ('author', 'TEXT'),
            'sPostId': ('postid', 'TEXT'),
            'aIndices': ('indices', 'INT[]')
    };
    def __init__(self, *arg, **kargs):
        super().__init__(*arg, **kargs);
        self.nComments = None; # only meaningful for the 1st floor
        self.nPage = None;

class Media(PostRecord):
    aPKs = ('id', 'type');
    mVarCast = {
            'sId': ('id', 'TEXT'),
            'sType': ('type', 'TEXT'),
            'sPostId': ('postid', 'TEXT'),
            'sUrl': ('url', 'TEXT'),
            'sText': ('text', 'TEXT'),
            'fetchTime': ('fetchtime', 'TIMESTAMP'),
            'date': ('date', 'TIMESTAMP'),
            'sAuthorId': ('authorid', 'TEXT'),
            'sAuthor': ('author', 'TEXT'),
            'sPreview': ('preview', 'TEXT')
    };
    def __init__(self, sType=None, sPostId=None, sPreview=None, **karg):
        super().__init__(**karg);
        self.sType = sType or 'media';
        self.sPostId = sPostId;
        self.sPreview = sPreview;
    def __repr__(self):
        return '< media "{}": "{}"-"{}" >'.format(type(self).__name__, self.sType, self.sName);
    def __eq__(self, media):
        return self.sId == media.sId;

class Image(Media):
    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs);
        self.sType = 'image';
