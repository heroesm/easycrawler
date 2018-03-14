import os
import logging

from .asset import discard
from .configure import config

import lxml.html

log = logging.getLogger(__name__);

class Record():
    __slots__ = ('__weakref__', 'sId', 'sType', 'sName', 'sUrl', '_aChildren', '_parent', 'sParentId', 'sText', 'sContent', 'fetchTime', 'aAttach', 'mProperties');
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
    def getAttrs(self):
        return {key: getattr(self, key, None) for key in self.__slots__}
    def updateAttrs(self, target):
        if (isinstance(target, type(self))):
            mTarget = target.getAttrs();
        else:
            mTarget = dict(target);
        for key in self.__slots__:
            setattr(self, key, mTarget.get(key) or getattr(self, key, None));
    def clear(self):
        for key in self.__slots__:
            setattr(self, key, None);
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
    __slots__ = ('aPosts', 'postIdSet');
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
    __slots__ = ('aPosts', 'sAvatar', 'sAvatarHd', 'sIntro', 'mProfile', 'sGender', 'birth', 'isAnonymous');
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
    __slots__ = ('sForumId', 'sForum', 'author', 'date', '_sAuthor', '_sAuthorId', 'nComments', 'aComments', 'sQuoteId', 'sQuote');
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

    def __init__(self, *arg, author=None, date=None, **kargs):
        super().__init__(*arg, **kargs);
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
    __slots__ = ('sPostId', 'aIndices');
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
    __slots__ = ('nFans', 'nFollow', 'sEntryCid', 'sProfileCid', 'sWeiboCid');
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
    __slots__ = ('nLike', 'sSource', 'sBid', 'aHotComments', 'repost', 'sThumb');
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
        self.sThumb = None;
    def revTrans(self, sKey, value):
        if (value is None):
            return None;
        elif (sKey == 'aComments'):
            return [WeiboComment(sId=sId) for sId in value];
        else:
            return super().revTrans(sKey, value);

class WeiboComment(CommentRecord):
    __slots__ = ('nLike', 'sSource');
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
    __slots__ = ('sPageId');
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
    __slots__ = ();
    mVarCast = {
            'sId': ('id', 'TEXT'),
            'sName': ('name', 'TEXT'),
            'sUrl': ('url', 'TEXT'),
            'fetchTime': ('fetchtime', 'TIMESTAMP')
    };
    def __init__(self, *arg, **kargs):
        super().__init__(*arg, **kargs);

class TiebaUser(UserRecord):
    __slots__ = ('sNickName');
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
    __slots__ = ();
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
    __slots__ = ('nComments', 'nPage');
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
    __slots__ = ('sPostId', 'sPreview');
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
    __slots__ = ();
    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs);
        self.sType = 'image';
