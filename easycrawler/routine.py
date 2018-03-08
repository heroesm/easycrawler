from . import source

def tryGrab(sUrl, aMethodAndDict):
    sUrl = source.Url(sUrl);
    for method, mKeyArg in aMethodAndDict:
        try:
            return method(sUrl=sUrl, **mKeyArg);
        except source.UrlUnmatchError:
            continue;

def grab(sUrl):
    weiboSou = source.WeiboSource();
    tiebaSou = source.TiebaSource();
    aMethodAndDict = [
            (weiboSou.getUser, {}),
            (weiboSou.getPost, {}),
            (weiboSou.getWeibo, {}),
            (tiebaSou.getUser, {}),
            (tiebaSou.getPost, {}),
            (tiebaSou.getForum, {})
    ];
    return tryGrab(sUrl, aMethodAndDict);

