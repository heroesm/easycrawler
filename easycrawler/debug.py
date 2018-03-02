import gc
import weakref
from pprint import pprint
import tracemalloc
import pdb

class Stater():
    def __init__(self):
        self.aSnapshot = [];
    def snap(self):
        wd = weakref.WeakKeyDictionary();
        for obj in gc.get_objects():
            wd[type(obj)] = wd.setdefault(type(obj), 0) + 1 

        self.aSnapshot.append(wd);
        return wd;
    def output(self, wd=None, nLimit=10):
        wd = wd or self.aSnapshot[-1];
        nLimit = nLimit or None;
        pprint(sorted(wd.items(), key=lambda item: item[1], reverse=True)[:nLimit]);
    def diff(self, wd0=None, wd1=None, nLimit=10):
        wd0 = wd0 or self.aSnapshot[0];
        wd1 = wd1 or self.aSnapshot[-1];
        nLimit = nLimit or None;
        wdiff = weakref.WeakKeyDictionary({
                key: wd1.get(key, 0) - wd0.get(key, 0)
                for key in set(wd0.keys()).union(set(wd1.keys()))
        });
        print('diff:');
        pprint(sorted(wdiff.items(), key=lambda item: item[1], reverse=True)[:nLimit]);
        return wdiff;

def statObj(func):
    def wrapped(*arg, **karg):
        wd0 = weakref.WeakKeyDictionary();
        for obj in gc.get_objects():
            wd0[type(obj)] = wd0.setdefault(type(obj), 0) + 1 
        print('initial state:');
        pprint(sorted(wd0.items(), key=lambda item: item[1], reverse=True)[:10]);

        result = func(*arg, **karg);

        wd1 = weakref.WeakKeyDictionary();
        for obj in gc.get_objects():
            wd1[type(obj)] = wd1.setdefault(type(obj), 0) + 1 
        print('final state:');
        pprint(sorted(wd1.items(), key=lambda item: item[1], reverse=True)[:10]);
        wdiff = weakref.WeakKeyDictionary({
                key: wd1.get(key, 0) - wd0.get(key, 0)
                for key in set(wd0.keys()).union(set(wd1.keys()))
        });
        print('diff:');
        pprint(sorted(wdiff.items(), key=lambda item: item[1], reverse=True)[:10]);
        return result;
    return wrapped;

def traceMemory(func):
    def wrapped(*arg, **karg):
        _startTrace();

        result = func(*arg, **karg);

        _OutputTrace();
        return result;
    return wrapped;

def _startTrace(nStack=2):
    nStack = nStack or 2;
    tracemalloc.start(nStack)

def _OutputTrace():
    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics('lineno')
    print("[ Top 10 ]")
    for stat in top_stats[:10]:
        print(stat)
    top_stats = snapshot.statistics('traceback')
    print("[ Top 10 ]")
    for stat in top_stats[:10]:
        print(stat)
        print("%s memory blocks: %.1f KiB" % (stat.count, stat.size / 1024))
        for line in stat.traceback.format():
            print(line)
    print(gc.garbage);

def setTrace():
    pdb.set_trace();

def pm():
    pdb.post_mortem();
