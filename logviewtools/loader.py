from pathlib import Path
from pickle import load, dump


class PETScLogView(object):
    def __init__(self, filename):
        self.filename = Path(filename)
        self._dataframe = None
        cachedir = self.filename.with_name('.logview_cache')
        if not cachedir.exists():
            cachedir.mkdir()
        self.cachefile = (cachedir/self.filename.stem).with_suffix('.pkl')
        if self.cachefile.exists():
            self.readcache()
        else:
            self.readlogviewpy()

    def readlogviewpy(self):
        with open(self.filename) as fh:
            gvars = {}
            lvars = {}
            for line in fh.readlines():
                try:
                    exec(line, gvars, lvars)
                except KeyError as e:
                    # ~ print('K', end='')
                    try:
                        for key in lvars['Stages']:
                            lvars['Stages'][key][e.args[0]] = {}
                        exec(line, gvars, lvars)
                    except KeyError as e:
                        print('Skipping key', e, 'due to error')
        self.initialize(lvars)

    def initialize(self, variables):
        self.attr = ['size', 'LocalTimes', 'LocalMessages', \
                     'LocalMessageLens', 'LocalReductions', 'LocalFlop', \
                     'LocalObjects', 'LocalMemory', 'Stages']
        self.size = 0
        self.LocalTimes = {}
        self.LocalMessages = {}
        self.LocalMessageLens = {}
        self.LocalReductions = {}
        self.LocalFlop = {}
        self.LocalObjects = {}
        self.LocalMemory = {}
        self.Stages = {}
        self.update(variables)

        if not self.cachefile.exists():
            self.writecache()

    def update(self, variables):
        for key in self.attr:
            if key in variables:
                setattr(self, key, variables[key])

    def readcache(self):
        with open(self.cachefile, 'rb') as fh:
            self.__dict__.update(load(fh))

    def writecache(self):
        with open(self.cachefile, 'wb') as fh:
            dump(self.__dict__, fh)

    @property
    def dataframe(self):
        if self._dataframe is None:
            from .data import logview2frame
            self._dataframe = logview2frame(self)
            self.writecache()
        return self._dataframe



