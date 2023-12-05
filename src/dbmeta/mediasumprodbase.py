""" Media Summary Producer Base """

__all__ = ["MediaSummaryProducerBase"]

from dbbase import MusicDBRootDataIO, getModVals
from utils import Timestat
from .mediasumfileio import MediaSummaryFileIO


class MediaSummaryProducerBase:
    def __repr__(self):
        return f"MediaSummaryProducerBase(db={self.db})"
    
    def __init__(self, rdio: MusicDBRootDataIO, mediaSummary: dict, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        self.maxMedia = kwargs.get('maxMedia', None)
        self.msfio = MediaSummaryFileIO(rdio, mediaSummary, **kwargs)
        self.summaryTypeInfo = {key: val for key, val in mediaSummary.items() if key not in ["Artist", "Media"]}
        self.rdio = rdio
        self.db = rdio.db
        self.joinedStatus = {}
        self.procs = {}
        self.splitMediaData = None
        
    ###########################################################################
    # Runner
    ###########################################################################
    def make(self, key=None, **kwargs):
        self.verbose = kwargs.get('verbose', True)
        self.test = kwargs.get('test', False)
        self.modVals = getModVals() if self.test is False else [0]
        keys = list(self.procs.keys())
        if isinstance(key, str):
            assert key in keys, f"key={key} is not available in {keys}"
            keys = [key]
            
        ts = Timestat("Loading Media Data")
        self.msfio.setMediaData(verbose=self.verbose, test=self.test)
        self.msfio.setArtistMediaData(verbose=self.verbose, test=self.test)
        for mediaName, mediaData in self.msfio.mediaData.items():
            print(mediaName, mediaData.columns)
        for mediaNames, cols in self.msfio.mediaConcats.items():
            self.msfio.concatMediaData(mediaNames, cols, verbose=self.verbose, test=self.test)
        joinPairs = {col: colPairs for col, colPairs in self.msfio.colPairings.items() if len(colPairs) == 2}
        for col, colPairs in joinPairs.items():
            self.msfio.joinMediaData(colPairs[0], colPairs[1], verbose=self.verbose)
        ts.stop()
        
        if key in keys:
            proc = self.procs[key].getMediaSummaryData()
        else:
            for key, proc in self.procs.items():
                proc.getMediaSummaryData()
    
    ###########################################################################
    # Easy I/O
    ###########################################################################
    def isUpdateModVal(self, n: int) -> 'bool':
        if self.verbose is False:
            return False
        retval = True if ((n + 1) % 25 == 0 or (n + 1) == 5) else False
        return retval
    
    def setModVals(self, modVals) -> 'None':
        assert isinstance(modVals, (list, range)), "ModVals must be a list or range"
        self.modVals = modVals