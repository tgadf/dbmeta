""" Base Class For MetaData Creation """

__all__ = ["MetaProducerBase"]

from dbbase import MusicDBRootDataIO, getModVals
from utils import Timestat
from .metabasicprod import MetaBasicProducer


class MetaProducerBase:
    def __repr__(self):
        return f"MetaProducerBase(db={self.rdio.db})"
        
    def __init__(self, rdio: MusicDBRootDataIO, **kwargs):
        self.rdio = rdio
        self.verbose = kwargs.get('verbose', False)
        self.mediaRanking = {}
        self.dbmetas = {}
        
        self.procs = {}
        self.procs["Basic"] = MetaBasicProducer()
        
    ###########################################################################
    # Easy I/O
    ###########################################################################
    def isUpdateModVal(self, n):
        if self.verbose is False:
            return False
        retval = True if ((n + 1) % 25 == 0 or (n + 1) == 5) else False
        return retval
    
    def getMetaTypes(self, key) -> 'dict':
        assert isinstance(self.dbmetas, dict), f"dbmetas [{type(self.dbmetas)}] is not a dict"
        if isinstance(key, str):
            assert key in self.dbmetas.keys(), f"MetaType key [{key}] is not allowed"
            return {key: self.dbmetas[key]}
        return self.dbmetas
    
    ###########################################################################
    # General Make Runner
    ###########################################################################
    def make(self, modVal=None, key=None, **kwargs):
        verbose = kwargs.get("verbose", self.verbose)
        test = kwargs.get('test', False)
        modVals = getModVals(modVal) if test is False else [0]
        metaTypes = list(self.procs.keys())
            
        ts = Timestat(f"Making {len(modVals)} {metaTypes} MetaData Files", verbose=verbose)
        
        for n, modVal in enumerate(modVals):
            if self.isUpdateModVal(n):
                ts.update(n=n + 1, N=len(modVals))

            modValData = self.rdio.getModValData(modVal)
            for metaType, metaTypeProd in self.procs.items():
                if verbose:
                    print(f"    ModVal={modVal: <4} | MetaType={metaType} ... ", end="")
                    
                metaData = metaTypeProd.getMetaData(modValData)
                if test is True:
                    print("Only testing. Will not save.")
                    continue
                
                if verbose:
                    print(f"{metaData.shape} ... ", end="")
                self.rdio.saveData(f"Meta{metaType}", modVal, data=metaData)
                if verbose is True:
                    print("✓")
                        
        ts.stop()