""" MusicDB Match Data Producer"""

__all__ = ["MatchProducerIO", "MusicDBIgnoreData"]

from dbmaster import MasterParams, MasterPersist
from dbbase import MusicDBRootDataIO
from utils import Timestat, FileIO, header
from pandas import Series, DataFrame
from .prodbase import MatchProducerBase, MatchOmitBase


###############################################################################
# Producer For Match Data
###############################################################################
class MatchProducerIO(MatchProducerBase):
    def __repr__(self):
        return f"MatchProducerIO(db={self.db})"
    
    def __init__(self, rdio: MusicDBRootDataIO, omit: MatchOmitBase, **kwargs):
        super().__init__(rdio, **kwargs)
        self.verbose = kwargs.get('verbose', False)
        self.minMedia = 1
        self.omit = omit
        
        if self.verbose is True:
            print(self.__repr__())
    
    def makeMatchable(self, value) -> 'Series':
        return self.mns.update(value)
    
    def testDuplicated(self, df: DataFrame) -> 'None':
        assert isinstance(df, (Series, DataFrame)), f"df [{type(df)}] is not a Series/DataFrame"
        dupls = df[df.index.duplicated()]
        if dupls.shape[0] > 0:
            raise ValueError(f"Found duplicated indices in metadata: {dupls.index}")
        
    ###########################################################################
    # Artist ID => Name/URL Map
    ###########################################################################
    def make(self, **kwargs):
        verbose = kwargs.get('verbose', True)
        test = kwargs.get('test', False)
        ts = Timestat(f"Making {self.db} Match Data (Name>=1 & Media>={self.minMedia})", verbose)

        if verbose:
            print("  ==> Loading Name and Counts Data ... ", end="")
        artistNameData = self.rdio.getData("SummaryName")
        artistCountsData = self.rdio.getData("SummaryNumAlbums")
        if verbose:
            print("Done")
            
        if verbose:
            print(f"  ==> Testing For Duplicate Artist Indices ({artistNameData.shape[0]}) ... ", end="")
        self.testDuplicated(artistNameData)
        if verbose:
            print("Done")
            
        if verbose:
            print(f"  ==> Getting Matchable Data ({artistNameData.shape[0]}) ... ", end="")
        countsReq = artistCountsData >= self.minMedia
        nameReq = artistNameData.apply(lambda value: (isinstance(value, str) and len(value) > 0))
        matchableResults = (countsReq & nameReq)
        if verbose:
            print("Done")
            print(f"  ==> Matchable Data ({artistNameData.shape[0]}) ==> ({matchableResults.sum()})")
                
        for key in self.omit.matchTypes:
            if verbose:
                print(f"  ==> Loading {key} ... ", end="")
            summaryData = self.rdio.getData(f"Summary{key}")
            if not isinstance(summaryData, (DataFrame, Series)):
                print("No Data")
                continue
                
            dtype = "Name" if key == "Name" else "Media"
            try:
                matchableData = summaryData.loc[matchableResults]
            except Exception as error:
                raise ValueError(f"No data for {key} using results {len(matchableResults)} ({matchableResults.head()}): {error}")
            if verbose:
                print("Transforming Data ... ", end="")
            matchData = self.mns.update(matchableData, dtype=dtype, verbose=False)
            matchData = matchData[matchData.index.map(self.omit.isValid)]
            matchData.name = key
            if verbose:
                print("")
            
            if verbose is True:
                print(f"  ==> Created {matchData.shape[0]} Artist ID => {key} Match Data")

            if test is True:
                print("  ==> Only testing. Will not save.")
            else:
                if len(matchData) > 0:
                    self.rdio.saveData(f"Match{key}", data=matchData)
        
        ts.stop()
            
            
###############################################################################
# Master Ignore Data
###############################################################################
class MusicDBIgnoreData:
    def __init__(self, **kwargs):
        mdbpd = MasterPersist()
        self.data = {}
        self.verbose = kwargs.get('debug', kwargs.get('verbose', False))
        if self.verbose:
            print("MusicDBIgnoreData():")
            print("  ==> Match Dir: {0}".format(mdbpd.getMetaPath().str))
                                   
        # Sort By DB
        self.mdbdata = MusicDBData(path=MusicDBDir(mdbpd.getMetaPath()), fname="manualIgnoreDBIDs", ext=".yaml")
        ignoreData = self.mdbdata.get()
        self.mp = MasterParams()
        ignoreDBIDData = {db: {} for db in self.mp.getDBs()}

        for gType, gData in ignoreData["General"].items():
            if self.verbose:
                print("    ==> {0}: {1}".format(gType, len(gData)))
            for db, dbID in gData.items():
                assert self.mp.isValid(db), f"There is a non valid DB [{db}] in the ignore DBIDs file"
                ignoreDBIDData[db][dbID] = True
        for db, dbData in ignoreData["Specific"].items():
            assert self.mp.isValid(db), f"There is a non valid DB [{db}] in the ignore DBIDs file"
            if self.verbose:
                print(f"    ==> {db}: {len(dbData)}")
            for artistName, dbID in dbData.items():
                if self.verbose:
                    print(f"      ==> {artistName}: {dbID}")
                ignoreDBIDData[db][dbID] = True
        self.ignoreDBIDData = ignoreDBIDData
        
    def copyLocal(self):
        io = FileIO()
        localName = self.mdbdata.getFilename().name
        print("Saving a local copy of master data to [{0}] (Relative to calling notebook...)".format(localName))
        io.save(idata=self.mdbdata.get(), ifile=localName)
                
    def getData(self):
        return self.ignoreDBIDData
    
    def getDBData(self, db):
        assert self.mp.isValid(db), "Must provide a valid DB"
        return self.ignoreDBIDData.get(db, [])
    
    def isValid(self, db, dbID):
        dbData = self.getDBData(db)
        retval = dbData.get(dbID) is False
        return retval
    