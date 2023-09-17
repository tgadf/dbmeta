""" Media Summary Producer Base """

__all__ = ["MediaSummaryProducerBase"]

from dbmaster import MasterMetas
from dbbase import MusicDBRootDataIO
from dbbase import getModVals
from utils import Timestat
from pandas import DataFrame, Series, concat


class MediaSummaryProducerBase:
    def __repr__(self):
        return f"MediaSummaryProducerBase(db={self.db})"
    
    def __init__(self, rdio: MusicDBRootDataIO, **kwargs):
        self.rdio = rdio
        self.db = rdio.db
        self.verbose = kwargs.get('verbose', False)
        self.maxMedia = kwargs.get('maxMedia', None)
        self.summaryTypes = MasterMetas().getSummaryTypes()
        
        self.mediaInfo = {}
        self.mediaData = {}
        self.artistIDPos = None
        self.artistMediaInfo = {}
        self.artistMediaData = {}
        self.joinedStatus = {}
                
        self.procs = {}
        
    ###########################################################################
    # Runner
    ###########################################################################
    def make(self, key=None, **kwargs):
        self.verbose = kwargs.get('verbose', True)
        self.test = kwargs.get('test', False)
        self.modVals = getModVals() if self.test is False else [0]
        if key in self.procs.keys():
            proc = self.procs[key]
            proc()
        else:
            for key, proc in self.procs.items():
                proc()
    
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
    
    def addMediaInfo(self, mediaName: str, mediaColNames) -> "None":
        self.mediaInfo[mediaName] = mediaColNames
        self.mediaData[mediaName] = None
        
    def addArtistMediaInfo(self, artistMediaName: str, artistMediaColNames) -> "None":
        self.artistMediaInfo[artistMediaName] = artistMediaColNames
        self.artistMediaData[artistMediaName] = None
        self.joinedStatus[artistMediaName] = False
        
    def setArtistIDPos(self, pos: int) -> 'None':
        assert isinstance(pos, int), f"pos [{pos}] is not an int"
        self.artistIDPos = pos
        
    ###########################################################################
    # Set Media Data
    ###########################################################################
    def setMediaData(self) -> 'None':
        mediaInfo = self.mediaInfo
        assert isinstance(mediaInfo, dict), f"MediaInfo [{type(mediaInfo)}] is not a dict"
        if all([isinstance(value, DataFrame) for value in self.mediaData.values()]):
            return
        
        def getMediaModValData(mediaName: str, modVal: int, mediaColNames: list) -> 'DataFrame':
            assert isinstance(mediaName, str), f"MediaName [{mediaName}] is not a string key"
            assert isinstance(mediaColNames, list), f"MediaColNames [{mediaColNames}] is not a list"
            data = self.rdio.getData(f"ModVal{mediaName}", modVal)
            assert isinstance(data, DataFrame), f"ModVal{mediaName} Data is not a DataFrame"
            
            for colName in mediaColNames:
                assert colName in data.columns, f"Column [{colName}] not in DataFrame [{data.columns}]"
            retval = data[mediaColNames]
            return retval

        for mediaName, mediaColNames in mediaInfo.items():
            ts = Timestat(f"Getting {mediaName} Data For {len(self.modVals)} ModVals With Columns {mediaColNames}", verbose=self.verbose)
            self.mediaData[mediaName] = concat([getMediaModValData(mediaName, modVal, mediaColNames) for modVal in self.modVals])
            assert isinstance(self.mediaData[mediaName], DataFrame), f"MediaData With Name [{mediaName}] is a {type(self.mediaData[mediaName])} and not a DataFrame"
            ts.stop()
                
    ###########################################################################
    # Set Artist Media Data
    ###########################################################################
    def setArtistMediaData(self) -> 'None':
        artistMediaInfo = self.artistMediaInfo
        artistIDPos = self.artistIDPos
        assert isinstance(artistMediaInfo, dict), f"ArtistMediaInfo [{type(artistMediaInfo)}] is not a dict"
        assert isinstance(artistIDPos, (int, str)), f"ArtistIDPos [{type(artistIDPos)}] is not an int or colname"
        if all([isinstance(value, DataFrame) for value in self.artistMediaData.values()]):
            return

        for artistMediaName, artistMediaColNames in artistMediaInfo.items():
            ts = Timestat(f"Getting {artistMediaName} Data For {len(self.modVals)} ModVals With Columns {artistMediaColNames}", verbose=self.verbose)
            assert isinstance(artistMediaName, str), f"ArtistMediaName [{artistMediaName}] is not a string key"
            assert isinstance(artistMediaColNames, list), f"ArtistMediaColNames [{artistMediaColNames}] is not a list"
            for n, modVal in enumerate(self.modVals):
                if self.isUpdateModVal(modVal):
                    ts.update(n=n + 1, N=len(self.modVals))
                artistMediaModValData = self.rdio.getData(f"ModVal{artistMediaName}", modVal)[artistMediaColNames]
                if isinstance(artistIDPos, int):
                    artistMediaModValData.index = artistMediaModValData['mediaID'].apply(lambda mediaID: mediaID.split('-')[-1] if isinstance(mediaID, str) else None)
                if isinstance(self.artistMediaData[artistMediaName], DataFrame):
                    self.artistMediaData[artistMediaName] = concat([self.artistMediaData[artistMediaName], artistMediaModValData])
                else:
                    self.artistMediaData[artistMediaName] = artistMediaModValData
            
            self.artistMediaData[artistMediaName].index.name = None
            if isinstance(artistIDPos, int):
                self.artistMediaData[artistMediaName]["ArtistID"] = self.artistMediaData[artistMediaName]["mediaID"].apply(lambda mediaID: mediaID.split("-")[artistIDPos] if isinstance(mediaID, str) else None)
            elif isinstance(artistIDPos, str):
                assert artistIDPos in self.artistMediaData[artistMediaName].columns, f"No column [{artistIDPos}] in {self.artistMediaData[artistMediaName].columns}"
                self.artistMediaData[artistMediaName] = self.artistMediaData[artistMediaName].rename(columns={artistIDPos: "ArtistID"})
                
            if "mediaID" in self.artistMediaData[artistMediaName].columns:
                self.artistMediaData[artistMediaName] = self.artistMediaData[artistMediaName].drop(["mediaID"], axis=1)
            ts.stop()
                                                               
    ###########################################################################
    # Join Artist Media <=> Media
    ###########################################################################
    def joinArtistMediaData(self, artistMediaName: str, mediaName: str) -> 'None':
        if not isinstance(mediaName, str):
            return
        assert all([isinstance(value, str) for value in [artistMediaName, mediaName]]), f"ArtistMediaName [{artistMediaName}] or MediaName [{mediaName}] are not strs"
        assert isinstance(self.artistMediaData.get(artistMediaName), DataFrame), f"ArtistMediaData [{type(self.artistMediaData.get(artistMediaName))}] is not a DataFrame"
        assert isinstance(self.mediaData.get(mediaName), DataFrame), f"MediaData [{type(self.mediaData.get(mediaName))}] is not a DataFrame"
        if self.joinedStatus[artistMediaName] is True:
            return
        
        ts = Timestat(f"Joining Artist [{artistMediaName}] Data [{self.artistMediaData[artistMediaName].shape[0]}] With [{mediaName}] Data [{self.mediaData[mediaName].shape[0]}]", verbose=self.verbose)
        self.artistMediaData[artistMediaName] = self.artistMediaData[artistMediaName].join(self.mediaData[mediaName], rsuffix='Media')
        self.artistMediaData[artistMediaName] = self.artistMediaData[artistMediaName].reset_index(drop=True)
        self.joinedStatus[artistMediaName] = True
        ts.stop()
              
    ###########################################################################
    # Join Artist Media <=> Media
    ###########################################################################
    def getJoinedMediaData(self, artistMediaName: str, mediaName: str, colNames) -> 'DataFrame':
        assert isinstance(colNames, list), "ColNames must be a list"
        if "ArtistID" not in colNames:
            colNames.insert(0, "ArtistID")
        self.setMediaData()
        self.setArtistMediaData()
        self.joinArtistMediaData(artistMediaName, mediaName)
        df = self.artistMediaData[artistMediaName][colNames]
        return df
              
    ###########################################################################
    # Unjoined Media Data
    ###########################################################################
    def getMediaData(self, mediaName: str) -> 'DataFrame':
        self.setMediaData()
        df = self.mediaData[mediaName]
        return df
              
    ###########################################################################
    # Join With Existing Summary Data
    ###########################################################################
    def joinSummaryData(self, summaryType: str, summaryData: DataFrame, ts: Timestat, saveit=True):
        assert isinstance(summaryType, str), f"summaryType [{summaryType}] is not a str"
        assert isinstance(summaryData, (DataFrame, Series)), f"summaryData [{type(summaryData)}] is not a DataFrame/Series"
        existingData = self.rdio.getData(f"Summary{summaryType}")
        createData = True if not isinstance(existingData, DataFrame) else False
        
        if createData is True:
            basicData = self.rdio.getData("SummaryNumAlbums")
            ts.update(cmt=f"  Joining [{summaryData.shape[0]}] {summaryType} Data With All [{basicData.shape[0]}] Summary IDs  ... ")
            summaryData = DataFrame(basicData).join(summaryData).drop(["NumAlbums"], axis=1)
            del basicData
        else:
            ts.update(cmt=f"  Joining [{summaryData.shape[0]}] {summaryType} Data With Existing [{existingData.shape[0]}] Summary IDs  ... ")
            colNames = summaryData.columns if isinstance(summaryData, DataFrame) else [summaryData.name]
            colNames = [colName for colName in colNames if colName in existingData.columns]
            existingData = existingData.drop(colNames, axis=1) if len(colNames) > 0 else existingData
            summaryData = existingData.join(summaryData)
            
        if self.verbose is True:
            print(f"  ==> Created {summaryData.shape[0]} Artist ID => {summaryType} Summary Data")

        if self.test is True:
            print("  ==> Only testing. Will not save.")
        else:
            if saveit is True:
                self.rdio.saveData(f"Summary{summaryType}", data=summaryData)
            else:
                return summaryData
            