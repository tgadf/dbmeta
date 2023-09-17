""" MusicDB Summary Data Producer"""

__all__ = ["SummaryProducerIO"]

from dbbase import MusicDBRootDataIO, getModVals
from utils import Timestat, getFlatList
from pandas import Series, DataFrame, concat
from .prodbase import SummaryProducerBase


###############################################################################
# MusicDB Summary Data Producer
###############################################################################
class SummaryProducerIO(SummaryProducerBase):
    def __repr__(self):
        return f"SummaryProducerIO(db={self.db})"
        
    def __init__(self, rdio: MusicDBRootDataIO, **kwargs):
        super().__init__(rdio, **kwargs)
        self.dbsums = {}
        if self.verbose:
            print(self.__repr__())
            
        for summaryType in self.summaryTypes.keys():
            func = "make{0}SummaryData".format(summaryType)
            if hasattr(self.__class__, func) and callable(getattr(self.__class__, func)):
                self.dbsums[summaryType] = eval("self.{0}".format(func))
                if self.verbose:
                    print(f"  ==> {summaryType}")
                
    def getSummaryTypes(self, key) -> 'dict':
        assert isinstance(self.dbsums, dict), f"dbsums [{type(self.dbsums)}] is not a dict"
        if isinstance(key, str):
            assert key in self.dbsums.keys(), f"SummaryType key [{key}] is not allowed"
            return {key: self.dbsums[key]}
        return self.dbsums
            
    ###########################################################################
    # Master Maker
    ###########################################################################
    def make(self, modVal=None, key=None, **kwargs):
        self.verbose = kwargs.get("verbose", self.verbose)
        self.test = kwargs.get('test', False)
        self.modVals = getModVals(modVal) if self.test is False else [0]
        summaryTypes = self.getSummaryTypes(key)
            
        ts = Timestat(f"Making {list(summaryTypes.keys())} Summary Data", verbose=self.verbose)
        for summaryType, summaryTypeFunc in summaryTypes.items():
            summaryTypeFunc(**kwargs)
        ts.stop()
        
    ###########################################################################
    # Artist ID => Name/URL Map
    ###########################################################################
    def makeBasicSummaryData(self, **kwargs) -> 'None':
        summaryType = "Basic"
        metaType = f"Meta{summaryType}"
        ts = Timestat(f"Making {self.db} {summaryType} Summary Data", verbose=self.verbose)
        
        artistIDToName = None
        artistIDToRef = None
        artistIDToNumAlbums = None
        for n, modVal in enumerate(self.modVals):
            if self.isUpdateModVal(n) is True:
                ts.update(n=n + 1, N=len(self.modVals))
            modValMetaData = self.rdio.getData(metaType, modVal) if self.rdio.getFilename(metaType, modVal).exists() else None
            if isinstance(modValMetaData, DataFrame):
                artistNames = self.sns.update(modValMetaData["ArtistName"], "Name")
                artistIDToName = concat([artistIDToName, artistNames]) if isinstance(artistIDToName, Series) else artistNames
                artistURLs = modValMetaData["URL"]
                artistIDToRef = concat([artistIDToRef, artistURLs]) if isinstance(artistIDToRef, Series) else artistURLs
                artistNumAlbums = modValMetaData["NumAlbums"]
                artistIDToNumAlbums = concat([artistIDToNumAlbums, artistNumAlbums]) if isinstance(artistIDToNumAlbums, Series) else artistNumAlbums

        if self.verbose is True:
            print(f"  ==> Created {artistIDToName.shape[0]} Artist ID => Name {summaryType} Summary Data")
            print(f"  ==> Created {artistIDToRef.shape[0]} Artist ID => Ref {summaryType} Summary Data")
            print(f"  ==> Created {artistIDToNumAlbums.shape[0]} Artist ID => NumAlbums {summaryType} Summary Data")

        if self.test is True:
            print("  ==> Only testing. Will not save.")
        else:
            artistIDToName.name = "Name"
            self.rdio.saveData("SummaryName", data=artistIDToName)
            artistIDToRef.name = "Ref"
            self.rdio.saveData("SummaryRef", data=artistIDToRef)
            artistIDToNumAlbums.name = "NumAlbums"
            self.rdio.saveData("SummaryNumAlbums", data=artistIDToNumAlbums)
        
        ts.stop()

    ###########################################################################
    # Artist ID => Media
    ###########################################################################
    def makeMediaSummaryData(self, **kwargs):
        summaryType = "Media"
        metaType = f"Meta{summaryType}"
        ts = Timestat(f"Making {self.db} {summaryType} Summary Data", verbose=self.verbose)
                
        artistIDToMedia = None
        artistIDToCounts = None
        for n, modVal in enumerate(self.modVals):
            if self.isUpdateModVal(n) is True:
                ts.update(n=n + 1, N=len(self.modVals))
            modValMetaData = self.rdio.getData(metaType, modVal) if self.rdio.getFilename(metaType, modVal).exists() else None
            if isinstance(modValMetaData, DataFrame):
                modValMetaData = modValMetaData.map(lambda x: getFlatList(x.values()) if isinstance(x, dict) else None)
                artistIDToMedia = concat([artistIDToMedia, modValMetaData]) if artistIDToMedia is not None else modValMetaData
            
        artistIDToCounts = artistIDToMedia.map(lambda x: len(x) if isinstance(x, list) else 0)
        artistIDToCounts = artistIDToCounts.fillna(0).astype(int)
        
        if self.verbose is True:
            print(f"  ==> Created {artistIDToCounts.shape[0]} Artist ID => Counts {summaryType} Summary Data")
            for rankedMediaType, rankedMediaTypeData in artistIDToMedia.items():
                if len(rankedMediaTypeData) > 0 and rankedMediaTypeData.count() > 0:
                    print(f"  ==> Created {rankedMediaTypeData.shape[0]} Artist ID => {rankedMediaType} {summaryType} Summary Data")
                
        if self.test is True:
            print("  ==> Only testing. Will not save.")
        else:
            artistIDToCounts.name = "Counts"
            self.rdio.saveData("SummaryCounts", data=artistIDToCounts)
            for rankedMediaType, rankedMediaTypeData in artistIDToMedia.items():
                if len(rankedMediaTypeData) > 0 and rankedMediaTypeData.count() > 0:
                    rankedMediaTypeData.name = rankedMediaType
                    self.rdio.saveData(f"Summary{rankedMediaType}Media", data=rankedMediaTypeData)
        
        ts.stop()

    ###########################################################################
    # Artist ID => Genre
    ###########################################################################
    def makeGenreSummaryData(self, **kwargs):
        summaryType = "Genre"
        metaType = f"Meta{summaryType}"
        ts = Timestat(f"Making {self.db} {summaryType} Summary Data", verbose=self.verbose)
        
        artistIDToGenre = None
        for n, modVal in enumerate(self.modVals):
            if self.isUpdateModVal(n) is True:
                ts.update(n=n + 1, N=len(self.modVals))

            modValMetaData = self.rdio.getData(metaType, modVal) if self.rdio.getFilename(metaType, modVal).exists() else None
            if isinstance(modValMetaData, DataFrame):
                artistIDToGenre = concat([artistIDToGenre, modValMetaData]) if artistIDToGenre is not None else modValMetaData

        if self.verbose is True:
            print(f"  ==> Created {artistIDToGenre.shape[0]} Artist ID => {summaryType} Summary Data")
            
        if self.test is True:
            print("  ==> Only testing. Will not save.")
        else:
            if isinstance(artistIDToGenre, DataFrame):
                artistIDToGenre.name = "Genre"
                self.rdio.saveData(f"Summary{summaryType}", data=artistIDToGenre)
        
        ts.stop()

    ###########################################################################
    # Artist ID => Link
    ###########################################################################
    def makeLinkSummaryData(self, **kwargs):
        summaryType = "Link"
        metaType = f"Meta{summaryType}"
        ts = Timestat(f"Making {self.db} {summaryType} Summary Data", verbose=self.verbose)
        
        artistIDToLink = None
        for n, modVal in enumerate(self.modVals):
            if self.isUpdateModVal(n) is True:
                ts.update(n=n + 1, N=len(self.modVals))

            modValMetaData = self.rdio.getData(metaType, modVal) if self.rdio.getFilename(metaType, modVal).exists() else None
            if isinstance(modValMetaData, DataFrame):
                artistIDToLink = concat([artistIDToLink, modValMetaData]) if artistIDToLink is not None else modValMetaData

        if self.verbose is True:
            print(f"  ==> Created {artistIDToLink.shape[0]} Artist ID => {summaryType} Summary Data")
            
        if self.test is True:
            print("  ==> Only testing. Will not save.")
        else:
            if isinstance(artistIDToLink, DataFrame):
                self.rdio.saveData(f"Summary{summaryType}", data=artistIDToLink)
        
        ts.stop()

    ###########################################################################
    # Artist ID => Bio
    ###########################################################################
    def makeBioSummaryData(self, **kwargs):
        summaryType = "Bio"
        metaType = f"Meta{summaryType}"
        ts = Timestat(f"Making {self.db} {summaryType} Summary Data", verbose=self.verbose)
        
        artistIDToBio = None
        for n, modVal in enumerate(self.modVals):
            if self.isUpdateModVal(n) is True:
                ts.update(n=n + 1, N=len(self.modVals))
            modValMetaData = self.rdio.getData(metaType, modVal) if self.rdio.getFilename(metaType, modVal).exists() else None
            if isinstance(modValMetaData, DataFrame):
                artistIDToBio = concat([artistIDToBio, modValMetaData]) if artistIDToBio is not None else modValMetaData

        if self.verbose is True:
            print(f"  ==> Created {artistIDToBio.shape[0]} Artist ID => {summaryType} Summary Data")
            
        if self.test is True:
            print("  ==> Only testing. Will not save.")
        else:
            if isinstance(artistIDToBio, DataFrame):
                self.rdio.saveData(f"Summary{summaryType}", data=artistIDToBio)
        
        ts.stop()
        
    ###########################################################################
    # Artist ID => Dates
    ###########################################################################
    def makeDatesSummaryData(self, **kwargs):
        return
        summaryType = "Dates"
        metaType = f"Meta{summaryType}"
        ts = Timestat(f"Making {self.db} {summaryType} Summary Data", verbose=self.verbose)
        
        artistIDToDates = None
        for n, modVal in enumerate(self.modVals):
            if self.isUpdateModVal(n) is True:
                ts.update(n=n + 1, N=len(self.modVals))
            modValMetaData = self.rdio.getData(metaType, modVal) if self.rdio.getFilename(metaType, modVal).exists() else None
            if isinstance(modValMetaData, DataFrame):
                artistIDToDates = concat([artistIDToDates, modValMetaData]) if artistIDToDates is not None else modValMetaData

        if self.verbose is True:
            print(f"  ==> Created {artistIDToDates.shape[0]} Artist ID => {summaryType} Summary Data")
            
        if self.test is True:
            print("  ==> Only testing. Will not save.")
        else:
            if isinstance(artistIDToDates, DataFrame):
                self.rdio.saveData(f"Summary{summaryType}", data=artistIDToDates)
        
        ts.stop()

    ###########################################################################
    # Artist ID => Metric
    ###########################################################################
    def makeMetricSummaryData(self, **kwargs):
        summaryType = "Metric"
        metaType = f"Meta{summaryType}"
        ts = Timestat(f"Making {self.db} {summaryType} Summary Data", verbose=self.verbose)
        
        artistIDToMetric = None
        for n, modVal in enumerate(self.modVals):
            if self.isUpdateModVal(n) is True:
                ts.update(n=n + 1, N=len(self.modVals))
            modValMetaData = self.rdio.getData(metaType, modVal) if self.rdio.getFilename(metaType, modVal).exists() else None
            if isinstance(modValMetaData, DataFrame):
                artistIDToMetric = concat([artistIDToMetric, modValMetaData]) if artistIDToMetric is not None else modValMetaData

        if self.verbose is True:
            print(f"  ==> Created {artistIDToMetric.shape[0]} Artist ID => {summaryType} Summary Data")
            
        if self.test is True:
            print("  ==> Only testing. Will not save.")
        else:
            if isinstance(artistIDToMetric, DataFrame):
                self.rdio.saveData(f"Summary{summaryType}", data=artistIDToMetric)
        
        ts.stop()
        