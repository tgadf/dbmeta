""" Base Class For MetaData Creation """

__all__ = ["MetaProducerBase"]

from dbmaster import MasterMetas
from dbbase import MusicDBRootDataIO, getModVals
from utils import Timestat, getFlatList
from pandas import DataFrame, Series, concat
import warnings

class MetaProducerBase:
    def __repr__(self):
        return f"MetaProducerBase(db={self.rdio.db})"
        
    def __init__(self, rdio: MusicDBRootDataIO, **kwargs):
        self.rdio = rdio
        self.verbose = kwargs.get('verbose', False)
        self.maxMedia = kwargs.get('maxMedia', None)
        self.mediaRanking = {}
        self.dbmetas = {}
        
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
        metaTypes = self.getMetaTypes(key)
            
        ts = Timestat(f"Making {len(modVals)} {list(metaTypes.keys())} MetaData", verbose=verbose)
        
        for n, modVal in enumerate(modVals):
            if self.isUpdateModVal(n):
                ts.update(n=n + 1, N=len(modVals))

            modValData = self.rdio.getModValData(modVal)
            for metaKey, metaFunc in metaTypes.items():
                if verbose:
                    print(f"    MetaType={metaKey} ... ", end="")
                metaData = metaFunc(modValData)
                if test is True:
                    print("Only testing. Will not save.")
                    continue
                else:
                    if verbose:
                        print(metaData.shape)
                    self.rdio.saveData(f"Meta{metaKey}", modVal, data=metaData)
                        
        ts.stop()
    
    ###########################################################################
    # Basic Data
    ###########################################################################
    def getBasicMetaData(self, modValData: DataFrame) -> 'DataFrame':
        assert isinstance(modValData, DataFrame), f"ModValData [{type(modValData)}] is not a DataFrame object"
        cols = ["name", "url"]
        assert all([col in modValData.columns for col in cols]), f"Could not find all basic columns [{cols}] in DataFrame columns [{modValData.columns}]"
        if "Media" in modValData.columns:
            cols.append("Media")
            basicData = modValData[cols].copy(deep=True)
            basicData["NumAlbums"] = basicData["Media"].apply(lambda media: media.shape[0] if isinstance(media, DataFrame) else 0)
            basicData = basicData.drop("Media", axis=1).rename(columns={"name": "ArtistName", "url": "URL"})
        else:
            basicData = modValData[cols].copy(deep=True)
            basicData["NumAlbums"] = 0
            basicData = basicData.rename(columns={"name": "ArtistName", "url": "URL"})
            
        return basicData
    
    ###########################################################################
    # Media Data
    ###########################################################################
    def getMediaMetaData(self, modValData: DataFrame) -> 'DataFrame':
        assert isinstance(self.mediaRanking, dict), f"MediaRanking [{type(self.mediaRanking)}] is not a dict object"
        assert isinstance(modValData, DataFrame), f"ModValData [{type(modValData)}] is not a DataFrame object"
        col = "Media"
        if col not in modValData.columns:
            return None
        assert col in modValData.columns, f"Could not find all media column [{col}] in DataFrame columns [{modValData.columns}]"
    
        def testMediaNameTypes(mediaData):
            mediaSampleData = mediaData[mediaData.notna()]
            mediaSampleData = concat(mediaSampleData.sample(n=min([50, mediaSampleData.shape[0]]), replace=True).values)
            if not all([isinstance(mediaSampleData, DataFrame), "Type" in mediaSampleData.columns]):
                warnings.warn("Warning: There is no media to test media name types...")
                return
            
            mediaTypeNames = set(mediaSampleData["Type"].unique())
            mediaRankTypeNameValues = set(getFlatList(self.mediaRanking.values()))
            unknownMediaTypeNames = mediaTypeNames.difference(mediaRankTypeNameValues)
            if len(unknownMediaTypeNames) > 0:
                raise ValueError(f"Could not find [{unknownMediaTypeNames}] mediaTypeNames in mediaRankings (from metadataio)\nMediaTypeNames = {mediaTypeNames}\nMediaRankTypeNameValues = {mediaRankTypeNameValues}")
            unknownMediaRankTypeNames = mediaRankTypeNameValues.difference(mediaTypeNames)
            if len(unknownMediaRankTypeNames) > 0:
                if self.verbose is True:
                    warnings.warn(f"Warning: Could not find {unknownMediaRankTypeNames} mediaRankTypeNames in mediaTypeNames (from data)")
    
        def getMediaFromType(media, mediaRank, mediaRankTypes):
            if not isinstance(media, DataFrame):
                return None
            retvalMediaData = media[media["Type"].isin(mediaRankTypes)] if ("Type" in media.columns) else None
            retval = {}
            if isinstance(retvalMediaData, DataFrame) and retvalMediaData.shape[0] > 0:
                for mediaRank, mediaRankData in retvalMediaData.groupby("Type"):
                    retval[mediaRank] = retvalMediaData.head(self.maxMedia)["name"].to_list() if isinstance(self.maxMedia, int) else mediaRankData["name"].to_list()
            else:
                retval = None
            return retval
        
        mediaData = modValData[col]
        testMediaNameTypes(mediaData)
        mediaRankData = mediaData.apply(lambda media: {mediaRank: getMediaFromType(media, mediaRank, mediaRankTypes) for mediaRank, mediaRankTypes in self.mediaRanking.items()})
        mediaRankData = DataFrame(mediaRankData.to_dict()).T
        mediaRankData = mediaRankData.rename(columns=MasterMetas().getMediaTypes())
        return mediaRankData
    
    ##########################################################################################
    # Helper Functions
    ##########################################################################################
    def getDictData(self, colData, key):
        retval = colData.get(key) if isinstance(colData, dict) else None
        return retval