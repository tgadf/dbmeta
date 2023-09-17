""" Media MetaData Classes """

__all__ = ["MediaMetaProducer"]

from pandas import DataFrame
from .prodbase import MetaProducerUtilBase


class MediaMetaProducer:
    def __init__(self, mediaTypeRank, **kwargs):
        self.utils = MetaProducerUtilBase(**kwargs)
        self.mediaTypeRank = mediaTypeRank
        self.maxMediaNum = kwargs.get("MaxMediaNum", 500)
            
    ###########################################################################
    # Media MetaData
    ###########################################################################
    def getRankedMediaCountsData(self, rankedMediaData):
        retval = {mediaTypeRank: sum([len(x) for x in mediaTypeRankData.values()]) for mediaTypeRank, mediaTypeRankData in rankedMediaData.items()}
        return retval
        
    def getRankedMediaData(self, rData):
        retval = {}
        mediaData = self.utils.getMediaNames(rData, maxNum=self.maxMediaNum)
        if isinstance(mediaData, dict):
            for mediaType, mediaTypeData in mediaData.items():
                mediaTypeRankName = self.mediaTypeRank.getMediaTypeRank(mediaType)
                if retval.get(mediaTypeRankName) is None:
                    retval[mediaTypeRankName] = {}
                retval[mediaTypeRankName][mediaType] = mediaTypeData
        del mediaData
        return retval
        
    def getMediaMetaData(self, modValData):
        mediaRankData = {mediaTypeRankName: {} for rank, mediaTypeRankName in self.mediaTypeRank.mediaTypes.items()}
        for n, (artistID, artistIDData) in enumerate(modValData.iteritems()):
            rankedMediaData = self.getRankedMediaData(artistIDData)
            if len(rankedMediaData) > 0:
                for mediaTypeRankName, mediaTypeRankNameData in rankedMediaData.items():
                    mediaRankData[mediaTypeRankName][artistID] = mediaTypeRankNameData
            else:
                for rank, mediaTypeRankName in self.mediaTypeRank.mediaTypes.items():
                    mediaRankData[mediaTypeRankName][artistID] = None
                    
        metaData = DataFrame(mediaRankData)
        return metaData