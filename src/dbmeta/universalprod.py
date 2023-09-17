""" Universal (All DBs) MetaData Producer Classes """

__all__ = ["UniversalMetaProducer"]

from utils import getFlatList
from pandas import DataFrame, Series
from statistics import median
from .prodbase import MetaProducerUtilBase


class UniversalMetaProducer:
    def __init__(self, **kwargs):
        self.utils = MetaProducerUtilBase(**kwargs)

    ###############################################################################################################
    # Basic MetaData
    ###############################################################################################################
    def getBasicMetaData(self, modValData: Series) -> 'Series':
        assert isinstance(modValData, Series), "modValData is not a Series"
        artistNames = modValData.apply(lambda rData: rData.artist.name)
        artistNames.name = "ArtistName"
        artistURLs = modValData.apply(lambda rData: rData.url.url)
        artistURLs.name = "URL"
        artistNumAlbums = modValData.apply(lambda rData: sum(rData.mediaCounts.counts.values()))
        artistNumAlbums.name = "NumAlbums"

        metaData = DataFrame([artistNames, artistURLs, artistNumAlbums]).T
        return metaData

    ###############################################################################################################
    # Date MetaData
    ###############################################################################################################
    def getDatesMetaData(self, modValData: Series) -> 'Series':
        assert isinstance(modValData, Series), "modValData is not a Series"
        
        def getMediaDateStats(mediaDates):
            mediaTypeDates = {}
            for mediaType, mediaTypeYears in mediaDates.items():
                mediaTypeYearsData = []
                for year in mediaTypeYears:
                    try:
                        yearValue = int(year)
                    except Exception as error:
                        continue
                    mediaTypeYearsData.append(yearValue)

                if len(mediaTypeYearsData) > 0:
                    mediaTypeDates[mediaType] = mediaTypeYearsData
            mediaTypeDates = getFlatList(mediaTypeDates.values())
            mediaDatesStats = (min(mediaTypeDates), max(mediaTypeDates), int(median(mediaTypeDates))) if len(mediaTypeDates) > 0 else (None, None, None)
            return mediaDatesStats

        artistMediaDates = modValData.apply(self.utils.getMediaDates).apply(getMediaDateStats)
        
        metaData = artistMediaDates.apply(Series)
        metaData.columns = ["MinYear", "MaxYear", "MedianYear"]
        return metaData