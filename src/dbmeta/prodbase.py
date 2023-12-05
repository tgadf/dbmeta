""" Metadata Producer Base Class """

__all__ = ["MatchProducerBase", "MatchOmitBase", "SummaryProducerBase",
           "MetaProducerUtilBase", "MediaTypeRankBase"]

from dbmaster import MasterMetas
from dbbase import SummaryNameStandard, MatchNameStandard, MusicDBRootDataIO
from dbraw import RawDataIOBase
from utils import getFlatList
from pandas import Series


###############################################################################
# Match Data Base
###############################################################################
class MatchProducerBase:
    def __repr__(self):
        return "MatchProducerBase(db={self.db})"
    
    def __init__(self, rdio: MusicDBRootDataIO, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        assert isinstance(rdio, MusicDBRootDataIO), f"rdio [{rdio}] is not of type MusicDBRootDataIO"
        mm = MasterMetas()
        self.matchTypes = mm.getMatchTypes()
        self.summaryTypes = mm.getSummaryTypes()
        self.mns = MatchNameStandard()
        self.rdio = rdio
        self.db = rdio.db


###############################################################################
# Match Omit Base
###############################################################################
class MatchOmitBase:
    def __repr__(self):
        return "MatchOmitBase()"
    
    def __init__(self, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        self.setMatchTypes()
        assert isinstance(self.matchTypes, list), f"MatchTypes [{self.matchTypes}] is not a list"
        self.omit = {}
        
    def setOmitData(self, omitData: dict) -> 'None':
        assert isinstance(omitData, dict), f"OmitData [{type(omitData)}] is not a dict"
        self.omit = omitData
        
    def setMatchTypes(self, keep=None, skip=None) -> 'None':
        mm = MasterMetas()
        if isinstance(skip, list):
            medias = [media for media in mm.medias.values() if media not in skip]
            retval = mm.getMatches(medias)
        elif isinstance(keep, list):
            retval = mm.getMatches(keep)
        else:
            retval = mm.getMatches()
        self.matchTypes = retval

    def isValid(self, dbid) -> 'bool':
        retval = not self.omit.get(dbid, False)
        return retval

        
###############################################################################
# Summary Producer Base
###############################################################################
class SummaryProducerBase:
    def __repr__(self):
        return f"SummaryProducerBase(db={self.db})"
    
    def __init__(self, rdio, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        assert isinstance(rdio, MusicDBRootDataIO), f"rdio [{rdio}] is not of type MusicDBRootDataIO"
        mm = MasterMetas()
        self.summaryTypes = mm.getSummaryTypes()
        self.sns = SummaryNameStandard()
        self.rdio = rdio
        self.db = rdio.db

    ###########################################################################
    # ModVals and When To Show
    ###########################################################################
    def isUpdateModVal(self, n):
        if self.verbose is False:
            return False
        retval = True if ((n + 1) % 25 == 0 or (n + 1) == 5) else False
        return retval
    
    
###############################################################################
# Media Type Rank Base
###############################################################################
class MediaTypeRankBase:
    def __init__(self, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        self.mediaTypes = MasterMetas().getMediaTypes()
        self.last = Series(self.mediaTypes).index.max()
        self.mediaRanking = {rank: [] for rank in self.mediaTypes.keys()}
         
    def sortMediaData(self, mediaData):
        results = {rank: {} for rank in self.mediaRanking.keys()}
        remaining = {}
        for mediaType, cnt in mediaData.iteritems():
            values = Series({rank: sum([tag in mediaType for tag in rankTags]) for rank, rankTags in self.mediaRanking.items() if rank not in [self.last]})
            values = values[values > 0]
            if len(values) > 0:
                results[values.index.max()][mediaType] = cnt
            else:
                remaining[mediaType] = cnt
                
        # Move overflow to remaining
        remaining.update(results[self.last])
        del results[self.last]
        results.update({"Remaining": remaining})
        return results
    
    def getMediaTypeRank(self, mediaType):
        if isinstance(mediaType, str):
            values = Series({rank: sum([tag in mediaType for tag in rankTags]) for rank, rankTags in self.mediaRanking.items() if rank not in [self.last]})
            values = values[values > 0]
            retval = self.mediaTypes[values.index.max()] if len(values) > 0 else self.mediaTypes[self.last]
            return retval
        else:
            retval = self.mediaTypes[self.last]
            return retval
   

###############################################################################
# Meta Data Utils Base
###############################################################################
class MetaProducerUtilBase:
    def __init__(self, **kwargs):
        self.rawbase = RawDataIOBase()
        
    def isRawData(self, rData):
        retval = rData.__class__.__name__ == "RawData"
        return retval
        
    def isRawTextData(self, rData):
        retval = rData.__class__.__name__ == "RawTextData"
        return retval
        
    def isRawLinkData(self, rData):
        retval = rData.__class__.__name__ == "RawLinkData"
        return retval
        
    def isRawURLInfoData(self, rData):
        retval = rData.__class__.__name__ == "RawURLInfoData"
        return retval
        
    def isRawMediaData(self, rData):
        retval = rData.__class__.__name__ == "RawMediaData"
        return retval
        
    def getProfileData(self, rData):
        retval = getattr(rData, 'profile') if (hasattr(rData, 'profile') and self.isRawData(rData)) else None
        return retval
        
    def getExtraDictData(self, rData, default=None):
        profileData = self.getProfileData(rData)
        retval = getattr(profileData, 'extra') if hasattr(profileData, 'extra') else None
        return retval
        
    def getExtraData(self, rData, key, default=None):
        extraData = self.getExtraDictData(rData)
        retval = extraData.get(key, default) if isinstance(extraData, dict) else default
        return retval
        
    def getGeneralDictData(self, rData):
        profileData = self.getProfileData(rData)
        retval = getattr(profileData, 'general') if hasattr(profileData, 'general') else None
        return retval
        
    def getGeneralData(self, rData, key, default=None):
        generalData = self.getGeneralDictData(rData)
        retval = generalData.get(key, default) if isinstance(generalData, dict) else default
        return retval
        
    def getExternalDictData(self, rData):
        profileData = self.getProfileData(rData)
        retval = getattr(profileData, 'external') if hasattr(profileData, 'external') else None
        return retval
        
    def getExternalData(self, rData, key, default=None):
        externalData = self.getExternalDictData(rData)
        retval = externalData.get(key, default) if isinstance(externalData, dict) else default
        return retval
        
    def getGenresData(self, rData, default=None):
        profileData = self.getProfileData(rData)
        retval = getattr(profileData, 'genres') if hasattr(profileData, 'genres') else default
        return retval
        
    def getTagsData(self, rData, default=None):
        profileData = self.getProfileData(rData)
        retval = getattr(profileData, 'tags') if hasattr(profileData, 'tags') else default
        return retval
        
    def getMediaData(self, rData, default=None):
        mediaData = getattr(rData, 'media') if (hasattr(rData, 'media') and self.isRawData(rData)) else None
        retval = getattr(mediaData, 'media') if hasattr(mediaData, 'media') else default
        return retval
    
    def getMediaNames(self, rData, maxNum=100):
        media = self.getMediaData(rData, {})
        if not isinstance(media, dict):
            return {}
        retval = {}
        for mediaType, mediaTypeData in media.items():
            retval[mediaType] = []
            for mediaID, media in mediaTypeData.items():
                if hasattr(media, 'album') is True:
                    retval[mediaType].append(getattr(media, 'album'))
                elif hasattr(media, 'name') is True:
                    retval[mediaType].append(getattr(media, 'name'))
                if len(retval[mediaType]) >= maxNum:
                    break
        return retval
    
    def getMediaDates(self, rData):
        return {}
        media = self.getMediaData(rData, {})
        retval = {mediaType: [release.year for release in mediaTypeData.values() if isinstance(release.year, (int, str))] for mediaType, mediaTypeData in media.items()}
        return retval
    
    def getMediaArtists(self, rData):
        return {}
        media = self.getMediaData(rData, {})
        retval = getFlatList([[release.artist for release in mediaTypeData.values()] for mediaTypeData in media.values()])
        return retval
    
    def getMediaFormats(self, rData, default=[]):
        return {}
        media = self.getMediaData(rData, {})
        retval = getFlatList([[release.aformat for release in mediaTypeData.values()] for mediaTypeData in media.values()])
        return retval