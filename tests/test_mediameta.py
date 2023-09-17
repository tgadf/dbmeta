from dbmeta import MediaMetaProducer


def test_mediameta():
    mediametaprod = MediaMetaProducer(mediaTypeRank={})
    assert hasattr(mediametaprod, 'getMediaMetaData'), f"MediaMetaProducer [{mediametaprod}] does not have a getMediaMetaData function"
    
    
if __name__ == "__main__":
    test_mediameta()
    