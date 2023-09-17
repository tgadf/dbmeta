from dbmaster import MasterDBs
from dbbase import MusicDBRootDataIO
from dbmeta import MediaSummaryProducerBase


def test_mediasummary():
    dbs = MasterDBs().getDBs()
    rdio = MusicDBRootDataIO(dbs[0])
    mediasumprod = MediaSummaryProducerBase(rdio)
    assert hasattr(mediasumprod, 'make'), f"MediaSummaryProducerBase [{mediasumprod}] does not have a make function"
    
    
if __name__ == "__main__":
    test_mediasummary()
    