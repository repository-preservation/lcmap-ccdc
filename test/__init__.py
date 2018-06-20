import os
import sys
import vcr as _vcr

TEST_ROOT = os.path.dirname(os.path.abspath(__file__))
cassette = 'test/resources/ccdc-v1-cassette.yaml'
vcr = _vcr.VCR(record_mode='new_episodes')
