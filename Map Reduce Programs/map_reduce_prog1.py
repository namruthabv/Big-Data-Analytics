from mrjob.job import MRJob
from mrjob.step import MRStep
import os

class WhiteHouseVisitorCnt(MRJob):
    def map_name_one(self, _, records):
        rec = records.split(',')   
        yield '%s_%s_%s' % (rec[0],rec[1],rec[2]), 1

    def reduce_count(self, key, value):
        yield (key,sum(value))

    def steps(self):
        return [MRStep(mapper=self.map_name_one,combiner=self.reduce_count,reducer=self.reduce_count)]

if __name__ == '__main__':
    os.environ['S3_USE_SIGV4'] = 'True'
    WhiteHouseVisitorCnt.run()
    del os.environ['S3_USE_SIGV4']
