from mrjob.job import MRJob
from mrjob.step import MRStep
import os

class WhiteHouseVisitorCnt(MRJob):
    def map_name_one(self, _, records):
        rec = records.split(',')   
        yield '%s_%s' % (rec[22],rec[23]), 1

    def combine_count(self, key, value):
        yield (key,sum(value))

    def reducer_count(self, key, value):
        yield None, (sum(value),key)

    def reduce_max_loc(self, key, value):
        yield max(value)

    def steps(self):
        return [MRStep(mapper=self.map_name_one,combiner=self.combine_count,reducer=self.reducer_count),MRStep(reducer=self.reduce_max_loc)]

if __name__ == '__main__':
    os.environ['S3_USE_SIGV4'] = 'True'
    WhiteHouseVisitorCnt.run()
    del os.environ['S3_USE_SIGV4']
