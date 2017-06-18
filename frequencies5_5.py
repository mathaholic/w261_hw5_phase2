#!~/anaconda2/bin/python
# -*- coding: utf-8 -*-

import re

import mrjob
from mrjob.protocol import RawProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import logging

class frequencies(MRJob):

    # START STUDENT CODE 5.4.1.B
    
    MRJob.SORT_VALUES = True
        
    def __init__(self, args):
        super(frequencies, self).__init__(args)
        #self.min_rank = 9001
        #self.max_rank = 10000 
        self.current_rank = 0

    def configure_options(self): 
        super(frequencies, self).configure_options() 
        self.add_passthrough_option('--min_rank', dest='min_rank', type='int', default=9001) 
        self.add_passthrough_option('--max_rank', dest='max_rank', type='int', default=10000) 
    
    def mapper(self, _, line):
        
        # Split line
        splits = line.rstrip("\n").split("\t")
        words = splits[0].lower().split()
        count = int(splits[1])
        
        for word in words:
            yield word, count
            
    
    def combiner(self, word, counts):
        total = sum(count for count in counts)
        yield word, total
    
    def reducer(self, word, counts):
        total = sum(count for count in counts)
        yield total, word
    
    def max_reducer(self, count, words):
        
        # Words come in frequency descending order here
        # Only yield the words that are within the min and max frequency ranking desired
        
        for word in words:
            self.current_rank += 1
            
            if self.current_rank >= self.options.min_rank and self.current_rank <= self.options.max_rank:
                yield word, count

    def steps(self):
        
        custom_jobconf = {
            'stream.num.map.output.key.fields':'2',
            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
            'mapred.text.key.comparator.options': '-k1,1nr',
            'mapred.reduce.tasks': '1'
        }

        return [
                MRStep(mapper=self.mapper,
                    reducer=self.reducer,
                    combiner = self.combiner),
                MRStep(jobconf=custom_jobconf,
                       reducer=self.max_reducer)
                 ]

    # END STUDENT CODE 5.4.1.B
        
if __name__ == '__main__':
    start_time = time.time()
    frequencies.run()
    elapsed_time = time.time() - start_time
    mins = elapsed_time/float(60)
    a = """
    Elapsed time: %s seconds
    In minutes: %s mins""" % (str(elapsed_time), str(mins))
    logging.warning(a)