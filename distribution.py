#!~/anaconda2/bin/python
# -*- coding: utf-8 -*-

import mrjob
from mrjob.protocol import RawProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import logging

class distribution(MRJob):
    
    # START STUDENT CODE 5.4.1.D
    
    MRJob.SORT_VALUES = True
        
    def mapper(self, _, line):
        
        # Split line
        splits = line.rstrip("\n").split("\t")
        words = splits[0].lower().split()
        
        char_count = 0
        
        # Count characters
        for word in words:
            char_count += len(word)
        
        yield char_count, 1
            
    
    def combiner(self, ngram_size, counts):
        yield ngram_size, sum(count for count in counts)
    
    def reducer(self, ngram_size, counts):
        yield ngram_size, sum(count for count in counts)

    def steps(self):
        
        custom_jobconf = {
            'stream.num.map.output.key.fields':'2',
            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
            'mapred.text.key.comparator.options': '-k1,1n',
            'mapred.reduce.tasks': '1'
        }

        return [
                MRStep(
                    jobconf=custom_jobconf,
                    mapper=self.mapper,
                    reducer=self.reducer,
                    combiner = self.combiner)
        ]

    # END STUDENT CODE 5.4.1.D
    
if __name__ == '__main__':
    start_time = time.time()
    distribution.run()
    elapsed_time = time.time() - start_time
    mins = elapsed_time/float(60)
    a = """
    Elapsed time: %s seconds
    In minutes: %s mins""" % (str(elapsed_time), str(mins))
    logging.warning(a)