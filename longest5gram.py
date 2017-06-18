#!~/anaconda2/bin/python
# -*- coding: utf-8 -*-

import re

import mrjob
from mrjob.protocol import RawProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep
import logging
import time

class longest5gram(MRJob):
    
    # START STUDENT CODE 5.4.1.A
    
    MRJob.SORT_VALUES = True

    def __init__(self, args):
        super(longest5gram, self).__init__(args)
        self.max_count = 0
        self.max_grams = []
        
    def mapper(self, _, line):
        
        # Split line
        splits = line.rstrip("\n").split("\t")
        words = splits[0].lower().split()
        
        char_count = 0
        
        # Count characters
        for word in words:
            char_count += len(word)
        
        # Optimization: we track the max count local to the current mapper instance. If records
        # have higher count than the max, we output them and update the max. We don't yield
        # records that are smaller than the local max. 
        # Even some non-max records are passed on, the good thing about this is that it is extremely memory efficient
        # in that it uses constant memory.
        if char_count > self.max_count:
            self.max_count = char_count
            yield (words), char_count
        elif char_count == self.max_count:
            yield (words), char_count
            
    
    def combiner(self, ngram, char_counts):
        current_max = max(char_counts)
        
        # Optimization: we track the max count local to the current combiner instance. If records
        # have higher count than the max, we output them and update the max. We don't yield
        # records that are smaller than the local max, drastically reducing work on shuffling and sorting
        # Even some non-max or local max records are passed on, the good thing about this is that it is extremely 
        # memory efficient in that it uses constant memory (just 1 integer :)
        if current_max > self.max_count:
            self.max_count = current_max
            yield ngram, current_max
        elif current_max == self.max_count:
            yield ngram, current_max
    
    def reducer(self, ngram, char_counts):
            
        current_count = max(char_counts)

        # Track in max_grams the n-grams with the max count of words
        if current_count > self.max_count:
            self.max_count = current_count
            self.max_grams = [(current_count, ngram)]
        elif current_count == self.max_count:
            self.max_grams.append((current_count, ngram))

    def reducer_final(self):
        # Once
        for gram in self.max_grams:
            yield gram[0], gram[1]


    def steps(self):
        
        # We need 1 reducer for this approach. However the optimizations in the mappers and combiners
        # help us ensure that a small percentage of records get to the reducer
        custom_jobconf = {
            'stream.num.map.output.key.fields':'1',
            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
            'mapred.text.key.comparator.options': '-k2,2nr',
            'mapred.reduce.tasks': '1'
        }

        return [
                MRStep(jobconf=custom_jobconf,
                    mapper=self.mapper,
                    reducer=self.reducer,
                    combiner = self.combiner,
                    reducer_final = self.reducer_final
                      )
                 ]

    # END STUDENT CODE 5.4.1.A

if __name__ == '__main__':
    start_time = time.time()
    longest5gram.run()
    elapsed_time = time.time() - start_time
    mins = elapsed_time/float(60)
    a = """
    Elapsed time: %s seconds
    In minutes: %s mins""" % (str(elapsed_time), str(mins))
    logging.warning(a)