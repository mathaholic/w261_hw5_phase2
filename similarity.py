#!~/anaconda2/bin/python
# -*- coding: utf-8 -*-

from __future__ import division
import collections
import re
import json
import math
#import numpy as np
import itertools
import mrjob
from mrjob.protocol import RawProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import logging

class MRsimilarity(MRJob):
  
    #START SUDENT CODE531_SIMILARITY

    MRJob.SORT_VALUES = True
    
    def mapper(self, _, line):
        key, valuesJson = line.strip().split('\t')
        key = key.strip("\"")
        values = json.loads(valuesJson)

        for pair in itertools.combinations(sorted(set(values)), 2):
            yield pair, [values[pair[0]], values[pair[1]]]
        
    def reducer(self, key, values):
        intersection = 0
        count1 = None
        count2 = None
        
        cosine = 0.0
        
        # Iterate through the values
        for value in values:
            # Jaccard, get counts for the intersection, and for each set
            intersection += 1
            if count1 == None:
                count1 = value[0]
                count2 = value[1]
        
            # Cosine
            a = 1 / math.sqrt(value[0])
            b = 1 / math.sqrt(value[1])
            cosine += a * b
            
        jaccard = float(intersection) / float(count1 + count2 - intersection)
        
        overlap_coefficient = float(intersection) / min(count1, count2)
        
        dice_coefficient = float(2 * intersection) / (count1 + count2)
        
        average = (cosine + jaccard + overlap_coefficient + dice_coefficient) / 4.0
        
        yield average, [key[0] + ' - ' + key[1], cosine, jaccard, overlap_coefficient, dice_coefficient]
            
    #END SUDENT CODE531_SIMILARITY
  
if __name__ == '__main__':
    start_time = time.time()
    MRsimilarity.run()
    elapsed_time = time.time() - start_time
    mins = elapsed_time/float(60)
    a = """
    Elapsed time: %s seconds
    In minutes: %s mins""" % (str(elapsed_time), str(mins))
    logging.warning(a)