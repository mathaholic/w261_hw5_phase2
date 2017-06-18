#!~/anaconda2/bin/python
# -*- coding: utf-8 -*-


from __future__ import division
import collections
import re
import json
import math
import numpy as np
import itertools
import mrjob
from mrjob.protocol import RawProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import logging
import time

class MRinvertedIndex(MRJob):
    
    #START SUDENT CODE531_INV_INDEX
  
    def mapper(self, _, line):
        key, stripeJson = line.strip().split('\t')
        key = key.strip("\"")
        stripe = json.loads(stripeJson)
        
        for k, v in stripe.iteritems():
            yield k, [key, len(stripe)]
        
    def reducer(self, key, values):

        table = {}
        for value in values:
            table[value[0]] = value[1]
            
        yield key, table
        
    #END SUDENT CODE531_INV_INDEX
        
if __name__ == '__main__':
    start_time = time.time()
    MRinvertedIndex.run() 
    elapsed_time = time.time() - start_time
    mins = elapsed_time/float(60)
    a = """
    Elapsed time: %s seconds
    In minutes: %s mins""" % (str(elapsed_time), str(mins))
    logging.warning(a)