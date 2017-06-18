#!~/anaconda2/bin/python
# -*- coding: utf-8 -*-

from __future__ import division
import re
import mrjob
import json
from mrjob.protocol import RawProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep
import itertools
import collections
import logging
import time



class MRbuildStripes(MRJob):
  
    #START SUDENT CODE531_STRIPES
  
    MRJob.SORT_VALUES = True
    
    #def mapper_init(self):
    #    return self.start_time = time.time()
        
    def mapper(self, _, line):
        splits = line.rstrip("\n").split("\t")

        words = splits[0].lower().split()
        count = splits[1]

        H = {}
        for subset in itertools.combinations(sorted(set(words)), 2):
            
            # Process combinations in sorted order, i.e. "hello","tomorrow"
            if subset[0] not in H.keys():
                H[subset[0]] = {}
                H[subset[0]][subset[1]] = count 
            elif subset[1] not in H[subset[0]]:
                H[subset[0]][subset[1]] = count
            else:
                H[subset[0]][subset[1]] += count

            # Obtain combinations in reverse order, to consider them both ways
            # TODO: Should refactor this and the block above, shameless copy-paste
            if subset[1] not in H.keys():
                H[subset[1]] = {}
                H[subset[1]][subset[0]] = count 
            elif subset[0] not in H[subset[1]]:
                H[subset[1]][subset[0]] = count
            else:
                H[subset[1]][subset[0]] += count
        for key in H.keys():
            #print "%s\t%s" % (key, json.dumps(H[key]))
            yield key, H[key]

    def reducer(self, key, values):
        
        counter = {}

        for value in values:
            
            for k, v in value.iteritems():
                if k in counter:
                    counter[k] += int(v)
                else:
                    counter[k] = int(v)
        
        yield key, counter
        
    
    def steps(self):
        return [

            MRStep(#mapper_init=self.mapper_init
                   #,
                   mapper=self.mapper
                   ,
                   reducer=self.reducer
                  )
            ]
  #END SUDENT CODE531_STRIPES
  
if __name__ == '__main__':
    start_time = time.time()
    MRbuildStripes.run()
    elapsed_time = time.time() - start_time
    mins = elapsed_time/float(60)
    a = """
    Elapsed time: %s seconds
    In minutes: %s mins""" % (str(elapsed_time), str(mins))
    logging.warning(a)