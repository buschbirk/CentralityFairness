import os, sys, inspect

import numpy as np
import pandas as pd
import math
import pickle

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

from lenskit.metrics.topnFair import *


class Evaluator:

  def __init__(self, file_name, centrality, sample):
    path = os.path.join(currentdir, file_name)
    self.data = pd.read_csv(path, sep='\t')
    self.data = self.data.query('Gender != -1')
    print('Data size:', len(self.data))
    if sample is not None:
      self.data = self.data.sample(sample)
    #print(self.data.Gender.value_counts(normalize = True))
    #print(self.data.Gender.value_counts(normalize = False))
    self.add_column()
    self.centrality = centrality
    self.data.sort_values(by = self.centrality, ascending = False, inplace = True)
    #print(self.data.head(100).Gender.value_counts(normalize = True))
    self.data.rename(columns = {'AuthorId': 'item', self.centrality: 'rank'}, inplace = True)

  def add_column(self):
    self.data['protected'] = self.data.Gender.apply(lambda x: int(x == 0))
  

  def topK(self, k, remove_unknown_gender = False):
    sort_data()
    if remove_unknown_gender:
      self.data = self.data.query('Gender != -1')

    self.data.rename(columns = {'Author': 'item', self.centrality: 'rank'}, inplace = True)
    return renamed.head()


  def run_evaluations(self):
    print('rND:', calculateNDFairnes(recs = self.data, truth = [], metric = 'rND', protected_varible = 'protected'))
    print('rKL:', calculateNDFairnes(recs = self.data, truth = [], metric = 'rKL', protected_varible = 'protected'))
    print('rRD:', calculateNDFairnes(recs = self.data, truth = [], metric = 'rRD', protected_varible = 'protected'))
    print('equal_ex:', calculateNDFairnes(recs = self.data, truth = [], metric = 'equal_ex', protected_varible = 'protected'))
    print('done')


if __name__ == '__main__':
  eval = Evaluator('economics.csv', 'PageRank', None)
  # eval = Evaluator('propublica.csv', 'Recidivism_rawscore')
  eval.run_evaluations()