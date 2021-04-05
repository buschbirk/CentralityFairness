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
    self.centrality = centrality
    self.read_data(file_name)
    if sample is not None:
      self.data = self.data.sample(sample)
    print('gender counts normalized:', self.data.Gender.value_counts(normalize = True))
    print('gender counts:', self.data.Gender.value_counts(normalize = False))
    self.add_protected_column()
    self.data.rename(columns = {'AuthorId': 'item', self.centrality: 'rank'}, inplace = True)
    self.sort_data()
    print('sorted data.head():')
    print(self.data.head())

  def sort_data(self):
    if self.centrality == 'Rank':
      self.data.sort_values(by = 'rank', ascending = True, inplace = True) 
    else: 
      self.data.sort_values(by = 'rank', ascending = False, inplace = True)

  def read_data(self, path):
    self.data = pd.read_csv(path, sep='\t')
    self.data = self.data.query('Gender != -1')
    print('Data size:', len(self.data))

  def add_protected_column(self):
    self.data['protected'] = self.data.Gender.apply(lambda x: int(x == 0))

  def run_evaluations(self, metrics):
    for m in metrics:
      print(m, calculateNDFairnes(recs=self.data, truth=[], metric=m, protected_varible='protected'))


if __name__ == '__main__':
  sample = sys.argv[1]
  if (sample == 'None'):
    sample = None
  else:
    sample = int(sample)
  centrality = sys.argv[2]
  path = sys.argv[3]
  metrics = ['rND', 'rKL', 'rRD', 'equal_ex']
  print('Evaluating', path, 'with sample size', sample, 'and centrality', centrality)
  eval = Evaluator(path, centrality, sample)
  eval.run_evaluations(metrics)