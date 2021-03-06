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

  def __init__(self, file_name="", centrality='PageRank', sample=None, data=None, verbose=False):
    if file_name != "":
      self.read_data(file_name)
    else:
      self.data = data.copy()
    if sample is not None:
      self.data = self.data.sample(sample)
    self.centrality = centrality

    self.verbose = verbose
    
    if self.verbose:
      print('gender counts normalized:\n')
      print(self.data.Gender.value_counts(normalize = True))
      print('gender counts: \n')
      print(self.data.Gender.value_counts(normalize = False))
    self.add_protected_column()
    self.data.rename(columns = {'AuthorId': 'item', self.centrality: 'rank'}, inplace = True)
    self.sort_data()

    if self.verbose:
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
    result = {}
    for m in metrics:
      result[m] = calculateNDFairnes(recs=self.data, truth=[], metric=m, protected_varible='protected')
    return result

  def run_and_print_evaluations(self, metrics):
    print(self.run_evaluations(metrics))

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
  eval = Evaluator(file_name=path, centrality=centrality, sample=sample)
  eval.run_and_print_evaluations(metrics)