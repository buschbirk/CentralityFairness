import os, sys, inspect

import numpy as np
import pandas as pd
import math
import pickle

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

from lenskit.metrics.topnFair import *

pd.options.mode.chained_assignment = None

CHUNK_SIZE = 100000
SLICES = 30

class SliceEvaluator:

  def __init__(self, file_name, centrality, metrics):
    self.centrality = centrality
    self.results = []
    self.metrics = metrics
    self.process_data(file_name)
    # print('gender counts normalized:')
    # print(self.data.Gender.value_counts(normalize = True))
    # print('gender counts:')
    # print(self.data.Gender.value_counts(normalize = False))
    # self.data.rename(columns = {'AuthorId': 'item', self.centrality: 'rank'}, inplace = True)
    # self.sort_data()
    # print('sorted data.head():')
    # print(self.data.head())

  def sort_data(self, data):
    if self.centrality == 'Rank':
      data.sort_values(by = 'rank', ascending = True, inplace = True) 
    else: 
      data.sort_values(by = 'rank', ascending = False, inplace = True)

  def process_data(self, path):
    for i in range(SLICES):
      _slice = []
      for chunk in pd.read_csv(path, sep = '\t', chunksize = CHUNK_SIZE):
        _slice.append(chunk[chunk.sliceid==i])
      df_slice = pd.concat(_slice)
      df_slice = df_slice.query('Gender != -1')
      print('Gender counts before filtering on InDegree:')
      print(df_slice.Gender.value_counts(normalize = False))
      df_slice = df_slice.query('InDegree != 0')
      print('\nGender counts before after on InDegree:')
      print(df_slice.Gender.value_counts(normalize = False))
      df_slice.rename(columns = {'AuthorId': 'item', self.centrality: 'rank'}, inplace = True)
      self.process_slice(df_slice, i, self.metrics)

  def process_slice(self, df_slice, slice_index, metrics):
    df_slice['protected'] = df_slice.Gender.apply(lambda x: int(x == 0))
    self.sort_data(df_slice)
    self.add_protected_column(df_slice)
    evaluations = {'index': slice_index}
    for m in metrics:
      evaluations[m] = self.calculate_fairness(df_slice, m)
    self.results.append(evaluations)
    self.print_results()
    self.results = []

  def calculate_fairness(self, df_slice, metric):
    return calculateNDFairnes(recs=df_slice, truth=[], metric=metric, protected_varible='protected')

  def add_protected_column(self, data):
    data['protected'] = data.Gender.apply(lambda x: int(x == 0))

  def print_results(self):
    print(self.results)

if __name__ == '__main__':
  sample = sys.argv[1]
  centrality = sys.argv[2]
  path = sys.argv[3]
  metrics = ['rND', 'rKL', 'rRD', 'equal_ex']
  print('Evaluating', path, 'for centrality', centrality)
  eval = SliceEvaluator(path, centrality, metrics)
  # eval.print_results()