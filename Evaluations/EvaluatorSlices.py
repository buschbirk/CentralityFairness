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

CHUNK_SIZE = 1000000

class SliceEvaluator:

  def __init__(self, file_name, centrality, metrics, destination, slices):
    self.centrality = centrality
    self.results = []
    self.metrics = metrics
    self.slices = slices
    self.process_data(file_name, destination)
    
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

  def process_data(self, path, destination):
    i = 0
    while i < self.slices:
      _slice = []
      for chunk in pd.read_csv(path, sep = '\t', chunksize = CHUNK_SIZE):
        _slice.append(chunk[chunk.sliceid == i])
      df_slice = pd.concat(_slice)
      if df_slice.empty:
        return
      df_slice = df_slice.query('Gender != -1')
      # TODO :) df_slice = df_slice.query('InDegree != 0')
      df_slice.rename(columns = {'AuthorId': 'item', self.centrality: 'rank'}, inplace = True)
      self.process_slice(df_slice, i)
      i += 1
    self.save_results(destination)

  def process_slice(self, df_slice, slice_index):
    df_slice['protected'] = df_slice.Gender.apply(lambda x: int(x == 0))
    self.sort_data(df_slice)
    self.add_protected_column(df_slice)
    evaluations = {'index': slice_index}
    for m in self.metrics:
      evaluations[m] = self.calculate_fairness(df_slice, m)
    self.results.append(evaluations)

  def calculate_fairness(self, df_slice, metric):
    return calculateNDFairnes(recs=df_slice, truth=[], metric=metric, protected_varible='protected')

  def add_protected_column(self, data):
    data['protected'] = data.Gender.apply(lambda x: int(x == 0))

  def save_results(self, destination):
    final_results = pd.DataFrame.from_records(self.results)
    final_results.to_csv(destination, index = False)
    print(self.results)

if __name__ == '__main__':
  slices = int(sys.argv[1])
  centrality = sys.argv[2]
  path = sys.argv[3]
  destination = sys.argv[4]
  metrics = ['rND', 'rKL', 'rRD', 'equal_ex']
  print('Evaluating', path, 'for centrality', centrality)
  eval = SliceEvaluator(path, centrality, metrics, destination, slices)
  # eval.print_results()