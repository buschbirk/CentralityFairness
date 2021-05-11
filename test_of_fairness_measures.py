import pandas as pd
import numpy as np
from MAG_network import CitationNetwork

from matplotlib import pyplot as plt

import sys

sys.path.insert(0,"/home/laal/MAG/CentralityFairness/Evaluations")

from Evaluations.Evaluator import Evaluator


plt.style.use('ggplot')



METRICS = ['rND']
CENTRALITIES = ['PageRank', 'PageRank05', 'InDegreeStrength', 'Rank']
cutpoint = 10
    
def evaluate(centrality, data):
    eval = Evaluator(centrality=centrality, data=data)
    return eval.run_evaluations(METRICS)

score_records_econ = []
score_records_psych = []

print("EVALUATING CUTPOINT: {}".format(cutpoint))

econ_centrality = pd.read_csv("/home/laal/MAG/DATA/NETWORKS/SimpleWeightEconomics2020CentralityGendered.csv", 
                              sep="\t").query("Gender != -1")
psych_centrality = pd.read_csv("/home/laal/MAG/DATA/NETWORKS/SimpleWeightPsychology2020CentralityGendered.csv", 
                              sep="\t").query("Gender != -1")


for samplen in range(20000, econ_centrality.shape[0], 20000):
    scores_econ = evaluate('PageRank', econ_centrality.sample(samplen, random_state=samplen))
    score_records_econ.append(scores_econ)
    
    scores_psych = evaluate('PageRank', psych_centrality.sample(samplen, random_state=samplen))
    score_records_psych.append(scores_psych)
    
econ_scores = pd.DataFrame.from_records(score_records_econ)
psych_scores = pd.DataFrame.from_records(score_records_psych)    

econ_scores.to_csv("/home/laal/MAG/CentralityFairness/METRICS/economics_scores_cutpoint_{}.csv".format(cutpoint), index=False)
psych_scores.to_csv("/home/laal/MAG/CentralityFairness/METRICS/psychology_scores_cutpoint_{}.csv".format(cutpoint), index=False)

econ_global = evaluate('PageRank', econ_centrality)
psych_global = evaluate('PageRank', psych_centrality)       

print("Global rate economics with cutpoint {}: {}".format(cutpoint, econ_global))
print("Global rate psychology with cutpoint {}: {}".format(cutpoint, psych_global))
 