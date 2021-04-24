import pandas as pd
import numpy as np
from MAG_network import CitationNetwork

from matplotlib import pyplot as plt

import sys

sys.path.insert(0,"/home/laal/MAG/CentralityFairness/Evaluations")

from Evaluations.Evaluator import Evaluator


plt.style.use('ggplot')

econ_centrality = pd.read_csv("/home/laal/MAG/DATA/NETWORKS/SimpleWeightEconomics2020CentralityGendered.csv", 
                              sep="\t").query("Gender != -1")
psych_centrality = pd.read_csv("/home/laal/MAG/DATA/NETWORKS/SimpleWeightPsychology2020CentralityGendered.csv", 
                              sep="\t").query("Gender != -1")

METRICS = ['rND']
CENTRALITIES = ['PageRank', 'PageRank05', 'InDegreeStrength', 'Rank']
cutpoint = 10
    
def evaluate(centrality, data):
    eval = Evaluator(centrality=centrality, data=data)
    return eval.run_evaluations(METRICS)

score_records_econ = []
score_records_psych = []


for samplen in range(20000, econ_centrality.shape[0], 20000):

    print("Current sample size: {} / {}".format(samplen, econ_centrality.shape[0]))

    scores_econ = evaluate('PageRank', econ_centrality.sample(samplen, random_state=samplen))
    score_records_econ.append(scores_econ)
    
    scores_psych = evaluate('PageRank', psych_centrality.sample(samplen, random_state=samplen))
    score_records_psych.append(scores_psych)
    
econ_scores = pd.DataFrame.from_records(score_records_econ)
psych_scores = pd.DataFrame.from_records(score_records_psych)

econ_global = evaluate('PageRank', econ_centrality)
psych_global = evaluate('PageRank', psych_centrality)

econ_scores.index = list(range(20000, econ_centrality.shape[0], 20000))
psych_scores.index = list(range(20000, econ_centrality.shape[0], 20000))

plt.figure(figsize=(12, 8))
econ_scores['rND'].plot(style='-o', figsize=(12,8), label="Economics")
psych_scores['rND'].plot(style='-o', figsize=(12,8), label="Psychology")

plt.axhline(y=econ_global['rND'], label="Economics global", linestyle='--', alpha=0.4, color="orangered")
plt.axhline(y=psych_global['rND'], label="Psychology global", linestyle='--', alpha=1, color="lightblue")


plt.ylim(0.08, 0.14)
plt.title("rND with increasing sample sizes (PageRank). Cutpoint = {}".format(cutpoint))
plt.ylabel('rND')
plt.xlabel("Sample size")
plt.legend()
plt.savefig("rnd_sample_sizes_cutpoint_{}.png".format(cutpoint))
plt.show()