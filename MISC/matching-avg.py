import pandas as pd
import os

data = pd.read_csv("/home/agbe/MAG/CentralityFairness/EVALUATIONS_OUTPUTS/matching_repeated_Chemistry.csv", sep="\t")

true_pr = data.query("centrality == 'PageRank' and type == 'true'").mean()
random_pr = data.query("centrality == 'PageRank' and type == 'random'").mean()
matched_pr = data.query("centrality == 'PageRank' and type == 'matched'").mean()

true_pr05 = data.query("centrality == 'PageRank05' and type == 'true'").mean()
random_pr05 = data.query("centrality == 'PageRank05' and type == 'random'").mean()
matched_pr05 = data.query("centrality == 'PageRank05' and type == 'matched'").mean()

true_ids = data.query("centrality == 'InDegreeStrength' and type == 'true'").mean()
random_ids = data.query("centrality == 'InDegreeStrength' and type == 'random'").mean()
matched_ids = data.query("centrality == 'InDegreeStrength' and type == 'matched'").mean()

true_rank = data.query("centrality == 'Rank' and type == 'true'").mean()
random_rank = data.query("centrality == 'Rank' and type == 'random'").mean()
matched_rank = data.query("centrality == 'Rank' and type == 'matched'").mean()


data.query("centrality == 'Rank' and type == 'true'").std()

destination = "/home/agbe/MAG/CentralityFairness/EVALUATIONS_OUTPUTS/new-chem_repeated_measures_avg.csv"

# true
results_df = pd.DataFrame()
results_df['true_PageRank_mean'] = true_pr
results_df['true_PageRank_std'] = data.query("centrality == 'PageRank' and type == 'true'").std()

results_df['true_PageRank05_mean'] = true_pr05
results_df['true_PageRank05_std'] = data.query("centrality == 'PageRank05' and type == 'true'").std()

results_df['true_InDegreeStrength_mean'] = true_ids
results_df['true_InDegreeStrength_std'] = data.query("centrality == 'InDegreeStrength' and type == 'true'").std()

results_df['true_Rank_mean'] = true_rank
results_df['true_Rank_std'] = data.query("centrality == 'Rank' and type == 'true'").std()

# random matched 
results_df['random_PageRank_mean']          = random_pr
results_df['random_PageRank_std']           = data.query("centrality == 'PageRank' and type == 'random'").std()
results_df['random_PageRank05_mean']        = random_pr05
results_df['random_PageRank05_std']         = data.query("centrality == 'PageRank05' and type == 'random'").std()
results_df['random_InDegreeStrength_mean']  = random_ids
results_df['random_InDegreeStrength_std']   = data.query("centrality == 'InDegreeStrength' and type == 'random'").std()
results_df['random_Rank_mean']              = random_rank
results_df['random_Rank_std']               = data.query("centrality == 'Rank' and type == 'random'").std()


results_df['matched_PageRank_mean']         = matched_pr
results_df['matched_PageRank_std']          = data.query("centrality == 'PageRank' and type == 'matched'").std()
results_df['matched_PageRank05_mean']       = matched_pr05
results_df['matched_PageRank05_std']        = data.query("centrality == 'PageRank05' and type == 'matched'").std()
results_df['matched_InDegreeStrength_mean'] = matched_ids
results_df['matched_InDegreeStrength_std']  = data.query("centrality == 'InDegreeStrength' and type == 'matched'").std()
results_df['matched_Rank_mean']             = matched_rank
results_df['matched_Rank_std']              = data.query("centrality == 'Rank' and type == 'matched'").std()

results_df.to_csv(destination, index=False, sep="\t")