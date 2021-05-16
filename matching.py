import os, sys
import pandas as pd
import numpy as np
from visualizations import plot_group_dist, plot_side_by_side, plot_matched_side_by_side, compute_ks_test, plot_inter_event_cdf, plot_centrality_correlations
import random


sys.path.insert(0, "Evaluations")

from Evaluations.Evaluator import Evaluator

from tqdm import tqdm

METRICS = ['rND', 'rKL', 'rRD', 'equal_ex']
CENTRALITIES = ['PageRank', 'PageRank05', 'InDegreeStrength', 'Rank']

class Matcher():

    def __init__(self, centrality_df, random_seed, field, fos_id, 
                 max_year_limit=2018, min_year_tolerance=0, 
                 base_filepath="/home/agbe/MAG", mag=None):

        self.cent_df_raw = centrality_df
        self.cent_df = centrality_df.query("Gender != -1")
        self.seed = random_seed
        self.max_year_limit = max_year_limit
        self.min_year_tolerance = min_year_tolerance
        self.fos_name = field
        self.field = field

        self.fos_id = fos_id
        self.mag = mag

        self.author_df = None

        pd.options.mode.chained_assignment = None

        self.female_population = None
        self.male_population = None
        self.unknown_population = None

        self.base_filepath = base_filepath

    def load_authors(self, folder_destination = "/home/agbe/MAG/DATA/AuthorMetadataField.csv"):

        # base_destination = "/home/laal/MAG/DATA/AuthorMetadataField.csv"
        fos_id = self.fos_id
        columns = ['AuthorId', 'FieldOfStudyId', 'Gender', 'MinAffiliationRank', 'NumPapers', 'MinPubDate', 'MaxPubDate', 'PubsPerYear']
        
        author_df = pd.DataFrame()
        
        for file in os.listdir(folder_destination):
            if file.endswith('.csv'):
                df = pd.read_csv(folder_destination + "/" + file, names=columns, sep="\t")
                author_df = pd.concat([author_df, df.query("FieldOfStudyId == {}".format(fos_id))])
        
        # parse datetimes
        author_df['MinPubDate'] = pd.to_datetime(author_df['MinPubDate'])
        author_df['MaxPubDate'] = pd.to_datetime(author_df['MaxPubDate'])
        
        author_df['MinPubYear'] = author_df['MinPubDate'].apply(lambda x: x.year)
        author_df['MaxPubYear'] = author_df['MaxPubDate'].apply(lambda x: x.year)
        
        unknowns_in_ranking = author_df[author_df.AuthorId.isin(self.cent_df_raw.AuthorId)].query("Gender == -1")
        
        self.unknown_population = author_df.query("Gender == -1 and NumPapers > 1")        

        # filter out unknown gender and authors not in centrality dataset
        self.author_df = author_df.query("Gender != -1")
        self.author_df = self.author_df[self.author_df.AuthorId.isin(self.cent_df.AuthorId)]


        # affiliation_quintile and max year mapping 
        self.author_df.loc[:,"AffiliationBin"] = pd.qcut(self.author_df.MinAffiliationRank, 15, labels = False)
        self.author_df.loc[:,'MaxYear'] = self.author_df.MaxPubYear.apply(lambda x: 2021 if x >= self.max_year_limit else x)
        print("Results of filtering:")
        print("  - Men with fewer than 2 papers: ", self.author_df.query("Gender == 1 and NumPapers < 2").shape[0])
        print("  - Women with fewer than 2 papers: ", self.author_df.query("Gender == 0 and NumPapers < 2").shape[0])
        print("  - Non-genderized with fewer than 2 papers: ", unknowns_in_ranking.query("NumPapers < 2").shape[0])


        print("  - Men with more than 1 paper: ", self.author_df.query("Gender == 1 and NumPapers > 1").shape[0])
        print("  - Women with more than 1 paper: ", self.author_df.query("Gender == 0 and NumPapers > 1").shape[0])
        print("  - Non-genderized with more than 1 paper: ", unknowns_in_ranking.query("NumPapers > 1").shape[0])
        print()

        
        self.male_population = self.author_df.query("Gender == 1 and NumPapers > 1")
        self.female_population = self.author_df.query("Gender == 0 and NumPapers > 1") 

        self.cent_df = self.samples_to_centrality_data(self.male_population, self.female_population)


    def random_sample(self):

        smallest_pop_size = min(self.female_population.shape[0], self.male_population.shape[0])
        
        if self.female_population.shape[0]  < self.male_population.shape[0]:
            male_sampled = self.male_population.sample(smallest_pop_size, replace=False, random_state=self.seed)
            female_sampled = self.female_population.sample(frac=1) 
        else:
            male_sampled = self.male_population.sample(frac=1)
            female_sampled = self.female_population.sample(smallest_pop_size, replace=False, random_state=self.seed)

        return male_sampled, female_sampled

    def matched_sample(self, verbose=False): 
        # shuffle male population using seed
        male_population = self.male_population.sample(frac=1, random_state=self.seed).copy()
        female_population = self.female_population.sample(frac=1, random_state=self.seed).copy()
            
        male_population = male_population[~pd.isnull(male_population.AffiliationBin)]
        female_population = female_population[~pd.isnull(female_population.AffiliationBin)]

        if verbose:
            print("  -  Number of men after removing NaN-affiliation bin: {}".format(male_population.shape[0]))
            print("  -  Number of women after removing NaN-affiliation bin: {}".format(female_population.shape[0]))

        sample_females = []
        sample_males = []

        # separate dataset according to min-year and max-year
        matcher_datasets = {}
        for min_year in male_population.MinPubYear.unique():
            matcher_datasets[min_year] = {}

            for max_year in male_population.MaxYear.unique():
                matcher_datasets[min_year][max_year] = male_population[ 
                  (male_population.MinPubYear == min_year) &
                  (male_population.MaxYear == max_year)
                ]

        # shuffle female population and iterate over each row as dict (record) 
        for female_record in tqdm(female_population.to_dict('records'), 
                                  total=len(female_population)):
            
            if female_record['MinPubYear'] not in matcher_datasets or female_record['MaxYear'] not in matcher_datasets[female_record['MinPubYear']]:
                continue

            # identify all matching candidates
            male_filtered_df = matcher_datasets[female_record['MinPubYear']][female_record['MaxYear']]

            if male_filtered_df.empty:
                continue

            male_filtered = male_filtered_df[male_filtered_df.AffiliationBin == female_record['AffiliationBin']]
                        
            # if no candidates found, continue to next record
            if male_filtered.empty:
                continue
            
            # sample a single record
            male_sample = male_filtered.sample(1, random_state=self.seed)

            
            # remove sample from male_population
            matcher_datasets[female_record['MinPubYear']][female_record['MaxYear']].drop(male_sample.index, axis='index', inplace=True)
            
            # append male and female samples as dicts
            sample_females.append(female_record)
            sample_males.append(male_sample.iloc[0].to_dict())
            

        print("Found {} matches to {} females".format(len(sample_females), len(self.female_population)))
        
        females_df = pd.DataFrame.from_records(sample_females)
        males_df = pd.DataFrame.from_records(sample_males)        

        return  males_df, females_df

    def samples_to_centrality_data(self, males, females, store=False, is_matched=False):
        ids = list(males.AuthorId.values) + list(females.AuthorId.values)
        cent_df_sampled = self.cent_df[self.cent_df.AuthorId.isin(ids)]

        if store:
            cent_df_sampled.to_csv(self.base_filepath + \
            "/CentralityFairness/MATCHING_OUTPUTS/{}_seed{}_{}.csv".format(self.fos_name, self.seed, 'matched' if is_matched else 'random') 
            , index=False, sep="\t")

        return cent_df_sampled

    def evaluate(self, centrality, data):
        eval = Evaluator(centrality=centrality, data=data, verbose=False)
        return eval.run_evaluations(METRICS)

    def evaluateAll(self, random_data, matched_data, i):
        results = []
        for centr in CENTRALITIES:
            evaluations_true = self.evaluate(centrality=centr, data=self.cent_df)
            evaluations_random = self.evaluate(centrality=centr, data = random_data)
            evaluations_matched = self.evaluate(centrality = centr, data = matched_data)
            
            results.append(self.save_line(centr, 'true', evaluations_true, i))
            results.append(self.save_line(centr, 'random', evaluations_random, i))
            results.append(self.save_line(centr, 'matched', evaluations_matched, i))

        return results
        # FIXME virker ikke

    def save_line(self, centrality, type, evaluations, i=0):
        line = evaluations
        line['centrality'] = centrality
        line['type'] = type
        line['iteration'] = i
        return line

    def save_results(self, destination, results):
        final_results = pd.DataFrame.from_records(results)
        final_results.to_csv(destination, index=False, sep="\t")

    def save_visualizations(self, centrality_df, random_centrality_df, matched_centrality_df):
        
        field = self.field

        # loop over each centrality 
        for centr in CENTRALITIES:        
            if centr == 'Rank':
                centrality_df['MAG Rank'] = centrality_df['Rank'].apply(lambda x: x*-1)
                random_centrality_df['MAG Rank'] = random_centrality_df['Rank'].apply(lambda x: x*-1)
                matched_centrality_df['MAG Rank'] = matched_centrality_df['Rank'].apply(lambda x: x*-1)

                centr = 'MAG Rank'

            # visualize TOP N (and top 10 % and 1 %) and save to file
            plot_side_by_side(centrality_df, field, interval=1000, figsize=(25,8), centrality=centr, 
            filepath=self.base_filepath + "/CentralityFairness/EVALUATIONS_PLOTS/" + field + "_" + centr + "_topn_visualization_seed_{}.png".format(self.seed))

            # visualize matched centrality datasets
            plot_matched_side_by_side(centrality_df.query("Gender != -1"), field, random_centrality_df, 
                                      matched_centrality_df, interval=1000, figsize=(20, 7.5), centrality=centr,
                                      filepath=self.base_filepath + \
                                      "/CentralityFairness/EVALUATIONS_PLOTS/" + field + "_" + centr + "_match_visualization_seed_{}.png".format(self.seed))
        
        datediffs, ks_test = compute_ks_test(self.mag, matched_centrality_df, fos_id=self.fos_id, base_filepath=self.base_filepath + "/DATA")

        plot_inter_event_cdf(datediffs, ks_test, field, 
                                filepath=self.base_filepath +  '/CentralityFairness/EVALUATIONS_PLOTS/' + \
                                field + "_interevent_match_visualization_seed_{}.png".format(self.seed)
                            )

        plot_centrality_correlations(field, matched_centrality_df, 
                                     filename=self.base_filepath +  '/CentralityFairness/EVALUATIONS_PLOTS/' + \
                                     field + "_centrality_match_visualization_seed_{}.png".format(self.seed)
                                    )


    def cycle(self, i, visualize=False, verbose=False):
        self.seed = i
        random_data_males, random_data_females = self.random_sample()
        random_data = self.samples_to_centrality_data(random_data_males, random_data_females)

        print("Sampled {} men and {} women randomly. Total {} records".format(len(random_data_males), len(random_data_females),
        len(random_data)))

        # step 2
        matched_data_males, matched_data_females = self.matched_sample(verbose=verbose)
        matched_data = self.samples_to_centrality_data(matched_data_males, matched_data_females, store=True, is_matched=True)

        results = self.evaluateAll(random_data=random_data, matched_data=matched_data, i=i)

        if visualize:
            print("Visualizing matching results")
            all_authors = list(self.male_population.AuthorId.values) + list(self.female_population.AuthorId.values) + \
                            list(self.unknown_population.AuthorId.values)
            centrality_raw = self.cent_df_raw[self.cent_df_raw.AuthorId.isin(all_authors)]
            self.save_visualizations(centrality_df=centrality_raw, random_centrality_df=random_data, 
                                        matched_centrality_df=matched_data)
            print("Visualizations stored in {}".format(self.base_filepath +  '/CentralityFairness/EVALUATIONS_PLOTS/' ))

        return results

    def repeatCycles(self, destination, visualize_seeds=[0]):
        evaluations = []
        for i in range(50):
            print("Completed ", i, "cycles")
            evaluations += self.cycle(i=i, visualize=i in visualize_seeds)
            self.save_results(destination, evaluations)

        return evaluations





if __name__ == '__main__':
    # FILE_PATH FIELD FIELD_ID BASE_FILEPATH
    file_path = sys.argv[1] # "/home/agbe/MAG/CentralityFairness/Evaluations/economics2020.csv"
    field = sys.argv[2]     # Economics 
    field_id = sys.argv[3]  # 162324750
    base_filepath = sys.argv[4] # "/home/agbe/MAG"
    spark_cluster_job_id = int(sys.argv[5]) # 45069 (SLURM job id for active Spark session)

    mag, spark = MAGspark.get_mag_with_cluster_connection(jobid=spark_cluster_job_id, memory_per_executor=14000,
                                                          data_folderpath=base_filepath + "/DATA/")

    destination = base_filepath + "/CentralityFairness/EVALUATIONS_OUTPUTS/matching_repeated_" + field + ".csv"

    data = pd.read_csv(file_path, sep="\t")
    matcher = Matcher(centrality_df = data, random_seed=99, field=field, fos_id=int(field_id), base_filepath=base_filepath, mag=mag)
    matcher.load_authors(folder_destination = base_filepath + "/DATA/AuthorMetadataField.csv")

    # step 1
    # random_data_males, random_data_females = matcher.random_sample()
    # random_data = matcher.samples_to_centrality_data(random_data_males, random_data_females)

    ##print("Sampled {} men and {} women randomly. Total {} records".format(len(random_data_males), len(random_data_females),
    ##len(random_data)))

    # step 2
    # matched_data_males, matched_data_females = matcher.matched_sample()
    # matched_data = matcher.samples_to_centrality_data(matched_data_males, matched_data_females, store=True, is_matched=True)
    
    ## total_results = matcher.evaluateAll(random_data=random_data, matched_data=matched_data, field=field)
    ## matcher.save_results(destination, total_results)
    ## print("Matched {} men and {} women. Total {} records".format(len(matched_data_males), len(matched_data_females),
    ## len(matched_data)))

    matcher.repeatCycles(destination=destination)

    #matcher.save_visualizations(field=field, centrality_df=data, random_centrality_df=random_data, 
      #                          matched_centrality_df=matched_data)