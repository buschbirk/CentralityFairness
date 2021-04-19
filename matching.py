
import os, sys
import pandas as pd
import numpy as np
from visualizations import plot_group_dist, plot_side_by_side, plot_matched_side_by_side
import random


sys.path.insert(0, "Evaluations")

from Evaluations.Evaluator import Evaluator

from tqdm import tqdm

METRICS = ['rND', 'rKL', 'rRD', 'equal_ex']
CENTRALITIES = ['PageRank', 'PageRank05', 'InDegreeStrength', 'Rank']

class Matcher():

    def __init__(self, centrality_df, random_seed, max_year_limit=2018, min_year_tolerance=0, 
                 base_filepath="/home/agbe/MAG", fos_name="Economics"):

        self.cent_df = centrality_df.query("Gender != -1")
        self.seed = random_seed
        self.max_year_limit = max_year_limit
        self.min_year_tolerance = min_year_tolerance
        self.fos_name = fos_name

        self.author_df = None

        pd.options.mode.chained_assignment = None

        self.female_population = None
        self.male_population = None

        self.base_filepath = base_filepath

    def load_authors(self, fos_id=162324750, folder_destination = "/home/agbe/MAG/DATA/AuthorMetadataField.csv"):

        # base_destination = "/home/laal/MAG/DATA/AuthorMetadataField.csv"
        
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
        

        # filter out unknown gender and authors not in centrality dataset
        self.author_df = author_df.query("Gender != -1")
        self.author_df = self.author_df[self.author_df.AuthorId.isin(self.cent_df.AuthorId)]


        # affiliation_quintile and max year mapping 
        self.author_df.loc[:,"AffiliationBin"] = pd.qcut(self.author_df.MinAffiliationRank, 15, labels = False)
        self.author_df.loc[:,'MaxYear'] = self.author_df.MaxPubYear.apply(lambda x: 2021 if x >= self.max_year_limit else x)

        self.male_population = self.author_df.query("Gender == 1")
        self.female_population = self.author_df.query("Gender == 0") 


    def random_sample(self):

        smallest_pop_size = min(self.female_population.shape[0], self.male_population.shape[0])
        
        if self.female_population.shape[0]  < self.male_population.shape[0]:
            male_sampled = self.male_population.sample(smallest_pop_size, replace=False, random_state=self.seed)
            female_sampled = self.female_population.sample(frac=1) 
        else:
            male_sampled = self.male_population.sample(frac=1)
            female_sampled = self.female_population.sample(smallest_pop_size, replace=False, random_state=self.seed)

        return male_sampled, female_sampled   

    def matched_sample(self): 
        # shuffle male population using seed
        male_population = self.male_population.sample(frac=1, random_state=self.seed).copy()
        sample_females = []
        sample_males = []

        # shuffle female population and iterate over each row as dict (record) 
        for female_record in tqdm(self.female_population.sample(frac=1, random_state=self.seed).to_dict('records'), 
                                total=len(self.female_population)):
            
            # identify all matching candidates
            male_filtered = male_population[ 
                  (abs(male_population.MinPubYear - female_record['MinPubYear']) <= self.min_year_tolerance) &
                  (male_population.MaxYear == female_record['MaxYear']) &
                  (male_population.AffiliationBin == female_record['AffiliationBin'])
                ]
            
            # if no candidates found, continue to next record
            if male_filtered.empty:
                continue
            
            # sample a single record
            male_sample = male_filtered.sample(1, random_state=self.seed)

            
            # remove sample from male_population
            male_population.drop(male_sample.index, axis='index', inplace=True)
            
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
        eval = Evaluator(centrality=centrality, data=data)
        return eval.run_evaluations(METRICS)

    def evaluateAll(self, data, field):
        results = []
        destination = "/home/agbe/MAG/CentralityFairness/EVALUATIONS_OUTPUTS/" + field + "_" + centr + ".csv"
        for centr in CENTRALITIES:
            evaluations = {'centr': centr}
            evaluations = self.evaluate(centrality=centr, data = data)
            results = self.evaluate(centrality=centr, data = data)
            destination = self.base_filepath + "/CentralityFairness/EVALUATIONS_OUTPUTS/" + field + "_" + centr + ".csv"
            print("Destination:", destination)
            self.save_results(destination, results)
            results.append(evaluations)
        self.save_results()
        # FIXME virker ikke

    def save_results(self, destination, results):
        final_results = pd.DataFrame.from_dict(results)
        final_results.to_csv(destination)

    def save_visualizations(self, field, centrality_df, random_centrality_df, matched_centrality_df):
        
        # loop over each centrality 
        for centr in CENTRALITIES:
            # visualize TOP N (and top 10 % and 1 %) and save to file
            plot_side_by_side(centrality_df, field, interval=1000, figsize=(25,8), centrality=centr, 
            filepath=self.base_filepath + "/CentralityFairness/EVALUATIONS_PLOTS/" + field + "_" + centr + "_topn_visualization.png")

            # visualize matched centrality datasets
            plot_matched_side_by_side(centrality_df.query("Gender != -1"), field, random_centrality_df, 
                                      matched_centrality_df, interval=1000, figsize=(15,6), centrality=centr,
                                      filepath=self.base_filepath + \
                                      "/CentralityFairness/EVALUATIONS_PLOTS/" + field + "_" + centr + "_match_visualization.png")


if __name__ == '__main__':
    # FILE_PATH FIELD
    file_path = sys.argv[1] # "/home/agbe/MAG/CentralityFairness/Evaluations/economics2020.csv"
    field = sys.argv[2]     # Economics 
    field_id = sys.argv[3]  # 162324750

    data = pd.read_csv(file_path, sep="\t")
    matcher = Matcher(centrality_df = data, random_seed=12, base_filepath="/home/laal/MAG", fos_name=field)
    matcher.load_authors(fos_id=int(field_id), folder_destination = "/home/laal/MAG/DATA/AuthorMetadataField.csv")

    # step 1
    random_data_males, random_data_females = matcher.random_sample()
    random_data = matcher.samples_to_centrality_data(random_data_males, random_data_females)

    print("Sampled {} males and {} females randomly. Total {} records".format(len(random_data_males), len(random_data_females),
    len(random_data)))
    #print(matcher.evaluate(centrality='PageRank', data=random_data))
    # matcher.evaluateAll(data=random_data, field=field)

    # step 2
    matched_data_males, matched_data_females = matcher.matched_sample()
    matched_data = matcher.samples_to_centrality_data(matched_data_males, matched_data_females, store=True, is_matched=True)
    # matcher.evaluateAll(data=matched_data, field = field)  
    print("done")


    print("Matched {} males and {} females. Total {} records".format(len(matched_data_males), len(matched_data_females),
    len(matched_data)))

    matcher.save_visualizations(field=field, centrality_df=data, random_centrality_df=random_data, 
                                matched_centrality_df=matched_data)