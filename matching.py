
import os 
import pandas as pd
import numpy as np
from visualizations import plot_group_dist, plot_side_by_side
import random

from tqdm import tqdm


def read_author_metadata(fos_id=162324750):
    base_destination = "/home/laal/MAG/DATA/AuthorMetadataField.csv"
    
    columns = ['AuthorId', 'FieldOfStudyId', 'Gender', 'MinAffiliationRank', 'NumPapers', 'MinPubDate', 'MaxPubDate', 'PubsPerYear']
    
    author_df = pd.DataFrame()
    
    for file in os.listdir(base_destination):
        if file.endswith('.csv'):
            df = pd.read_csv(base_destination + "/" + file, names=columns, sep="\t")
            author_df = pd.concat([author_df, df.query("FieldOfStudyId == {}".format(fos_id))])
    
    # parse datetimes
    author_df['MinPubDate'] = pd.to_datetime(author_df['MinPubDate'])
    author_df['MaxPubDate'] = pd.to_datetime(author_df['MaxPubDate'])
    
    author_df['MinPubYear'] = author_df['MinPubDate'].apply(lambda x: x.year)
    author_df['MaxPubYear'] = author_df['MaxPubDate'].apply(lambda x: x.year)
    
    return author_df



class Matcher():

    def __init__(self, centrality_df, random_seed, max_year_limit=2018, min_year_tolerance=0):

        self.cent_df = centrality_df
        self.seed = random_seed
        self.max_year_limit = max_year_limit
        self.min_year_tolerance = min_year_tolerance

        self.author_df = None

        pd.options.mode.chained_assignment = None

        self.female_population = None
        self.male_population = None

    def load_authors(self, fos_id=162324750, folder_destination = "/home/laal/MAG/DATA/AuthorMetadataField.csv"):

        # base_destination = "/home/laal/MAG/DATA/AuthorMetadataField.csv"
        
        columns = ['AuthorId', 'FieldOfStudyId', 'Gender', 'MinAffiliationRank', 'NumPapers', 'MinPubDate', 'MaxPubDate', 'PubsPerYear']
        
        author_df = pd.DataFrame()
        
        for file in os.listdir(base_destination):
            if file.endswith('.csv'):
                df = pd.read_csv(base_destination + "/" + file, names=columns, sep="\t")
                author_df = pd.concat([author_df, df.query("FieldOfStudyId == {}".format(fos_id))])
        
        # parse datetimes
        author_df['MinPubDate'] = pd.to_datetime(author_df['MinPubDate'])
        author_df['MaxPubDate'] = pd.to_datetime(author_df['MaxPubDate'])
        
        author_df['MinPubYear'] = author_df['MinPubDate'].apply(lambda x: x.year)
        author_df['MaxPubYear'] = author_df['MaxPubDate'].apply(lambda x: x.year)
        

        # filter out unknown gender and authors not in centrality dataset
        self.author_df = author_df.query("Gender != -1")
        self.author_df = self.author_df[self.author_df.AuthorId.isin(self.centr_df.AuthorId)]


        # affiliation_quintile and max year mapping 
        self.author_df.loc[:,"AffiliationBin"] = pd.qcut(self.author_df.MinAffiliationRank, 15, labels = False)
        self.author_df.loc[:,'MaxYear'] = self.author_df.MaxPubYear.apply(lambda x: 2021 if x >= self.max_year_limit else x)

        self.male_population = self.author_df.query("Gender == 1")
        self.female_population = self.author_df.query("Gender == 0") 


    def random_sample(self):

        smallest_pop_size = min(self.female_population.shape[0], self.male_population.shape[0])
        
        if self.female_population.shape[0]  < self.male_population.shape[0]:
            male_sampled = self.male_population.sample(self.female_population.shape[0], replace=False, random_state=self.seed)
            female_sampled = self.female_population     
        else:
            male_sampled = self.male_population.sample(frac=1)
            female_sampled = self.female_population.sample(self.male_population.shape[0], replace=False, random_state=self.seed)   

        return male_sampled, female_sampled   

    def matched_sample(self): 
        # shuffle male population using seed
        male_population = self.male_population.sample(frac=1, random_state=random_seed).copy()

        # shuffle female population and iterate over each row as dict (record) 
        for female_record in tqdm(self.female_population.sample(frac=1, random_state=random_seed).to_dict('records'), 
                                total=len(self.female_population)):
            
            # identify all matching candidates
            male_filtered = male_population[ 
                  (abs(male_population.MinPubYear - female_record['MinPubYear']) <= min_year_tolerance) &
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
            

        print("Found {} matches to {} females".format(len(sample_females), len(female_population)))
        
        females_df = pd.DataFrame.from_records(sample_females)
        males_df = pd.DataFrame.from_records(sample_males)        


        return females_df, males_df 


if __name__ == '__main__':
    pass