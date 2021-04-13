
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


def match(centrality_df, author_df, random_seed, centrality='PageRank', 
          max_year_limit=2018, min_year_tolerance=0):
        
    pd.options.mode.chained_assignment = None
        
    # remove unknown gender
    dataset = author_df.query("Gender != -1")
    dataset = dataset[dataset.AuthorId.isin(centrality_df.AuthorId)]
    
    # affiliation_quintile 
    dataset.loc[:,"AffiliationBin"] = pd.qcut(dataset.MinAffiliationRank, 15, labels = False)
    dataset.loc[:,'MaxYear'] = dataset.MaxPubYear.apply(lambda x: 2021 if x >= max_year_limit else x)
    
    male_population = dataset.query("Gender == 1")
    female_population = dataset.query("Gender == 0")
    
    # sample random from opposite gender
    male_sampled = male_population.sample(female_population.shape[0], replace=False, random_state=random_seed)
    female_sampled = female_population
    
    print("Collected {} female and {} male samples".format(len(male_sampled), len(female_sampled)))
    
    ## Compute statistics and visualize    
    ids = list(male_sampled.AuthorId.values) + list(female_sampled.AuthorId.values)
    cent_df_sampled = cent_df[cent_df.AuthorId.isin(ids)]
    
    _, __ = plot_group_dist(cent_df_sampled, centrality, interval_size=100, max_N=len(cent_df_sampled), protected_group=0, 
                            unprotected=1, show_unknown=False, field_name='Economics', na_removed=True, 
                            ax=None, global_rates=None)
    
    # sample from population with same starting time and end time
    sample_females = []
    sample_males   = []
    
    print("Initiating matching on carreer characteristics")
    
    # shguffle male and female before sampling
    
    for female_record in tqdm(female_population.to_dict('records'), total=len(female_population)):
        
        
        male_filtered = male_population[  (abs(male_population.MinPubYear - female_record['MinPubYear']) <= min_year_tolerance)
                                        & (male_population.MaxYear == female_record['MaxYear']) 
                                        & (male_population.AffiliationBin == female_record['AffiliationBin'])]
        if male_filtered.empty:
            continue
        
        male_sample = male_filtered.sample(1, random_state=random_seed)
        # remove sample from male_population
        male_population.drop(male_sample.index, axis='index', inplace=True)
        
        sample_females.append(female_record)
        sample_males.append(male_sample.iloc[0].to_dict())
        
    print("Found {} matches to {} females".format(len(sample_females), len(female_population)))
    
    females_df = pd.DataFrame.from_records(sample_females)
    males_df = pd.DataFrame.from_records(sample_males)
    
    ## Compute statistics and visualize    
    ids = list(male_sampled.AuthorId.values) + list(female_sampled.AuthorId.values)
    cent_df_sampled = cent_df[cent_df.AuthorId.isin(ids)]
    
    _, __ = plot_group_dist(cent_df_sampled, centrality, interval_size=100, max_N=len(cent_df_sampled), protected_group=0, 
                            unprotected=1, show_unknown=False, field_name='Economics', na_removed=True, 
                            ax=None, global_rates=None)
    
    return sample_females, sample_males

if __name__ == '__main__':
    author_df = read_author_metadata()
    cent_df = pd.read_csv("/home/laal/MAG/DATA/NETWORKS/SimpleWeightEconomics2020CentralityGendered.csv", sep="\t")
    test = match(cent_df, author_df, random_seed=42, centrality='PageRank', 
                 max_year_limit=2018, min_year_tolerance=0)