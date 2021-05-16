from matplotlib import pyplot as plt
import seaborn as sns
import pandas as pd
import matplotlib
# import powerlaw

import numpy as np
import scipy
from scipy.stats import ks_2samp

import os 
import itertools

from MAG_network import CitationNetwork
# from matching import Matcher
import MAGspark
import warnings

warnings.filterwarnings( "ignore", module = "matplotlib\..*" )


colors = {
    'Psychology': '#617be3',
    'Chemistry' : '#27496d',
    'Mathematics': '#007580',
    'Economics': '#e8505b' 
}

matplotlib.rcParams['figure.figsize'] = (15.0, 10.0) # default plots are app. same size as notebook
plt.style.use('ggplot')



def plot_group_dist(centrality_df, centrality, interval_size, max_N, protected_group, unprotected, 
                    show_unknown=True, field_name=None, na_removed=False, ax=None, global_rates=None):
    
    if ax is None:
        fig, ax = plt.subplots()
    
    sorted_df = centrality_df.sort_values(by=centrality, ascending=False)
    
    if global_rates is not None:
        global_rate_protected = global_rates['protected']
        global_rate_unprotected = global_rates['unprotected']
    else:   
        global_rate_protected = sorted_df.Gender.value_counts(normalize=True)[protected_group]
        global_rate_unprotected = sorted_df.Gender.value_counts(normalize=True)[unprotected]
    
    normalizer = centrality_df.shape[0]
    
    xticks = []
    y_values = []
    y_values_unprotected = []
    y_values_unknown = []
    
    parity_x = None
    parity_pct = None
    
    for N in range(interval_size, max_N, interval_size):
        
        xticks.append((N / normalizer) * 100)
        top_n_df = sorted_df[:N]
        value_counts = top_n_df.Gender.value_counts(normalize=True)
        
        y_values.append(value_counts[protected_group] if protected_group in value_counts else 0)
        y_values_unprotected.append(value_counts[unprotected] if unprotected in value_counts else 0)
        
        abs_protected = abs(y_values[-1] - global_rate_protected)
        abs_unprotected = abs(y_values_unprotected[-1] - global_rate_unprotected)
        
        
        if parity_x is None and abs_protected <= 0.01 and abs_unprotected <= 0.01:
            parity_pct = (N / normalizer) * 100
            parity_x = N
        
        if show_unknown: y_values_unknown.append(value_counts[-1] if -1 in value_counts else 0)
    
    if show_unknown:
        global_rate_unknown = sorted_df.Gender.value_counts(normalize=True)[-1] \
        if global_rates is None else global_rates.get('unknown')
        ax.plot(xticks, y_values_unknown, '-o', label="N/A", markersize=3, color="#b8b8b8", alpha=0.2)
        ax.axhline(y=global_rate_unknown, label="Total population N/A", linestyle='--', alpha=0.8, color="#b8b8b8")
    
    
    ax.plot(xticks, y_values, '-o', label="Women", markersize=6, color="#6fc9f2")
    ax.axhline(y=global_rate_protected, label="Total population women", linestyle='--', alpha=1.0, color="#6fc9f2")
    
    
    ax.plot(xticks, y_values_unprotected, '-o', label="Men", markersize=6, color="#bd8aff")
    ax.axhline(y=global_rate_unprotected, label="Total population men", linestyle='--', alpha=1.0, color="#bd8aff")
    
    if parity_x is not None:
        ax.axvline(x=parity_pct, color="black", linestyle='-', label='Parity ($\pm$ 1 %)')
        ax.text(parity_pct - 6, 0.55, "{:,} ({} %)".format(parity_x, int(parity_pct)), rotation=90, alpha=0.8,
                fontsize=20)
    
    if global_rates is None:
        title = "Group membership in Top N rank ({})".format(centrality)
        title += ": {}".format(field_name) if field_name is not None else ""
        title += " Increment = {}".format(interval_size)
        title += ". N/A removed" if na_removed else ""
        ax.set_title(title, fontsize=12)

    ax.set_ylabel("Proportion")
    ax.set_xlabel("Top N")
    
    ax.legend()
    
    if ax is None:
        plt.show()
    
    return y_values, xticks



def plot_side_by_side(cent_df, field_name, interval=1000, figsize=(15,12), centrality="Pagerank",
                      filepath=None):
    idx = 0
    fig, axs = plt.subplots(nrows=1, ncols=4, figsize=figsize, sharex=False, sharey=True)
    axs = list(axs.flatten())
    
    labelsize = 28
    
    plt.rcParams['axes.labelsize'] = 16
    
    global_rates = {
        'protected': cent_df.Gender.value_counts(normalize=True)[0],
        'unprotected': cent_df.Gender.value_counts(normalize=True)[1],
        'unknown': cent_df.Gender.value_counts(normalize=True)[-1]
    }
    
    cent_df.sort_values(by=centrality, ascending=False, inplace=True)
    
    plot_group_dist(cent_df, centrality, 
                    interval_size=interval, 
                    max_N=len(cent_df), 
                    protected_group=0, 
                    unprotected=1,
                    field_name=field_name, 
                    ax=axs[0], global_rates=global_rates)
    
    axs[0].set_title("Top 100 % of N = {:,} \n Increment = {}".format(cent_df.shape[0], interval), 
                     fontsize=labelsize - 3, color='#363534')
    centrality_format = r"$\bf{" + centrality.replace(" ", "\ ") + "}$"
    
    axs[0].set_ylabel( centrality_format + "\nGender prop. in top N", fontsize=labelsize + 2)
    
    if centrality == 'PageRank':
        axs[0].legend(fontsize=labelsize - 10, loc='lower right').set_visible(False)
    else:
        axs[0].legend(fontsize=labelsize - 6, loc='lower right').set_visible(False)
    
    # axs[0].set_yticklabels(axs[0].get_yticklabels(),fontsize=labelsize)
    axs[0].tick_params(axis='y', labelsize=labelsize + 6)
    
    axs[0].set_ylim(-0.05, 1.05)
    # normalize x-axis
    #axs[0].set_xticks( axs[0].get_xticks() / axs[0].get_xticks().max() )
    
    #axs[2].get_xticks() / axs[1].get_xticks().max()
    #axs[2].get_xticks() / axs[1].get_xticks().max()
    
    
    
    cent_df_filtered = cent_df.query("Gender != -1")
    
    global_rates = {
        'protected': cent_df_filtered.Gender.value_counts(normalize=True)[0],
        'unprotected': cent_df_filtered.Gender.value_counts(normalize=True)[1],
    }
    
    y, x = plot_group_dist(cent_df_filtered, centrality, 
                           interval_size=interval,
                           max_N=len(cent_df_filtered), 
                           protected_group=0, 
                           unprotected=1, 
                           show_unknown=False,
                           na_removed=True,
                           field_name=field_name,
                           ax=axs[1],
                           global_rates=global_rates)
    axs[1].set_title("Top 100 % of N = {:,}\n Increment = {}. N/A removed".format(cent_df_filtered.shape[0], 
                                                                               interval), fontsize=labelsize - 3,
                    color='#363534')
    axs[1].set_ylabel(None)
    axs[1].legend().set_visible(False)
    
    # 10 %
    cent_df_filtered_ten = cent_df_filtered[:int(cent_df_filtered.shape[0] * 0.1)]
    y, x = plot_group_dist(cent_df_filtered_ten, centrality, 
                           interval_size=100,
                           max_N=len(cent_df_filtered_ten), 
                           protected_group=0, 
                           unprotected=1, 
                           show_unknown=False,
                           na_removed=True,
                           field_name=field_name,
                           ax=axs[2],
                           global_rates=global_rates)
    axs[2].set_title("Top 10 % of N = {:,}\n Increment = {}. N/A removed".format(cent_df_filtered.shape[0],
                                                                               100), 
                     fontsize=labelsize - 3, color='#363534')
    axs[2].set_ylabel(None)
    axs[2].legend().set_visible(False)
    
    
    cent_df_filtered_one = cent_df_filtered[:int(cent_df_filtered.shape[0] * 0.01)]
    y, x = plot_group_dist(cent_df_filtered_one, centrality, 
                           interval_size=10,
                           max_N=len(cent_df_filtered_one), 
                           protected_group=0, 
                           unprotected=1, 
                           show_unknown=False,
                           na_removed=True,
                           field_name=field_name,
                           ax=axs[3],
                           global_rates=global_rates)
    
    axs[3].set_title("Top 1 % of N = {:,} \n Increment = {}. N/A removed".format(cent_df_filtered.shape[0], 10), 
                     fontsize=labelsize - 3, color='#363534')
    axs[3].set_ylabel(None)
    axs[3].legend(fontsize=13, loc='right').set_visible(False)
    
    plt.suptitle("Gender distribution in top N rankings in " + r"$\bf{" + field_name + "}$", 
                 fontsize=labelsize + 4, color='#363534')
    plt.tight_layout()
    
    maxval = cent_df.shape[0]
    
    
    xticks = []
    for tick in axs[2].get_xticklabels()[1:-1]:
        tick.set_text("{}".format(int(tick._x / 10)))
        xticks.append(tick)
    axs[2].set_xticklabels(xticks, fontsize=labelsize)
    axs[2].set_xlabel('% of top N', fontsize=labelsize)
    
    
    xticks = []
    for tick in axs[3].get_xticklabels()[1:-1]:
        tick.set_text("{0:.1f}".format(tick._x / 100))
        xticks.append(tick)

    axs[3].set_xticklabels(xticks, fontsize=labelsize)
    axs[3].set_xlabel('% of top N', fontsize=labelsize)
    
    axs[3].labelsize = 30

    
    for i in range(4):
        axs[i].set_xticks([0. ,  20,  40,  60,  80,  100])
        axs[i].set_xlabel('% of top N', fontsize=labelsize)
        axs[i].tick_params(axis='x', labelsize=labelsize + 10, rotation=90)
    
    if filepath is None:
        plt.show()
    else:
        plt.savefig(filepath, bbox_inches='tight', pad_inches=0.2)
        
    return axs



def plot_matched_side_by_side(cent_df, field_name, centrality_random_sample, centrality_matched_sample, 
                              interval=1000, figsize=(15,12), centrality="Pagerank", filepath=None):
    
    idx = 0
    fig, axs = plt.subplots(nrows=1, ncols=3, figsize=figsize, sharex=False, sharey=True)
    axs = list(axs.flatten())
    
    labelsize = 26
    
    cent_df.sort_values(by=centrality, ascending=False, inplace=True)

    cent_df.reset_index(inplace=True)

    cent_df['rank_position'] = cent_df.index
    rank_position_df = cent_df[['AuthorId', 'rank_position']]

    
    centrality_format = r"$\bf{" + centrality.replace(" ", "\ ") + "}$"
    
    plot_group_dist(cent_df, centrality, 
                    interval_size=interval, 
                    max_N=len(cent_df), 
                    protected_group=0, 
                    unprotected=1,
                    show_unknown=False,
                    na_removed=True,
                    field_name=field_name, 
                    ax=axs[0], global_rates=None)
    
    axs[0].set_title("True population \n N = {:,}".format(cent_df.shape[0]),
                    fontsize=labelsize, color='#363534')
    axs[0].set_ylabel(centrality_format + "\nGender prop. in top N", fontsize=labelsize + 4)
    
    axs[0].tick_params(axis='y', labelsize=labelsize + 8)
    
    axs[0].legend().set_visible(False)
    axs[0].set_ylim(-0.05, 1.05)

    print("Median rank position of all authors: {}".format(cent_df['rank_position'].median()))
            
    y, x = plot_group_dist(centrality_random_sample, centrality, 
                           interval_size=interval,
                           max_N=len(centrality_random_sample), 
                           protected_group=0, 
                           unprotected=1, 
                           show_unknown=False,
                           na_removed=True,
                           field_name=field_name,
                           ax=axs[1],
                           global_rates=None)
                           
    axs[1].set_title("Random matching \n N = {:,}".format(centrality_random_sample.shape[0]),
                    fontsize=labelsize, color='#363534')
    axs[1].set_ylabel(None)
    axs[1].legend().set_visible(False)

    print("Median rank position of authors in random matching: {}".format(centrality_random_sample.merge(rank_position_df, how='left', left_on='AuthorId', right_on='AuthorId')['rank_position'].median()))
        

    y, x = plot_group_dist(centrality_matched_sample, centrality, 
                           interval_size=interval,
                           max_N=len(centrality_matched_sample), 
                           protected_group=0, 
                           unprotected=1, 
                           show_unknown=False,
                           na_removed=True,
                           field_name=field_name,
                           ax=axs[2],
                           global_rates=None)

    axs[2].set_title("Career and affiliation matching \n N = {:,}"
                     .format(centrality_matched_sample.shape[0]),
                     fontsize=labelsize, color='#363534')
    axs[2].set_ylabel(None)
    
    if centrality == 'PageRank':
        axs[2].legend(loc="lower right", fontsize=labelsize-5).set_visible(False)
    else:
        axs[2].legend(loc="lower right", fontsize=labelsize-6).set_visible(False)
    # print("Matched data median: {}".format(centrality_matched_sample.join(rank_position_df, how='left', on='AuthorId', rsuffix='x')['rank_position'].median()))
    print("Median rank position of authors in career and aff. matching: {}".format(centrality_matched_sample.merge(rank_position_df, how='left', left_on='AuthorId', right_on='AuthorId')['rank_position'].median()))
        
    
    plt.suptitle("Gender distribution in Top N ranking in " + r"$\bf{" +  field_name + "}$ on matched populations"
                 , fontsize=labelsize + 4, color='#363534')
    plt.tight_layout()

    datasets = [cent_df, centrality_random_sample, centrality_matched_sample]
    
    # for i in range(3):
    #     maxval = datasets[i].shape[0]
    #     xticks = []
    #     for tick in axs[i].get_xticklabels():
    #         tick.set_text("{0:.1f}".format(100 * (tick._x / maxval)))
    #         xticks.append(tick)
    #     axs[i].set_xticklabels(xticks)

    #     axs[i].set_xlabel('% of top N')

    for i in range(3):
        axs[i].set_xticks([0. ,  20,  40,  60,  80,  100])
        axs[i].set_xlabel('% of top N', fontsize=labelsize)
        axs[i].tick_params(axis='x', labelsize=labelsize + 10, rotation=90)

    if filepath is None:
        plt.show()
    else:
        plt.savefig(filepath, bbox_inches='tight', pad_inches=0.2)
        plt.show()


def compute_ks_test(mag, centrality_df, fos_id, base_filepath="/home/laal/MAG/DATA"):
    
    centrality_df[['AuthorId', 'Gender']].to_csv(base_filepath + "/CentralityAuthors.txt", header=False,index=False, sep="\t")
    
    mag.streams['CentralityAuthors'] = ('CentralityAuthors.txt', ['AuthorId:long', 'Gender:int'])
    
    inter_event = mag.getDataframe('InterEventPublications')
    cent_authors = mag.getDataframe('CentralityAuthors')
    
    query = """
        SELECT iep.AuthorId, ca.Gender, iep.DateDiff 
        FROM InterEventPublications  iep
        INNER JOIN CentralityAuthors ca ON iep.AuthorId = ca.AuthorId
        WHERE FieldOfStudyId = {} AND PrevPaperId != CurrentPaperId
    """.format(fos_id)
    
    
    datediffs = mag.query_sql(query).toPandas()
    women_interevent = datediffs.query("Gender == 0")['DateDiff']
    men_interevent = datediffs.query("Gender == 1")['DateDiff']
    
    ks_test = ks_2samp(women_interevent.values, men_interevent.values)
    
    return datediffs, ks_test


def plot_inter_event_cdf(datediffs, ks_test, field_name, filepath=None):
    labelsize = 20
    women_interevent = datediffs.query("Gender == 0")['DateDiff']
    men_interevent = datediffs.query("Gender == 1")['DateDiff']
    
    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(7,5))

    women_interevent.hist(cumulative=True, density=1, bins=2000, histtype='step', 
                          linewidth=3, color="#6fc9f2", label="Women")

    men_interevent.hist(cumulative=True, density=1, bins=2000, histtype='step', linewidth=3, 
                        label="Men", color="#bd8aff")

    plt.xlim(-100, 2500)
    plt.ylim(0.0, 1.05)

    title = r"$\bf{" +  field_name.replace(" ", "\ ")  + "}$" +  \
    ":\nCDF: Number of days between consequtive publishing dates"
    title += "\n KS statistic: {0:.3f}".format(ks_test.statistic) + ", p-value: {0:.3f}".format(ks_test.pvalue)
    
    plt.title(title, color='#363534')
    plt.xlabel('Number of days', color='#363534', fontsize=labelsize)
    plt.ylabel('Cumulative probability', color='#363534', fontsize=labelsize)
    
    ax.tick_params(axis='y', labelsize=labelsize)
    ax.tick_params(axis='x', labelsize=labelsize)
    
    plt.legend(loc="right")
    
    if filepath is not None:
        plt.savefig(filepath)
        
    plt.show()


def plot_centrality_correlations(field_name, matched_data, filename=None):

    fig, axs = plt.subplots(nrows=2, ncols=3, figsize=(15,8), sharex=False, sharey=False)
    axs = list(axs.flatten())
    
    labelsize = 12
    idx = 0
    
    matched_data['MAG Rank'] = 1 / matched_data['Rank'] 
    
    CENTRALITIES = ['PageRank', 'PageRank05', 'InDegreeStrength', 'MAG Rank']
    
    for cent1, cent2 in itertools.combinations(CENTRALITIES, r=2):
    
        corr = scipy.stats.pearsonr(matched_data[cent1], matched_data[cent2])
        axs[idx].scatter(x=matched_data[cent1], y=matched_data[cent2], color="#ff8c00", alpha=0.3)
        axs[idx].set_title("{} vs. {}".format( cent1, cent2 ) + "\n Corr = {0:.3f}, p-value=".format(corr[0]) + "{0:.3f}".format(corr[1]),
                          color='#363534')
        axs[idx].set_ylabel(cent2, fontsize=labelsize, color='#363534')
        axs[idx].set_xlabel(cent1, fontsize=labelsize, color='#363534')
        
        axs[idx].tick_params(axis='y', labelsize=labelsize)
        axs[idx].tick_params(axis='x', labelsize=labelsize, rotation=45)
        
        idx += 1


    plt.suptitle('Centrality correlations for matched population: ' + r"$\bf{" +  field_name.replace(" ", "\ ")  + "}$", 
                 fontsize=16, color='#363534')
    plt.tight_layout()
    
    if filename is not None:
        plt.savefig(filename)
    plt.show()


def plot_all_fields(centrality, interval=1000):
    
    field_mapping = {
        "Psychology": "/home/laal/MAG/DATA/NETWORKS/SimpleWeightPsychology2020CentralityGendered.csv",
        "Economics": "/home/laal/MAG/DATA/NETWORKS/SimpleWeightEconomics2020CentralityGendered.csv", 
        "Mathematics": "/home/laal/MAG/DATA/NETWORKS/SimpleWeightMathematics2020CentralityGendered.csv",
        "Chemistry": "/home/laal/MAG/DATA/NETWORKS/SimpleWeightChemistry2020CentralityGendered.csv",
    }
    
    for field_name, fpath in field_mapping.items():
        
        cent_df = pd.read_csv(fpath, sep="\t")
        cent_df['MAG Rank'] = cent_df['Rank'].apply(lambda x: x*-1)
        
        if centrality == 'Rank':
            centrality = 'MAG Rank'
                
        plot_side_by_side(cent_df, field_name, interval=1000, figsize=(25,8), centrality=centrality)
        