from matplotlib import pyplot as plt
import seaborn as sns
import pandas as pd
import matplotlib
# import powerlaw


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
    
    
    xticks = []
    y_values = []
    y_values_unprotected = []
    y_values_unknown = []
    
    for N in range(interval_size, max_N, interval_size):
        
        xticks.append(N)
        top_n_df = sorted_df[:N]
        value_counts = top_n_df.Gender.value_counts(normalize=True)
        
        y_values.append(value_counts[protected_group] if protected_group in value_counts else 0)
        y_values_unprotected.append(value_counts[unprotected] if unprotected in value_counts else 0)
        
        if show_unknown: y_values_unknown.append(value_counts[-1] if -1 in value_counts else 0)
    
    if show_unknown:
        global_rate_unknown = sorted_df.Gender.value_counts(normalize=True)[-1] \
        if global_rates is None else global_rates.get('unknown')
        ax.plot(xticks, y_values_unknown, '-o', label="N/A", markersize=3, color="#b8b8b8", alpha=0.2)
        ax.axhline(y=global_rate_unknown, label="Total population N/A", linestyle='--', alpha=0.8, color="#b8b8b8")
    
    
    ax.plot(xticks, y_values, '-o', label="Female", markersize=6, color="#6fc9f2")
    ax.axhline(y=global_rate_protected, label="Total population female", linestyle='--', alpha=1.0, color="#6fc9f2")
    
    
    ax.plot(xticks, y_values_unprotected, '-o', label="Male", markersize=6, color="#bd8aff")
    ax.axhline(y=global_rate_unprotected, label="Total population male", linestyle='--', alpha=1.0, color="#bd8aff")
    
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
    
    axs[0].set_title("Top 100 % of N = {:,} \n Increment = {}".format(cent_df.shape[0], interval))
    axs[0].set_ylabel("Membership proportion in top N")
    axs[0].legend().set_visible(True)
        

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
                                                                               interval))
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
                                                                               100))
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
    
    axs[3].set_title("Top 1 % of N = {:,} \n Increment = {}. N/A removed".format(cent_df_filtered.shape[0], 10))
    axs[3].set_ylabel(None)
    axs[3].legend().set_visible(False)
    
    plt.suptitle("Group membership in Top N ranking in {} ({})".format(field_name, centrality), fontsize=20)
    plt.tight_layout()
    
    maxval = cent_df.shape[0]
    
    xticks = []
    for tick in axs[0].get_xticklabels():
        tick.set_text("{0:.1f}".format(int(100 * (tick._x / maxval))))
        xticks.append(tick)
    axs[0].set_xticklabels(xticks)
    axs[0].set_xlabel('% of top N')
    
    
    maxval = cent_df_filtered.shape[0]
    
    for i in range(1, 4):
        xticks = []
        for tick in axs[i].get_xticklabels():
            tick.set_text("{0:.1f}".format(100 * (tick._x / maxval)))
            xticks.append(tick)
        axs[i].set_xticklabels(xticks)

        axs[i].set_xlabel('% of top N')

    
    
    if filepath is None:
        plt.show()
    else:
        plt.savefig(filepath)
        
    return axs



def plot_matched_side_by_side(cent_df, field_name, centrality_random_sample, centrality_matched_sample, 
                              interval=1000, figsize=(15,12), centrality="Pagerank", filepath=None):
    
    idx = 0
    fig, axs = plt.subplots(nrows=1, ncols=3, figsize=figsize, sharex=False, sharey=True)
    axs = list(axs.flatten())

    
    cent_df.sort_values(by=centrality, ascending=False, inplace=True)
    
    plot_group_dist(cent_df, centrality, 
                    interval_size=interval, 
                    max_N=len(cent_df), 
                    protected_group=0, 
                    unprotected=1,
                    show_unknown=False,
                    na_removed=True,
                    field_name=field_name, 
                    ax=axs[0], global_rates=None)
    
    axs[0].set_title("True population \n N = {:,}, Increment = {}".format(cent_df.shape[0], interval))
    axs[0].set_ylabel("Membership proportion in top N")
    axs[0].legend().set_visible(True)
            
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
                           
    axs[1].set_title("Random matching \n N = {:,}, Increment = {}".format(centrality_random_sample.shape[0], interval))
    axs[1].set_ylabel(None)
    axs[1].legend().set_visible(False)
    

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

    axs[2].set_title("Career and affiliation matching \n N = {:,}, Increment = {}".format(centrality_matched_sample.shape[0], interval))
    axs[2].set_ylabel(None)
    axs[2].legend().set_visible(False)
    
    
    plt.suptitle("Group membership in Top N ranking in {} ({}) on matched populations"
                 .format(field_name, centrality), fontsize=16)
    plt.tight_layout()

    datasets = [cent_df, centrality_random_sample, centrality_matched_sample]
    
    for i in range(3):
        maxval = datasets[i].shape[0]
        xticks = []
        for tick in axs[i].get_xticklabels():
            tick.set_text("{0:.1f}".format(100 * (tick._x / maxval)))
            xticks.append(tick)
        axs[i].set_xticklabels(xticks)

        axs[i].set_xlabel('% of top N')

    if filepath is None:
        plt.show()
    else:
        plt.savefig(filepath)

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
        