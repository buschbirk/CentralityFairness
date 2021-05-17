
from graph_tool.all import *
import pandas as pd
import os
import graph_tool as gt
import sys

from MAG_network import CitationNetwork
import os 
from matplotlib import pyplot as plt
import pandas as pd
import findspark
import MAGspark 

import sys

sys.path.insert(0,"/home/laal/MAG/CentralityFairness/Evaluations")

# from Evaluations.Evaluator import Evaluator


# set environment variables
os.environ["SPARK_LOCAL_DIRS"] = "/home/laal/MAG/TMP"
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.242.b08-0.el7_7.x86_64"
os.environ['SPARK_HOME'] = "/home/laal/MAG/spark-3.0.2-bin-hadoop2.7"


def build_wacn_graphs(fos_id, fos_name, root_data_folder, csv_filepath):
    
    network = CitationNetwork(mag, fos_id=fos_id, fos_name=fos_name, root_data_folder=root_data_folder)
    network.check_references_and_citations(overwrite=False)

    print("Extracting ordered references")
    
    # papers = network.mag.getDataframe('Papers')
    # paper_refs = network.mag.getDataframe(network.paper_references_name)
    
    if not os.path.exists(csv_filepath):
        return
    
    slice_df = pd.read_csv(csv_filepath)
        
    # loop over rows and create networks
    for record in slice_df.to_dict('records'):
        
        if os.path.exists("/home/laal/MAG/DATA/NETWORKS/SimpleWeight{}2020Slice{}Centrality.csv".
                         format(fos_name, record['slice_index'])):
            continue
        
        print("Extracting {} slice index {} / {}".format(fos_name, record['slice_index'], len(slice_df)))

        network_name = "SimpleWeight{}2020Slice{}".format(fos_name, record['slice_index'])
        
        network.load_author_author_network(network_name)
        graph, node_mapping, eweight = network.build_graph()
        df = network.compute_centralities(graph, node_mapping, eweight, pr_damping=0.85)
        df['sliceid'] = record['slice_index']
        
        if record['slice_index'] == 0:
            df.to_csv("/home/laal/MAG/DATA/NETWORKS/SimpleWeight{}2020SliceMasterCentrality.csv"
                      .format(fos_name), index=False, header=False, sep="\t")
        else:
            df.to_csv("/home/laal/MAG/DATA/NETWORKS/SimpleWeight{}2020SliceMasterCentrality.csv"
                      .format(fos_name), index=False, header=False, mode='a', sep="\t")        
            



if __name__ == '__main__':

    mag, spark = MAGspark.get_mag_with_cluster_connection(jobid=0, memory_per_executor=14000,
                                                          data_folderpath="/home/laal/MAG/DATA/")

    build_wacn_graphs(185592680, 'Chemistry', root_data_folder="/home/laal/MAG/DATA", 
                      csv_filepath="/home/laal/MAG/CentralityFairness/SLICES/Chemistry2020.csv")



