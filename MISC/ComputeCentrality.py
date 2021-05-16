
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



def build_graph(citation_folderpath):
    
    edges = []
    g = gt.Graph()
    eweight = g.new_ep("double")
    
    for filename in sorted(os.listdir(citation_folderpath)):
        # edges = []
        if not filename.endswith('.csv'):
            continue

        print(filename)
        with open(citation_folderpath + "/" + filename) as file:
            for line in file:
                contents = line.strip().split("\t")
                edges.append((str(contents[0]), str(contents[1]), float(contents[2])))
    
    node_mapping = g.add_edge_list(edges, eprops=[eweight], hashed=True, hash_type='string')
    
    return g, node_mapping, eweight


def compute_centralities(graph, node_mapping, eweight, filename, 
                         pr_damping=0.85, katz_alpha=0.01, katz_beta=None):
    
    nodes = list(graph.vertices())
    
    largest_comp = gt.topology.label_largest_component(graph)
    
    print("Initiating PageRank")
    pr = pagerank(graph, weight=eweight, damping=pr_damping)

    pr_half = pagerank(graph, weight=eweight, damping=0.5)
    
    # print("Initiating Katz")
    #katz_centrality = gt.centrality.katz(gt.GraphView(graph, vfilt=largest_comp), 
    #                                     weight=eweight, alpha=katz_alpha, beta=katz_beta)
    
    #katz_list = list(katz_centrality)
    
    #katz_scores = []
    #for indicator in largest_comp.a:
    #    if indicator == 1:
    #        katz_scores.append(katz_list.pop(0))
    #    else:
    #        katz_scores.append(None)     
                                         
    print("Initiating degree measures")
    in_degree_strength = graph.get_in_degrees(nodes, eweight=eweight)
    in_degree = graph.get_in_degrees(nodes)

    out_degree_strength = graph.get_out_degrees(nodes, eweight=eweight)
    out_degree = graph.get_out_degrees(nodes, eweight=eweight)
    
    print("Finished centrality computations")
    
    df = pd.DataFrame()
    df['node'] = list(node_mapping)
    df['pagerank'] = list(pr.get_array())
    df['pagerank_05'] = list(pr_half.get_array())
    df['in_degree_strength'] = list(in_degree_strength)
    df['in_degree'] = list(in_degree)
    df['out_degree_strength'] = list(out_degree_strength)
    df['out_degree'] = list(out_degree)

    df.to_csv(filename, index=False, sep="\t", header=False)
    
    return


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
        df = network.compute_centralities(graph, node_mapping, eweight, "", pr_damping=0.85)
        df['sliceid'] = record['slice_index']
        
        if record['slice_index'] == 0:
            df.to_csv("/home/laal/MAG/DATA/NETWORKS/SimpleWeight{}2020SliceMasterCentrality.csv"
                      .format(fos_name), index=False, header=False, sep="\t")
        else:
            df.to_csv("/home/laal/MAG/DATA/NETWORKS/SimpleWeight{}2020SliceMasterCentrality.csv"
                      .format(fos_name), index=False, header=False, mode='a', sep="\t")        
            
    # network.append_gender_and_macrank("SimpleWeight{}2020SliceMasterCentrality".format(fos_name))


if __name__ == '__main__':

    # args = sys.argv

    # citation_location = args[1]
    # destination = args[2]

    # graph, nodes, eweight = build_graph(citation_location)
    # compute_centralities(graph, nodes, eweight, destination, 
    #                      pr_damping=0.85, katz_alpha=0.01, katz_beta=None)
    mag, spark = MAGspark.get_mag_with_cluster_connection(jobid=0, memory_per_executor=14000,
                                                        data_folderpath="/home/laal/MAG/DATA/")

    build_wacn_graphs(185592680, 'Chemistry', root_data_folder="/home/laal/MAG/DATA", 
                      csv_filepath="/home/laal/MAG/CentralityFairness/SLICES/Chemistry2020.csv")



