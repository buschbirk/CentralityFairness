
from graph_tool.all import *
import pandas as pd
import os
import graph_tool as gt
import sys


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

if __name__ == '__main__':

    args = sys.argv

    citation_location = args[1]
    destination = args[2]

    graph, nodes, eweight = build_graph(citation_location)
    compute_centralities(graph, nodes, eweight, destination, 
                         pr_damping=0.85, katz_alpha=0.01, katz_beta=None)





