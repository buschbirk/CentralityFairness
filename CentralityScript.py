from MAG_network import CitationNetwork
import os 
from matplotlib import pyplot as plt
import pandas as pd
import findspark
import MAGspark 
import numpy as np

# set environment variables
os.environ["SPARK_LOCAL_DIRS"] = "/home/laal/MAG/TMP"
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.242.b08-0.el7_7.x86_64"
os.environ['SPARK_HOME'] = "/home/laal/MAG/spark-3.0.2-bin-hadoop2.7"


if __name__ == '__main__':
    mag, spark = MAGspark.get_mag_with_cluster_connection(jobid=39990, memory_per_executor=14000,
                                                          data_folderpath="/home/laal/MAG/DATA/")

    id_names = [(162324750, 'Economics'), (15744967, 'Psychology'), (33923547, 'Mathematics'), (185592680, 'Chemistry')]
    id_names = [(185592680, 'Chemistry')]
    
    for fos_id, name in id_names:
        print("Starting network extraction for {}".format(name))
        
        network = CitationNetwork(mag, fos_id=fos_id, fos_name=name, root_data_folder="/home/laal/MAG/DATA")
        network.check_references_and_citations()
        network.save_author_network("SimpleWeight{}2020".format(name), mindate='1800-01-01', maxdate='2020-12-31')
        network.load_author_author_network("SimpleWeight{}2020".format(name))
        
        print("Building graph of {}".format(name))
        graph, node_mapping, eweight = network.build_graph()
        print("Computing centralities of {}".format(name))
        network.compute_centralities(graph, node_mapping, eweight, "", pr_damping=0.85)
        # network.append_gender_and_macrank()
        
        # network.nodelist_and_edge_count()    
        print("\n----------------------------\n")

    