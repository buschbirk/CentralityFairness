# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 06:04:40 2021

@author: lasse
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from MAG import MicrosoftAcademicGraph
import MAGspark 
import os
import shutil
import pandas as pd


def paper_root_field_mag(mag, destination="/home/laal/MAG/DATA/PaperRootFieldMag.txt"):
    """
    Computes a mapping between PaperId and level-0 (root) Field of Study.
    Each paper can have multiple level-0 Fields of Study. 

    Saves result to TSV at given destination
    """

    fos = mag.getDataframe('FieldsOfStudy')
    pfos = fos = mag.getDataframe('PaperFieldsOfStudy')
    
    query = """
        SELECT pfs.PaperId, fs.FieldOfStudyId
        FROM PaperFieldsOfStudy pfs
        INNER JOIN FieldsOfStudy fs ON pfs.FieldOfStudyId = fs.FieldOfStudyId
        WHERE fs.Level = 0
    """
    paper_fos = mag.query_sql(query)
    
    # write to file
    paper_fos.write.option("sep", "\t").option("encoding", "UTF-8")\
    .csv(destination)
    
    return


def paper_author_affiliation_gendered(mag, destination="/home/laal/MAG/DATA/PaperAuthorAffiliationsGendered.txt"):
    """
    Computes a mapping between PaperId, AuthorId and Gender 
    where unknown gender is mapped to -1. 

    Saves result to TSV at given destination
    """

    paper_author_affiliations = mag.getDataframe('PaperAuthorAffiliations')
    wtm = mag.getDataframe('WosToMag')
    
    query = """
        SELECT PaperId, AuthorId, 
        CASE WHEN wtm.Gender in (0, 1) THEN wtm.Gender ELSE -1 END as Gender
        FROM PaperAuthorAffiliations paa
        LEFT JOIN WosToMag wtm ON paa.AuthorId = wtm.MAG
    """

    # execute query and save to TSV file at destination
    paa = mag.query_sql(query)
    paa.write.option("sep", "\t").option("encoding", "UTF-8")\
    .csv(destination)

    return paa

def authors_per_paper(mag, destination="/home/laal/MAG/DATA/NumAuthorsPerPaper.txt"):
    """
    Computes a the number of authors per paper.
    Saves result to TSV at given destination
    """

    author_affiliations = mag.getDataframe('PaperAuthorAffiliations')
    authors = mag.getDataframe('Papers')
    
    query = """
        SELECT paa.PaperId, 
        COUNT(DISTINCT(AuthorId)) as NumAuthors, 
        MIN(p.Date) as PublishDate
        FROM PaperAuthorAffiliations paa
        INNER JOIN Papers AS p ON paa.PaperId = p.PaperId  
        GROUP BY paa.PaperId
    """
    
    authors_paper = mag.query_sql(query)
    authors_paper.write.option("sep", "\t").option("encoding", "UTF-8")\
    .csv(destination)

    return authors_paper


# consider removing
def get_paper_references_gendered(mag):
    author_affiliations = mag.getDataframe('PaperAuthorAffiliationsGendered')
    paper_references = mag.getDataframe('PaperReferences')
    
    
    query = "SELECT DISTINCT(PaperId) FROM PaperAuthorAffiliationsGendered"
    mag.query_sql(query).createOrReplaceTempView('DistinctPapers')
    
    query = """
        SELECT pr.PaperId, pr.PaperReferenceId 
        FROM PaperReferences pr 
        WHERE pr.PaperId IN (SELECT * FROM DistinctPapers)
        AND pr.PaperReferenceId IN (SELECT * FROM DistinctPapers)
    """
    
    paper_ref_gendered = mag.query_sql(query)
    return paper_ref_gendered

class CitationNetwork():


    def __init__(self, mag, fos_id, fos_name, 
               root_data_folder="/home/laal/MAG/DATA"):
        self.mag = mag
        self.fos_id = fos_id            # Field of Study ID
        self.fos_name = fos_name

        self.root_folder = root_data_folder

        self.network_name = None
        self.network_destination = None

    def get_paper_references_field(self):

        paper_references = self.mag.getDataframe('PaperReferences')
        paper_root_field = self.mag.getDataframe('PaperRootFieldMag')

        query = """
            SELECT pr.PaperId, pr.PaperReferenceId 
            FROM PaperReferences pr 
            INNER JOIN PaperRootFieldMag prf1 ON prf1.PaperId = pr.PaperId 
            INNER JOIN PaperRootFieldMag prf2 ON prf2.PaperId = pr.PaperReferenceId
            WHERE prf1.FieldOfStudyId = {} AND prf2.FieldOfStudyId = {}
        """.format(self.fos_id, self.fos_id)

        paper_ref_field = self.mag.query_sql(query)
        return paper_ref_field


    def get_field_citations(self, paper_references_name):
        """
        paper_references_name is name of paperReferences dataset
        """
        author_affiliations = self.mag.getDataframe('PaperAuthorAffiliationsGendered') 
        paper_references = self.mag.getDataframe(paper_references_name)
        paper_root_field = self.mag.getDataframe('PaperRootFieldMag')
        
        query = """
            SELECT 
                paa1.AuthorId AS CitingAuthorId, 
                pr.PaperId AS CitingPaperId, 
                paa1.Gender as CitingAuthorGender,
                paa2.AuthorId as CitedAuthorId,
                pr.PaperReferenceId as CitedPaperId,
                paa2.Gender as CitedAuthorGender
            FROM PaperAuthorAffiliationsGendered AS paa1 
            INNER JOIN {} pr ON paa1.PaperId = pr.PaperId
            INNER JOIN PaperAuthorAffiliationsGendered AS paa2 ON pr.PaperReferenceId = paa2.PaperId 
        """.format(paper_references_name)
        
        citations = self.mag.query_sql(query)
        return citations


    def check_references_and_citations(self, overwrite=False):
        """
        Ensures that field-specific paper references and citations are available. 
        If not, extracts these and writes to file.

        overwrite will remove and replace current file
        """
        paper_references_location = self.root_folder + "/PaperReferences{}.txt".format(self.fos_name)
        if (not os.path.exists(paper_references_location)) or overwrite:

            # remove folder if it exists
            if os.path.exists(paper_references_location):
                shutil.rmtree(paper_references_location)

            print("Extracting paper references for {} to {}".format(self.fos_name, paper_references_location))

            paper_references = self.get_paper_references_field()
            
            # save to file at paper_references_location
            paper_references.write.option("sep", "\t").option("encoding", "UTF-8")\
            .csv(paper_references_location)

        # ensure that mag has dataset information for paper references
        self.mag.streams["PaperReferences{}".format(self.fos_name)] = \
        ('PaperReferences{}.txt'.format(self.fos_name), ['PaperId:long', 'PaperReferenceId:long'])

        field_citations_location = self.root_folder + "/Citations{}.txt".format(self.fos_name)
        if (not os.path.exists(field_citations_location)) or overwrite:

            if os.path.exists(field_citations_location):
                shutil.rmtree(field_citations_location)

            print("Extracting all citations for {} to {}".format(self.fos_name, field_citations_location))

            citations = self.get_field_citations(paper_references_name='PaperReferences{}'.format(self.fos_name))
            citations.write.option("sep", "\t").option("encoding", "UTF-8")\
            .csv(field_citations_location)
        

        # ensure that mag has dataset information for field citations
        self.mag.streams["Citations{}".format(self.fos_name)] = \
        ('Citations{}.txt'.format(self.fos_name), \
        ['CitingAuthorId:long', 'CitingPaperId:long', 'CitingAuthorGender:int', 
         'CitedAuthorId:long', 'CitedPaperId:long', 'CitedAuthorGender:int'])

        print("Paper references and citations available for {}".format(self.fos_name))

        return


    def extract_author_author_network(self, mindate='1800-01-01', maxdate='2021-06-01'):
        """
        Filters by publishing date of citing paper (Radicchi et. al., 2009). 
        Does not allow references to cited papers published after maxdate.
        """

        # ensure that field references and citation datasets are available
        self.check_references_and_citations(overwrite=False)

        citations_dataset_name = 'Citations{}'.format(self.fos_name)

        citations =  self.mag.getDataframe(citations_dataset_name)
        author_affiliations = self.mag.getDataframe('PaperAuthorAffiliations')
        authors_per_paper = self.mag.getDataframe('NumAuthorsPerPaper')

        query = """
            SELECT 
              CitingAuthorId, 
              CitedAuthorId, 
              SUM( 1 / (app1.NumAuthors * app2.NumAuthors)  ) as Weight,
              COUNT(DISTINCT(CitingPaperId)) as numCitingPapers,
              COUNT(DISTINCT(CitedPaperId)) as numCitedPapers,
              COUNT(*) as numCitations
            FROM {} AS ac
            INNER JOIN NumAuthorsPerPaper app1 ON ac.CitingPaperId = app1.PaperId
            INNER JOIN NumAuthorsPerPaper app2 ON ac.CitedPaperId = app2.PaperId 
            WHERE 
            app1.PublishDate >= '{}' AND
            app1.PublishDate <= '{}' AND app2.PublishDate <= '{}' 
            GROUP BY CitingAuthorId, CitedAuthorId
        """.format(citations_dataset_name, 
                   mindate, maxdate, maxdate)    
        
        author_edgelist = self.mag.query_sql(query)
        
        return author_edgelist


    def save_author_network(self, network_name, mindate='1800-01-01', maxdate='2021-06-01', overwrite=False):

        self.network_name = network_name
        self.network_destination = self.root_folder + "/NETWORKS/{}.txt".format(self.network_name)

        if os.path.exists(self.network_destination) and not overwrite:
            print("Network exists at " + self.network_destination + ". Use overwrite to replace")
            return

        author_network = self.extract_author_author_network(mindate=mindate, maxdate=maxdate)
        author_network.write.option("sep", "\t").option("encoding", "UTF-8")\
        .csv(self.network_destination)

        print("Network {} saved to {}".format(self.network_name, self.network_destination))

        return


    def load_author_author_network(self, network_name):
        self.network_name = network_name
        self.network_destination = self.root_folder + "/NETWORKS/{}.txt".format(network_name)

        if not os.path.exists(self.network_destination):
            print("No network found at {}".format(self.network_destination))
            print("Use save_author_network() to extract network")
            return

        # Assign network info on mag
        self.mag.streams[self.network_name] = ('NETWORKS/{}.txt'.format(self.network_name), 
                                         ['CitingAuthorId:long', 'CitedAuthorId:long', 'Weight:float',
                                          'numCitingPapers:int', 'numCitedPapers:int', 'numCitations:int'])

        network = self.mag.getDataframe(self.network_name)

        query = "SELECT * FROM {}".format(self.network_name)
        return self.mag.query_sql(query)


    def nodelist_and_edge_count(self):

        if self.network_name is None:
            print("Please load network using load_author_author_network()")
            return
        
        authors = self.mag.getDataframe('WosToMag')
        network = self.mag.getDataframe(self.network_name)
        
        num_edges = network.count()
        print("The network has {} edges".format(num_edges))
        
        query = """
        SELECT AuthorId, 
        CASE WHEN wtm.Gender IN (0,1) THEN wtm.Gender ELSE -1 END AS Gender
        FROM
        (
            SELECT DISTINCT(n.CitingAuthorId) as AuthorId
            FROM {} n
            
            UNION 
            
            SELECT DISTINCT(n.CitedAuthorId) 
            FROM {} n
        ) a
        LEFT JOIN WosToMag wtm ON a.AuthorId = wtm.MAG
        """.format(self.network_name, self.network_name)

        nodelist = self.mag.query_sql(query)
        num_nodes = nodelist.count()
        print("The network has {} nodes".format(num_nodes))
        
        return network, nodelist


    def build_graph(self):
        """
        Constructs a graph-tool Graph from an author-author citation network. 
        This is usually a very memory-intensive task.
        """

        if self.network_destination is None:
            print("Network destination is not set. Load network")
            return

        # import graph-tool 
        # from graph_tool.all import *
        import graph_tool as gt

        edges = []
        g = gt.Graph()
        eweight = g.new_ep("double")
        
        for filename in sorted(os.listdir(self.network_destination)):

            # skip all Spark helper-files
            if not filename.endswith('.csv'):
                continue

            # print("Parsing file: {}".format(filename))

            with open(self.network_destination + "/" + filename) as file:
                for line in file:
                    # extract source and target node + edge weight
                    contents = line.strip().split("\t")
                    edges.append((str(contents[0]), str(contents[1]), float(contents[2])))
        
        # construct Graph from weighted edgelist, hashing node IDs
        # node_mapping will contain mapping between MAG ID and node index in graph
        node_mapping = g.add_edge_list(edges, eprops=[eweight], hashed=True, hash_type='string')
        
        return g, node_mapping, eweight


    def compute_centralities(self, graph, node_mapping, eweight, filename, pr_damping=0.85):
        """
        Computes PageRank, in-degree and out-degree centralities on graph-tool graph using edge weights.
        Saves results to csv at given destination.  
        """
        from graph_tool.all import pagerank, label_largest_component

        # get list of nodes in graph
        nodes = list(graph.vertices())
        
        # identify nodes in largest component for Katz centrality
        largest_comp = label_largest_component(graph)
        
        print("Initiating PageRank")
        # compute PageRank with given damping factor
        pr = pagerank(graph, weight=eweight, damping=pr_damping)

        # compute PageRank with 0.5 damping factor
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
        
        # Compute in- and out-degree for all nodes with and without edge weights
        print("Initiating degree measures")
        in_degree_strength = graph.get_in_degrees(nodes, eweight=eweight)
        in_degree = graph.get_in_degrees(nodes)

        out_degree_strength = graph.get_out_degrees(nodes, eweight=eweight)
        out_degree = graph.get_out_degrees(nodes)
        
        print("Finished centrality computations")
        
        # cast all centralities to Pandas dataframe
        df = pd.DataFrame()
        df['node'] = list(node_mapping)
        df['pagerank'] = list(pr.get_array())
        df['pagerank_05'] = list(pr_half.get_array())
        df['in_degree_strength'] = list(in_degree_strength)
        df['in_degree'] = list(in_degree)
        df['out_degree_strength'] = list(out_degree_strength)
        df['out_degree'] = list(out_degree)

        # write to CSV 
        filepath = ".".join(self.network_destination.split(".")[:-1]) + "Centrality.csv"
        df.to_csv(filepath, index=False, sep="\t", header=False)
        
        print("Centrality CSV saved to {}".format(filepath))

        return


    def append_gender_and_macrank(self):
        """
        Merges CSV with centrality scores with Gender information and MAG Rank for each author.
        Stores results in single CSV file with header. 
        """
        centrality_filename = self.network_destination.split("/")[-1].split(".")[0] + "Centrality"

        # Assign centrality dataset info on mag
        self.mag.streams[centrality_filename] = ('NETWORKS/{}.csv'.format(centrality_filename), 
                                                ['AuthorId:long', 'PageRank:float', 'PageRank05:float', 
                                                 'InDegreeStrength:float', 'InDegree:float', 'OutDegreeStrength:float', 
                                                 'OutDegree:float'])
        
        cent = self.mag.getDataframe(centrality_filename)
        wtm = self.mag.getDataframe('WosToMag')
        authors = self.mag.getDataframe('Authors')
        
        query = """
            SELECT c.*, 
            CASE WHEN wtm.Gender IN (0, 1) THEN wtm.Gender ELSE -1 END as Gender,
            a.Rank
            FROM {} c
            LEFT JOIN WosToMag wtm ON c.AuthorId = wtm.MAG 
            INNER JOIN Authors a ON c.AuthorId = a.AuthorId
        """.format(centrality_filename)
        
        # run query and extract to Pandas dataframe
        centrality = self.mag.query_sql(query)
        centrality_df = centrality.toPandas()

        csv_destination = self.root_folder + "/NETWORKS/{}".format(centrality_filename + 'Gendered.csv')

        # save as CSV with header
        centrality_df.to_csv(csv_destination,
                             index=False, sep="\t")

        print("Gendered centrality measures saved to {}".format(csv_destination))


        


                




if __name__ == '__main__':

  os.environ["SPARK_LOCAL_DIRS"] = "/home/laal/MAG/TMP"
  os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.242.b08-0.el7_7.x86_64"
  os.environ['SPARK_HOME'] = "/home/laal/MAG/spark-3.0.2-bin-hadoop2.7"

  mag, spark = MAGspark.get_mag_with_cluster_connection(jobid=39715, memory_per_executor=14000,
                                                        data_folderpath="/home/laal/MAG/DATA/")


  # spark = SparkSession \
  #     .builder \
  #     .config("spark.executor.memory", "4g")\
  #     .config("spark.driver.memory", "2g")\
  #     .config("spark.executor.cores", 7)\
  #     .config("spark.memory.offHeap.enabled", True)\
  #     .config("spark.memory.offHeap.size","1g")\
  #     .config("spark.sql.adaptive.enabled", True)\
  #     .config("spark.sql.adaptive.coalescePartitions.enabled", True)\
  #     .config("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", False)\
  #     .appName("MAG app") \
  #     .getOrCreate()

  # mag = MicrosoftAcademicGraph(spark=spark, data_folderpath="/home/agbe/MAG/DATA/")

  # country_df = assign_country(mag)
  # print(country_df.show(50))
  # country_df.coalesce(1).write.option("sep", "\t").option("encoding", "UTF-8")\
  # .csv("/home/agbe/MAG/DATA/AuthorCountry.txt")

  # paper_field_df = assign_field_of_study(mag)
  # print(paper_field_df.show(50))

  # paper_field_df.write.option("sep", "\t").option("encoding", "UTF-8")\
  # .csv("/home/agbe/MAG/DATA/PaperRootField.txt")

  # author_to_field = author_to_field_of_study(mag)

  # print(author_to_field.show(25))

  # author_to_field.write.option("sep", "\t").option("encoding", "UTF-8")\
  # .csv("/home/agbe/MAG/DATA/AuthorRootField.csv")

  # AUTHOR METADATA
  # author_metadata = author_metadata(mag)
  # print(author_metadata.show(50))
  # author_metadata.write.option("sep", "\t").option("encoding", "UTF-8")\
  # .csv("/home/agbe/MAG/DATA/AuthorMetadata.csv")

  # CITATIONS
  # citations = citation_edges(mag)

  # print(citations.show(50))
  # print(citations.describe().show())

  # citations.write.option("sep", "\t").option("encoding", "UTF-8")\
  # .csv("/home/agbe/MAG/DATA/Citations.txt")

  # spark.stop()

