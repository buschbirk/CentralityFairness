# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 06:04:40 2021

@author: lasse
"""


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from MAG import MicrosoftAcademicGraph
import os

def assign_country(mag):
  """
  Returns DataFrame with most frequent country per author from author-paper-affiliation links
  """
 
  author_affiliations = mag.getDataframe('PaperAuthorAffiliations')
  authors = mag.getDataframe('WosToMag')
  affiliations = mag.getDataframe('Affiliations')

  query = """
  SELECT X.AuthorId, X.Iso3166Code, X.NumPaperAffiliations, 
         ROW_NUMBER() OVER (PARTITION BY X.AuthorId ORDER BY X.NumPaperAffiliations DESC) AS countryRank
  FROM (
    SELECT paa.AuthorId, 
           a.Iso3166Code,
           COUNT(*) AS NumPaperAffiliations
    FROM PaperAuthorAffiliations AS paa
    INNER JOIN Affiliations AS a ON a.AffiliationId = paa.AffiliationId
    INNER JOIN WosToMag AS wtm ON paa.AuthorId = wtm.MAG
    GROUP BY paa.AuthorId, a.Iso3166Code
  ) as X
  ORDER BY AuthorId
  """

  country_df = mag.query_sql(query)
  return country_df

def assign_field_of_study(mag):
  """
  Returns DataFrame with all level-0 Field of Study
  per paper
  """
 
  paper_fos = mag.getDataframe('PaperFieldsOfStudy')
  fos_root = mag.getDataframe('FieldOfStudyRoot')

  query = """
  SELECT X.PaperId, X.AncestorId, X.NumPapersInField, 
         ROW_NUMBER() OVER (PARTITION BY X.PaperId ORDER BY X.NumPapersInField DESC) AS fieldRank
  FROM (
    SELECT pfs.PaperId,
           fsr.AncestorId,
           COUNT(*) AS NumPapersInField
    FROM PaperFieldsOfStudy AS pfs
    INNER JOIN FieldOfStudyRoot AS fsr ON pfs.FieldOfStudyId = fsr.ChildId
    GROUP BY pfs.PaperId, fsr.AncestorId
  ) as X
  ORDER BY PaperId
  """
  paper_field_df = mag.query_sql(query)
  return paper_field_df


def author_to_field_of_study(mag):
  author_affiliations = mag.getDataframe('PaperAuthorAffiliations')
  authors = mag.getDataframe('WosToMag')
  paper_root_field = mag.getDataframe('PaperRootField')

  query = """
  SELECT X.AuthorId, X.AncestorId, X.NumPapersInField, 
         ROW_NUMBER() OVER (PARTITION BY X.AuthorId ORDER BY X.NumPapersInField DESC) AS fieldRank
  FROM (
    SELECT paa.AuthorId, 
           prf.AncestorId,
           COUNT(*) AS NumPapersInField
    FROM PaperAuthorAffiliations AS paa
    INNER JOIN WosToMag AS wtm ON paa.AuthorId = wtm.MAG 
    INNER JOIN PaperRootField AS prf ON paa.PaperId = prf.PaperId
    WHERE prf.fieldRank = 1
    GROUP BY paa.AuthorId, 
             prf.AncestorId
  ) X  
  ORDER BY X.AuthorId
  """
  author_to_field = mag.query_sql(query)
  return author_to_field
  

def author_metadata(mag):

  author_affiliations = mag.getDataframe('PaperAuthorAffiliations')
  authors = mag.getDataframe('WosToMag')
  paper_root_field = mag.getDataframe('PaperRootField')
  papers = mag.getDataframe('Papers')
  affiliation = mag.getDataframe('Affiliations')

  query = """
  SELECT paa.AuthorId, prf.AncestorId, 
  MIN(a.Rank) as MinAffiliationRank,
  COUNT(DISTINCT(paa.PaperId)) as NumPapers,
  MIN(p.Date) as MinPubDate,
  MAX(p.Date) as MaxPubDate, 
  (COUNT(DISTINCT(paa.PaperId)) / (DATEDIFF( MAX(p.Date), MIN(p.Date) ) / 365)) as PubsPerYear
  FROM PaperAuthorAffiliations AS paa 
  INNER JOIN WosToMag AS wtm ON paa.AuthorId = wtm.MAG 
  INNER JOIN PaperRootField prf ON paa.PaperId = prf.PaperId
  INNER JOIN Papers p ON p.PaperId = paa.PaperId
  INNER JOIN Affiliations AS a ON paa.AffiliationId = a.AffiliationId
  WHERE prf.fieldRank = 1
  GROUP BY paa.AuthorId, prf.AncestorId
  ORDER BY paa.AuthorId
  """
  author_metadata_df = mag.query_sql(query)
  return author_metadata_df


def author_metadata_field(mag):

  author_affiliations = mag.getDataframe('PaperAuthorAffiliations')
  authors = mag.getDataframe('WosToMag')
  paper_root_field = mag.getDataframe('PaperRootFieldMag')
  papers = mag.getDataframe('Papers')
  affiliation = mag.getDataframe('Affiliations')

  query = """
  SELECT paa.AuthorId, prf.FieldOfStudyId, 
  CASE WHEN wtm.Gender IN (0, 1) THEN wtm.Gender ELSE -1 END as Gender,
  MIN(a.Rank) as MinAffiliationRank,
  COUNT(DISTINCT(COALESCE(paa.FamilyId, paa.PaperId))) as NumPapers,
  MIN(p.Date) as MinPubDate,
  MAX(p.Date) as MaxPubDate, 
  (COUNT(DISTINCT(paa.PaperId)) / (DATEDIFF( MAX(p.Date), MIN(p.Date) ) / 365)) as PubsPerYear
  FROM PaperAuthorAffiliations AS paa 
  INNER JOIN WosToMag AS wtm ON paa.AuthorId = wtm.MAG 
  INNER JOIN PaperRootFieldMag prf ON paa.PaperId = prf.PaperId
  INNER JOIN Papers p ON p.PaperId = paa.PaperId
  INNER JOIN Affiliations AS a ON paa.AffiliationId = a.AffiliationId
  GROUP BY paa.AuthorId, prf.FieldOfStudyId, CASE WHEN wtm.Gender IN (0, 1) THEN wtm.Gender ELSE -1 END 
  ORDER BY paa.AuthorId
  """
  author_metadata_df = mag.query_sql(query)
  return author_metadata_df

def author_metadata_cross_disciplines(mag):

  author_affiliations = mag.getDataframe('PaperAuthorAffiliations')
  authors = mag.getDataframe('WosToMag')
  paper_root_field = mag.getDataframe('PaperRootFieldMag')
  papers = mag.getDataframe('Papers')
  affiliation = mag.getDataframe('Affiliations')

  query = """
  SELECT paa.AuthorId,
  CASE WHEN wtm.Gender IN (0, 1) THEN wtm.Gender ELSE -1 END as Gender,
  MIN(a.Rank) as MinAffiliationRank,
  COUNT(DISTINCT(paa.PaperId)) as NumPapers,
  MIN(p.Date) as MinPubDate,
  MAX(p.Date) as MaxPubDate, 
  (COUNT(DISTINCT(paa.PaperId)) / (DATEDIFF( MAX(p.Date), MIN(p.Date) ) / 365)) as PubsPerYear
  FROM PaperAuthorAffiliations AS paa 
  INNER JOIN WosToMag AS wtm ON paa.AuthorId = wtm.MAG 
  INNER JOIN Papers p ON p.PaperId = paa.PaperId
  INNER JOIN Affiliations AS a ON paa.AffiliationId = a.AffiliationId
  GROUP BY paa.AuthorId, CASE WHEN wtm.Gender IN (0, 1) THEN wtm.Gender ELSE -1 END 
  ORDER BY paa.AuthorId
  """
  author_metadata_df = mag.query_sql(query)
  return author_metadata_df


def citation_edges(mag):

  author_affiliations = mag.getDataframe('PaperAuthorAffiliations')
  authors = mag.getDataframe('WosToMag')
  paper_references = mag.getDataframe('PaperReferences')
  paper_root_field = mag.getDataframe('PaperRootField')
  # papers = mag.getDataframe('Papers')

  query = """
    SELECT 
      paa1.AuthorId AS CitingAuthorId, 
      pr.PaperId AS CitingPaperId, 
      paa2.AuthorId as CitedAuthorId,
      pr.PaperReferenceId as CitedPaperId
    FROM PaperAuthorAffiliations AS paa1 
    INNER JOIN PaperReferences pr ON paa1.PaperId = pr.PaperId
    INNER JOIN PaperAuthorAffiliations AS paa2 ON pr.PaperReferenceId = paa2.PaperId 
  """

  query = """
    SELECT 
      paa1.AuthorId AS CitingAuthorId, 
      pr.PaperId AS CitingPaperId, 
      prf1.AncestorId as CitingPaperFieldOfStudy,
      wtm1.Gender as CitingAuthorGender,
      paa2.AuthorId as CitedAuthorId,
      pr.PaperReferenceId as CitedPaperId,
      prf2.AncestorId as CitedPaperFieldOfStudy,
      wtm2.Gender as CitedAuthorGender
    FROM PaperAuthorAffiliations AS paa1 
    INNER JOIN PaperReferences pr ON paa1.PaperId = pr.PaperId
    INNER JOIN PaperAuthorAffiliations AS paa2 ON pr.PaperReferenceId = paa2.PaperId 
    INNER JOIN PaperRootField AS prf1 ON paa1.PaperId = prf1.PaperId 
    INNER JOIN PaperRootField AS prf2 ON paa2.PaperId = pr.PaperReferenceId
    INNER JOIN WosToMag AS wtm1 ON paa1.AuthorId = wtm1.MAG 
    INNER JOIN WosToMag AS wtm2 ON paa2.AuthorId = wtm2.MAG 
    WHERE prf1.fieldRank = 1 AND prf2.fieldRank = 1 
    LIMIT 10000
  """
  query = """
    SELECT 
      paa1.AuthorId AS CitingAuthorId, 
      pr.PaperId AS CitingPaperId, 
      paa2.AuthorId as CitedAuthorId,
      pr.PaperReferenceId as CitedPaperId
    FROM PaperAuthorAffiliations AS paa1 
    INNER JOIN PaperReferences pr ON paa1.PaperId = pr.PaperId
    INNER JOIN PaperAuthorAffiliations AS paa2 ON pr.PaperReferenceId = paa2.PaperId 
  """


  citations = mag.query_sql(query)
  return citations




if __name__ == '__main__':

  os.environ["SPARK_LOCAL_DIRS"] = "/home/laal/MAG/TMP"

  spark = SparkSession \
      .builder \
      .config("spark.executor.memory", "16g")\
      .config("spark.driver.memory", "4g")\
      .config("spark.executor.cores", 6)\
      .config("spark.memory.offHeap.enabled", True)\
      .config("spark.memory.offHeap.size","1g")\
      .config("spark.sql.adaptive.enabled", True)\
      .config("spark.sql.adaptive.coalescePartitions.enabled", True)\
      .config("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", False)\
      .appName("MAG app") \
      .getOrCreate()

  mag = MicrosoftAcademicGraph(spark=spark, data_folderpath="/home/laal/MAG/DATA/")

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
  author_metadata = author_metadata_field(mag)
  # print(author_metadata.show(50))
  author_metadata.write.option("sep", "\t").option("encoding", "UTF-8")\
  .csv("/home/laal/MAG/DATA/AuthorMetadataField.csv")

  # CITATIONS
  #citations = citation_edges(mag)

  #print(citations.show(50))
  #print(citations.describe().show())

  #citations.write.option("sep", "\t").option("encoding", "UTF-8")\
  #.csv("/home/agbe/MAG/DATA/Citations.txt")

  spark.stop()

