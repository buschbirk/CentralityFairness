# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 06:05:03 2021

@author: lasse
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Feb 15 18:11:38 2021

@author: lasse
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *


class MicrosoftAcademicGraph(object):
  # constructor
  def __init__(self, spark, data_folderpath="/home/laal/MAG/DATA/"):
    # AzureStorageAccess.__init__(self, container, account, sas, key) 
    self.data_folderpath = data_folderpath
    self.spark = spark

  datatypedict = {
    'bool' : BooleanType(),
    'int' : IntegerType(),
    'uint' : IntegerType(),
    'long' : LongType(),
    'ulong' : LongType(),
    'float' : FloatType(),
    'string' : StringType(),
    'DateTime' : DateType(),
  }

  # return stream schema
  def getSchema(self, streamName):
    schema = StructType()
    for field in self.streams[streamName][1]:
      fieldname, fieldtype = field.split(':')
      nullable = fieldtype.endswith('?')
      if nullable:
        fieldtype = fieldtype[:-1]
      schema.add(StructField(fieldname, self.datatypedict[fieldtype], nullable))
    return schema

  # return stream dataframe
  def getDataframe(self, streamName):

    df = self.spark.read.format('csv').options(header='false', delimiter='\t').schema(self.getSchema(streamName))\
           .load(self.data_folderpath + self.streams[streamName][0])
    # create temporary view for streamName
    df.createOrReplaceTempView(streamName)
    return df


  def query_sql(self, query):
    """ 
    Executes Spark SQL query and returns DataFrame with results.
    Assumes views are created prior to execution
    """
    df = self.spark.sql(query)
    return df

  # define stream dictionary
  streams = {
    'Affiliations' : ('Affiliations.txt', ['AffiliationId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'GridId:string', 'OfficialPage:string', 'WikiPage:string', 'PaperCount:long', 'PaperFamilyCount:long', 'CitationCount:long', 'Iso3166Code:string', 'Latitude:float?', 'Longitude:float?', 'CreatedDate:DateTime']),
    # 'AuthorExtendedAttributes' : ('mag/AuthorExtendedAttributes.txt', ['AuthorId:long', 'AttributeType:int', 'AttributeValue:string']),
    'Authors' : ('Authors.txt', ['AuthorId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'LastKnownAffiliationId:long?', 'PaperCount:long', 'PaperFamilyCount:long', 'CitationCount:long', 'CreatedDate:DateTime']),
    #'ConferenceInstances' : ('mag/ConferenceInstances.txt', ['ConferenceInstanceId:long', 'NormalizedName:string', 'DisplayName:string', 'ConferenceSeriesId:long', 'Location:string', 'OfficialUrl:string', 'StartDate:DateTime?', 'EndDate:DateTime?', 'AbstractRegistrationDate:DateTime?', 'SubmissionDeadlineDate:DateTime?', 'NotificationDueDate:DateTime?', 'FinalVersionDueDate:DateTime?', 'PaperCount:long', 'PaperFamilyCount:long', 'CitationCount:long', 'Latitude:float?', 'Longitude:float?', 'CreatedDate:DateTime']),
    #'ConferenceSeries' : ('mag/ConferenceSeries.txt', ['ConferenceSeriesId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'PaperCount:long', 'PaperFamilyCount:long', 'CitationCount:long', 'CreatedDate:DateTime']),
    #'EntityRelatedEntities' : ('advanced/EntityRelatedEntities.txt', ['EntityId:long', 'EntityType:string', 'RelatedEntityId:long', 'RelatedEntityType:string', 'RelatedType:int', 'Score:float']),
    'FieldOfStudyChildren' : ('FieldOfStudyChildren.txt', ['FieldOfStudyId:long', 'ChildFieldOfStudyId:long']),
    #'FieldOfStudyExtendedAttributes' : ('advanced/FieldOfStudyExtendedAttributes.txt', ['FieldOfStudyId:long', 'AttributeType:int', 'AttributeValue:string']),
    'FieldsOfStudy' : ('FieldsOfStudy.txt', ['FieldOfStudyId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'MainType:string', 'Level:int', 'PaperCount:long', 'PaperFamilyCount:long', 'CitationCount:long', 'CreatedDate:DateTime']),
    #'Journals' : ('mag/Journals.txt', ['JournalId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'Issn:string', 'Publisher:string', 'Webpage:string', 'PaperCount:long', 'PaperFamilyCount:long', 'CitationCount:long', 'CreatedDate:DateTime']),
    #'PaperAbstractsInvertedIndex' : ('nlp/PaperAbstractsInvertedIndex.txt.{*}', ['PaperId:long', 'IndexedAbstract:string']),
    'PaperAuthorAffiliations' : ('PaperAuthorAffiliations.txt', ['PaperId:long', 'AuthorId:long', 'AffiliationId:long?', 'AuthorSequenceNumber:uint', 'OriginalAuthor:string', 'OriginalAffiliation:string']),
    #'PaperCitationContexts' : ('nlp/PaperCitationContexts.txt', ['PaperId:long', 'PaperReferenceId:long', 'CitationContext:string']),
    #'PaperExtendedAttributes' : ('mag/PaperExtendedAttributes.txt', ['PaperId:long', 'AttributeType:int', 'AttributeValue:string']),
    'PaperFieldsOfStudy' : ('PaperFieldsOfStudy.txt', ['PaperId:long', 'FieldOfStudyId:long', 'Score:float']),
    #'PaperMeSH' : ('advanced/PaperMeSH.txt', ['PaperId:long', 'DescriptorUI:string', 'DescriptorName:string', 'QualifierUI:string', 'QualifierName:string', 'IsMajorTopic:bool']),
    #'PaperRecommendations' : ('advanced/PaperRecommendations.txt', ['PaperId:long', 'RecommendedPaperId:long', 'Score:float']),
    'PaperReferences' : ('PaperReferences.txt', ['PaperId:long', 'PaperReferenceId:long']),
    #'PaperResources' : ('mag/PaperResources.txt', ['PaperId:long', 'ResourceType:int', 'ResourceUrl:string', 'SourceUrl:string', 'RelationshipType:int']),
    #'PaperUrls' : ('mag/PaperUrls.txt', ['PaperId:long', 'SourceType:int?', 'SourceUrl:string', 'LanguageCode:string']),
    'Papers' : ('Papers.txt', ['PaperId:long', 'Rank:uint', 'Doi:string', 'DocType:string', 'PaperTitle:string', 'OriginalTitle:string', 'BookTitle:string', 'Year:int?', 'Date:DateTime?', 'OnlineDate:DateTime?', 'Publisher:string', 'JournalId:long?', 'ConferenceSeriesId:long?', 'ConferenceInstanceId:long?', 'Volume:string', 'Issue:string', 'FirstPage:string', 'LastPage:string', 'ReferenceCount:long', 'CitationCount:long', 'EstimatedCitation:long', 'OriginalVenue:string', 'FamilyId:long?', 'FamilyRank:uint?', 'CreatedDate:DateTime']),
    'RelatedFieldOfStudy' : ('RelatedFieldOfStudy.txt', ['FieldOfStudyId1:long', 'Type1:string', 'FieldOfStudyId2:long', 'Type2:string', 'Rank:float']),
    'WosToMag': ('WosToMag.txt', ['WOS:long', 'MAG:long', 'Gender:int']),
    'FieldOfStudyRoot': ('FieldOfStudyRoot.txt', ['ChildId:long', 'AncestorId:long']),
    'AuthorCountry': ('AuthorCountry.txt', ['AuthorId:long', 'CountryCode:string', 'NumAffiliations:int', 'CountryRank:int']),
    'PaperRootField': ('PaperRootField.csv', ['PaperId:long', 'AncestorId:long', 'NumSubfieldsInField:int', 'fieldRank:int']),
    'PaperRootFieldMag': ('PaperRootFieldMag.txt', ['PaperId:long', 'FieldOfStudyId:long']),
    'AuthorRootField': ('AuthorRootField.csv', ['AuthorId:long', 'AncestorId:long', 'NumPapersInField:int', 'fieldRank:int']),
    'AuthorsPerPaper': ('AuthorsPerPaper.txt', ['PaperId:long', 'NumAuthors:int', 'Weight:float', 'NumMagGendered:int']),
    'Citations': ('Citations.tsv', ['CitingAuthorId:long', 'CitingPaperId:long', 'CitingAuthorGender:int', 'CitedAuthorId:long', 'CitedPaperId:long', 'CitedAuthorGender:int']),
    'CitationsFromUnknownGender': ('CitationsFromUnknownGender.txt', ['CitingAuthorId:long', 'CitingPaperId:long', 'CitingAuthorGender:int', 'CitedAuthorId:long', 'CitedPaperId:long', 'CitedAuthorGender:int']),
    'CitationsToUnknownGender': ('CitationsToUnknownGender.txt', ['CitingAuthorId:long', 'CitingPaperId:long', 'CitingAuthorGender:int', 'CitedAuthorId:long', 'CitedPaperId:long', 'CitedAuthorGender:int']),
    'PaperAuthorAffiliationsGendered' : ('PaperAuthorAffiliationsGendered.txt', ['PaperId:long', 'AuthorId:long', 'Gender:int']),
    'PaperAuthorAffiliationsGenderedFull' : ('PaperAuthorAffiliationsGenderedFull.txt', ['PaperId:long', 'AuthorId:long', 'Gender:int']),
    'PaperReferencesMedicine' : ('PaperReferencesMedicine.txt', ['PaperId:long', 'PaperReferenceId:long']),
    'PaperReferencesBiology' : ('PaperReferencesBiology.txt', ['PaperId:long', 'PaperReferenceId:long']),
    'PaperReferencesChemistry' : ('PaperReferencesChemistry.txt', ['PaperId:long', 'PaperReferenceId:long']),
    'PaperReferencesPsychology' : ('PaperReferencesPsychology.txt', ['PaperId:long', 'PaperReferenceId:long']),
    'PaperReferencesMathematics' : ('PaperReferencesMathematics.txt', ['PaperId:long', 'PaperReferenceId:long']),
    'PaperReferencesEconomics' : ('PaperReferencesEconomics.txt', ['PaperId:long', 'PaperReferenceId:long']),
    'CitationsBiology': ('CitationsBiology.txt', ['CitingAuthorId:long', 'CitingPaperId:long', 'CitingAuthorGender:int', 'CitedAuthorId:long', 'CitedPaperId:long', 'CitedAuthorGender:int']),
    'CitationsChemistry': ('CitationsChemistry.txt', ['CitingAuthorId:long', 'CitingPaperId:long', 'CitingAuthorGender:int', 'CitedAuthorId:long', 'CitedPaperId:long', 'CitedAuthorGender:int']),
    'CitationsMathematics': ('CitationsMathematics.txt', ['CitingAuthorId:long', 'CitingPaperId:long', 'CitingAuthorGender:int', 'CitedAuthorId:long', 'CitedPaperId:long', 'CitedAuthorGender:int']),
    'CitationsMedicine': ('CitationsMedicine.txt', ['CitingAuthorId:long', 'CitingPaperId:long', 'CitingAuthorGender:int', 'CitedAuthorId:long', 'CitedPaperId:long', 'CitedAuthorGender:int']),
    'CitationsPsychology': ('CitationsPsychology.txt', ['CitingAuthorId:long', 'CitingPaperId:long', 'CitingAuthorGender:int', 'CitedAuthorId:long', 'CitedPaperId:long', 'CitedAuthorGender:int']),
    'CitationsEconomics': ('CitationsEconomics.txt', ['CitingAuthorId:long', 'CitingPaperId:long', 'CitingAuthorGender:int', 'CitedAuthorId:long', 'CitedPaperId:long', 'CitedAuthorGender:int']),
  }


