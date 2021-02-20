# -*- coding: utf-8 -*-
"""
Created on Mon Feb 15 19:38:10 2021

@author: lasse
"""

import pandas as pd
import json


# streams taken from sparktest.py
streams = {
    'Affiliations' : ('mag/Affiliations.txt', ['AffiliationId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'GridId:string', 'OfficialPage:string', 'WikiPage:string', 'PaperCount:long', 'PaperFamilyCount:long', 'CitationCount:long', 'Iso3166Code:string', 'Latitude:float?', 'Longitude:float?', 'CreatedDate:DateTime']),
    'AuthorExtendedAttributes' : ('mag/AuthorExtendedAttributes.txt', ['AuthorId:long', 'AttributeType:int', 'AttributeValue:string']),
    'Authors' : ('mag/Authors.txt', ['AuthorId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'LastKnownAffiliationId:long?', 'PaperCount:long', 'PaperFamilyCount:long', 'CitationCount:long', 'CreatedDate:DateTime']),
    'ConferenceInstances' : ('mag/ConferenceInstances.txt', ['ConferenceInstanceId:long', 'NormalizedName:string', 'DisplayName:string', 'ConferenceSeriesId:long', 'Location:string', 'OfficialUrl:string', 'StartDate:DateTime?', 'EndDate:DateTime?', 'AbstractRegistrationDate:DateTime?', 'SubmissionDeadlineDate:DateTime?', 'NotificationDueDate:DateTime?', 'FinalVersionDueDate:DateTime?', 'PaperCount:long', 'PaperFamilyCount:long', 'CitationCount:long', 'Latitude:float?', 'Longitude:float?', 'CreatedDate:DateTime']),
    'ConferenceSeries' : ('mag/ConferenceSeries.txt', ['ConferenceSeriesId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'PaperCount:long', 'PaperFamilyCount:long', 'CitationCount:long', 'CreatedDate:DateTime']),
    'EntityRelatedEntities' : ('advanced/EntityRelatedEntities.txt', ['EntityId:long', 'EntityType:string', 'RelatedEntityId:long', 'RelatedEntityType:string', 'RelatedType:int', 'Score:float']),
    'FieldOfStudyChildren' : ('advanced/FieldOfStudyChildren.txt', ['FieldOfStudyId:long', 'ChildFieldOfStudyId:long']),
    'FieldOfStudyExtendedAttributes' : ('advanced/FieldOfStudyExtendedAttributes.txt', ['FieldOfStudyId:long', 'AttributeType:int', 'AttributeValue:string']),
    'FieldsOfStudy' : ('advanced/FieldsOfStudy.txt', ['FieldOfStudyId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'MainType:string', 'Level:int', 'PaperCount:long', 'PaperFamilyCount:long', 'CitationCount:long', 'CreatedDate:DateTime']),
    'Journals' : ('mag/Journals.txt', ['JournalId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'Issn:string', 'Publisher:string', 'Webpage:string', 'PaperCount:long', 'PaperFamilyCount:long', 'CitationCount:long', 'CreatedDate:DateTime']),
    'PaperAbstractsInvertedIndex' : ('nlp/PaperAbstractsInvertedIndex.txt.{*}', ['PaperId:long', 'IndexedAbstract:string']),
    'PaperAuthorAffiliations' : ('mag/PaperAuthorAffiliations.txt', ['PaperId:long', 'AuthorId:long', 'AffiliationId:long?', 'AuthorSequenceNumber:uint', 'OriginalAuthor:string', 'OriginalAffiliation:string']),
    'PaperCitationContexts' : ('nlp/PaperCitationContexts.txt', ['PaperId:long', 'PaperReferenceId:long', 'CitationContext:string']),
    'PaperExtendedAttributes' : ('mag/PaperExtendedAttributes.txt', ['PaperId:long', 'AttributeType:int', 'AttributeValue:string']),
    'PaperFieldsOfStudy' : ('advanced/PaperFieldsOfStudy.txt', ['PaperId:long', 'FieldOfStudyId:long', 'Score:float']),
    'PaperMeSH' : ('advanced/PaperMeSH.txt', ['PaperId:long', 'DescriptorUI:string', 'DescriptorName:string', 'QualifierUI:string', 'QualifierName:string', 'IsMajorTopic:bool']),
    'PaperRecommendations' : ('advanced/PaperRecommendations.txt', ['PaperId:long', 'RecommendedPaperId:long', 'Score:float']),
    'PaperReferences' : ('mag/PaperReferences.txt', ['PaperId:long', 'PaperReferenceId:long']),
    'PaperResources' : ('mag/PaperResources.txt', ['PaperId:long', 'ResourceType:int', 'ResourceUrl:string', 'SourceUrl:string', 'RelationshipType:int']),
    'PaperUrls' : ('mag/PaperUrls.txt', ['PaperId:long', 'SourceType:int?', 'SourceUrl:string', 'LanguageCode:string']),
    'Papers' : ('mag/Papers.txt', ['PaperId:long', 'Rank:uint', 'Doi:string', 'DocType:string', 'PaperTitle:string', 'OriginalTitle:string', 'BookTitle:string', 'Year:int?', 'Date:DateTime?', 'OnlineDate:DateTime?', 'Publisher:string', 'JournalId:long?', 'ConferenceSeriesId:long?', 'ConferenceInstanceId:long?', 'Volume:string', 'Issue:string', 'FirstPage:string', 'LastPage:string', 'ReferenceCount:long', 'CitationCount:long', 'EstimatedCitation:long', 'OriginalVenue:string', 'FamilyId:long?', 'FamilyRank:uint?', 'CreatedDate:DateTime']),
    'RelatedFieldOfStudy' : ('advanced/RelatedFieldOfStudy.txt', ['FieldOfStudyId1:long', 'Type1:string', 'FieldOfStudyId2:long', 'Type2:string', 'Rank:float']),
  }





def map_fos_children(fos_df, level=0):
    
    child_fos_df = pd.read_csv('MAG_FILES/FieldOfStudyChildren.txt', sep=r"\t", header=None, names=['fos_id', 'child_id'])
    
    mapping = {}
    
    for idx, row in child_fos_df.iterrows():
        mapping[row['child_id']] = row['fos_id']
    
    
    # mapping = {child_fos_df.iloc[i]['child_id']: child_fos_df.iloc[i]['fos_id'] for i in range(child_fos_df.shape[0])}
    
    for child, parent in mapping.items():
        
        ancestor = mapping[child]
        while ancestor in mapping:
            ancestor = mapping[ancestor]
            
        mapping[child] = ancestor
        
    with open('FieldOfStudyRoot.txt', 'w') as file:
        for child, parent in mapping.items():
            file.write("{},{}\n".format(child, parent))
        
    return mapping


if __name__ == '__main__':

    field_of_study_cols = [col.split(":")[0] for col in streams['FieldsOfStudy'][1]]
    
    fos_df = pd.read_csv('MAG_FILES/FieldsOfStudy.txt', sep=r"\t", header=None, names=field_of_study_cols)
    fos_df.sort_values(by='Level', inplace=True)
    fos_df.to_csv('field_of_study.csv', index=False)
    
    mapping = map_fos_children(fos_df, level=0)
        
    
    
    
    
    
    





