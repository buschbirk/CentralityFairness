# Databricks notebook source
# MAGIC %md # Compute h-index for authors
# MAGIC 
# MAGIC #### In this tutorial, you compute h-index for all authors using Azure Databricks. You extract data from Azure Storage into data frames, compute h-index, and visualize the result in table and graph forms.

# COMMAND ----------

# MAGIC %md ## Prerequisites
# MAGIC 
# MAGIC Complete these tasks before you begin this tutorial:
# MAGIC 
# MAGIC - Setting up provisioning of Microsoft Academic Graph to an Azure blob storage account. See [Get Microsoft Academic Graph on Azure storage](get-started-setup-provisioning.md).
# MAGIC - Setting up Azure Databricks service. See [Set up Azure Databricks](get-started-setup-databricks.md).

# COMMAND ----------

# MAGIC %md ## Gather the information
# MAGIC 
# MAGIC Before you begin, you should have these items of information:
# MAGIC 
# MAGIC - The name of your Azure Storage (AS) account containing MAG dataset from [Get Microsoft Academic Graph on Azure storage](get-started-setup-provisioning.md#note-azure-storage-account-name-and-primary-key).
# MAGIC - The access key of your Azure Storage (AS) account from [Get Microsoft Academic Graph on Azure storage](get-started-setup-provisioning.md#note-azure-storage-account-name-and-primary-key).
# MAGIC - The name of the container in your Azure Storage (AS) account containing MAG dataset.

# COMMAND ----------

# MAGIC %md ## Import notebooks
# MAGIC 
# MAGIC - [Import](https://docs.databricks.com/user-guide/notebooks/notebook-manage.html#import-a-notebook) samples/pyspark/MagClass.py under your working folder.
# MAGIC - [Import](https://docs.databricks.com/user-guide/notebooks/notebook-manage.html#import-a-notebook) this notebook (samples/pyspark/HIndexDatabricksSample.py) under the same folder.

# COMMAND ----------

# MAGIC %md ### Initialize storage account and container details
# MAGIC 
# MAGIC   | Variable  | Value | Description  |
# MAGIC   | --------- | --------- | --------- |
# MAGIC   | AzureStorageAccount | Replace **`<AzureStorageAccount>`** | This is the Azure Storage account containing MAG dataset. |
# MAGIC   | AzureStorageAccessKey | Replace **`<AzureStorageAccessKey>`** | This is the Access Key of the Azure Storage account. |
# MAGIC   | MagContainer | Replace **`<MagContainer>`** | This is the container name in Azure Storage account containing MAG dataset, usually in the form of mag-yyyy-mm-dd. |
# MAGIC   | OutputContainer | Replace **`<OutputContainer>`** | This is the container name in Azure Storage account where the output goes to. |

# COMMAND ----------

AzureStorageAccount = '<AzureStorageAccount>'
AzureStorageAccessKey = '<AzureStorageAccessKey>'
MagContainer = '<MagContainer>'
OutputContainer = '<OutputContainer>'

# COMMAND ----------

# MAGIC %md ### Define MicrosoftAcademicGraph class
# MAGIC 
# MAGIC Run the MagClass notebook to define MicrosoftAcademicGraph class.

# COMMAND ----------

# MAGIC %run "./MagClass"

# COMMAND ----------

# MAGIC %md ### Create a MicrosoftAcademicGraph instance to access MAG dataset
# MAGIC Use account=AzureStorageAccount, key=AzureStorageAccessKey, container=MagContainer.

# COMMAND ----------

MAG = MicrosoftAcademicGraph(account=AzureStorageAccount, key=AzureStorageAccessKey, container=MagContainer)

# COMMAND ----------

# MAGIC %md ### Import python libraries

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md ### Get affiliations

# COMMAND ----------

Affiliations = MAG.getDataframe('Affiliations')
Affiliations = Affiliations.select(Affiliations.AffiliationId, Affiliations.DisplayName)
Affiliations.show(3)

# COMMAND ----------

# MAGIC %md ### Get authors

# COMMAND ----------

Authors = MAG.getDataframe('Authors')
Authors = Authors.select(Authors.AuthorId, Authors.DisplayName, Authors.LastKnownAffiliationId, Authors.PaperCount)
Authors.show(3)

# COMMAND ----------

# MAGIC %md ### Get (author, paper) pairs

# COMMAND ----------

PaperAuthorAffiliations = MAG.getDataframe('PaperAuthorAffiliations')
AuthorPaper = PaperAuthorAffiliations.select(PaperAuthorAffiliations.AuthorId, PaperAuthorAffiliations.PaperId).distinct()
AuthorPaper.show(3)

# COMMAND ----------

# MAGIC %md ### Get papers and estimated citation
# MAGIC 
# MAGIC Treat papers with same FamilyId as a single paper and sum EstimatedCitation for all papers with the same FamilyId

# COMMAND ----------

Papers = MAG.getDataframe('Papers')

p = Papers.where(Papers.EstimatedCitation > 0) \
  .select(F.when(Papers.FamilyId.isNull(), Papers.PaperId) \
  .otherwise(Papers.FamilyId).alias('PaperId'), Papers.EstimatedCitation) \
  .alias('p')

PaperCitation = p \
  .groupBy(p.PaperId) \
  .agg(F.sum(p.EstimatedCitation).alias('EstimatedCitation'))

# COMMAND ----------

# MAGIC %md ### Generate author, paper, citation dataframe

# COMMAND ----------

AuthorPaperCitation = AuthorPaper \
    .join(PaperCitation, AuthorPaper.PaperId == PaperCitation.PaperId, 'inner') \
    .select(AuthorPaper.AuthorId, AuthorPaper.PaperId, PaperCitation.EstimatedCitation)

# COMMAND ----------

# MAGIC %md ### Order by citation

# COMMAND ----------

AuthorPaperOrderByCitation = AuthorPaperCitation \
  .withColumn('Rank', F.row_number().over(Window.partitionBy('AuthorId').orderBy(F.desc('EstimatedCitation'))))

# COMMAND ----------

# MAGIC %md ### Generate author hindex

# COMMAND ----------

ap = AuthorPaperOrderByCitation.alias('ap')
AuthorHIndexTemp = ap \
  .groupBy(ap.AuthorId) \
  .agg(F.sum(ap.EstimatedCitation).alias('TotalEstimatedCitation'), \
       F.max(F.when(ap.EstimatedCitation >= ap.Rank, ap.Rank).otherwise(0)).alias('HIndex'))

# COMMAND ----------

# MAGIC %md ### Get author detail information

# COMMAND ----------

i = AuthorHIndexTemp.alias('i')
a = Authors.alias('a')
af = Affiliations.alias('af')

AuthorHIndex = i \
  .join(a, a.AuthorId == i.AuthorId, 'inner') \
  .join(af, a.LastKnownAffiliationId == af.AffiliationId, 'outer') \
  .select(i.AuthorId, a.DisplayName, af.DisplayName.alias('AffiliationDisplayName'), a.PaperCount, i.TotalEstimatedCitation, i.HIndex)

# COMMAND ----------

# MAGIC %md ### Display top authors

# COMMAND ----------

TopAuthorHIndex = AuthorHIndex \
  .select(AuthorHIndex.DisplayName, AuthorHIndex.AffiliationDisplayName, AuthorHIndex.PaperCount, AuthorHIndex.TotalEstimatedCitation, AuthorHIndex.HIndex) \
  .orderBy(F.desc('HIndex')) \
  .limit(100)
display(TopAuthorHIndex)

# COMMAND ----------

# MAGIC %md ### Create an AzureStorageUtil instance for output

# COMMAND ----------

ASU = AzureStorageUtil(container=OutputContainer, account=AzureStorageAccount, key=AzureStorageAccessKey)

# COMMAND ----------

# MAGIC %md ### Save result as an Azure Storage blob

# COMMAND ----------

ASU.save(AuthorHIndex, 'AuthorHIndex.csv', coalesce=True)
