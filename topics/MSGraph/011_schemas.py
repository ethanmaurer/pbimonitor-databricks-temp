# Databricks notebook source
# MAGIC %md
# MAGIC #Graph Schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Definitions

# COMMAND ----------

from pyspark.sql.types import *

securityGroups = StructType(
    [
        StructField('id',StringType(),True),
        StructField('displayName',StringType(),True),
        StructField('description',StringType(),True),
        StructField('securityEnabled',StringType(),True),
        StructField('groupTypes',ArrayType(StringType()),True),
        StructField('createdDateTime',StringType(),True),
        StructField('renewedDateTime',StringType(),True),
        StructField('deletedDateTime',StringType(),True)
    ]
)

securityGroupsSchema = StructType(
    [
        StructField('value',ArrayType(securityGroups),True),
        #StructField('ADF_PipelineRunId',StringType(),True),
        #StructField('ADF_PipelineTriggerTime',StringType(),True),
        StructField('processedTimeStamp',StringType(),True),
        StructField('processedDate',StringType(),True)
    ]
)

prepaidUnits = StructType(
    [
        StructField('enabled',StringType(),True),
        StructField('suspended',StringType(),True),
        StructField('warning',StringType(),True),
        StructField('lockedOut',StringType(),True)
    ]
)

servicePlans = StructType(
    [
        StructField('servicePlanId',StringType(),True),
        StructField('servicePlanName',StringType(),True),
        StructField('provisioningStatus',StringType(),True),
        StructField('appliesTo',StringType(),True)
    ]
)

subscribedSKUs = StructType(
    [
        StructField('accountName',StringType(),True),
        StructField('accountId',StringType(),True),
        StructField('appliesTo',StringType(),True),
        StructField('capabilityStatus',StringType(),True),
        StructField('consumedUnits',StringType(),True),
        StructField('id',StringType(),True),
        StructField('skuId',StringType(),True),
        StructField('skuPartNumber',StringType(),True),
        StructField('subscriptionIds',ArrayType(StringType()),True),
        StructField('prepaidUnits',prepaidUnits,True),
        StructField('servicePlans',ArrayType(servicePlans),True)
    ]
)


subscribedSKUsSchema = StructType(
    [
        #StructField('@odata.context',StringType(),True),
        StructField('value',ArrayType(subscribedSKUs),True),
        #StructField('ADF_PipelineRunId',StringType(),True),
        #StructField('ADF_PipelineTriggerTime',StringType(),True),
        StructField('processedTimeStamp',StringType(),True),
        StructField('processedDate',StringType(),True)
    ]
)

assignedLicenses = StructType(
    [
        StructField('disabledPlans',StringType(),True),
        StructField('skuId',StringType(),True)
    ]
)

memberOf = StructType(
    [
        StructField('@odata.type',StringType(),True),
        StructField('id',StringType(),True),
        StructField('deletedDateTime',StringType(),True),
        StructField('classification',StringType(),True),
        StructField('createdDateTime',StringType(),True),
        StructField('creationOptions',StringType(),True),
        StructField('description',StringType(),True),
        StructField('displayName',StringType(),True),
        StructField('expirationDateTime',StringType(),True),
        StructField('groupTypes',StringType(),True),
        StructField('isAssignableToRole',StringType(),True),
        StructField('mail',StringType(),True),
        StructField('mailEnabled',StringType(),True),
        StructField('mailNickname',StringType(),True),
        StructField('membershipRule',StringType(),True),
        StructField('membershipRuleProcessingState',StringType(),True),
        StructField('onPremiseDomainName',StringType(),True),
        StructField('onPremiseLastSyncDate',StringType(),True),
        StructField('onPremiseNetBiosName',StringType(),True),
        StructField('onPremiseSamAccountName',StringType(),True),
        StructField('onPremisesUserPrinciple',StringType(),True),
        StructField('onPremiseSecurityIdentifier',StringType(),True),
        StructField('onPremiseSyncEnabled',StringType(),True),
        StructField('prefferedDataLocation',StringType(),True),
        StructField('prefferedLanguage',StringType(),True),
        StructField('proxyAddress',StringType(),True),
        StructField('resourceBehaviorOptions',StringType(),True),
        StructField('resourceProvisioningOptions',StringType(),True),
        StructField('securityEnabled',StringType(),True),
        StructField('securityIdentifier',StringType(),True),
        StructField('theme',StringType(),True),
        StructField('visablility',StringType(),True),
        StructField('onPremisesProvisioningErrors',StringType(),True),
        StructField('roleTemplateId',StringType(),True)
    ]
)

users = StructType(
    [
        StructField('id',StringType(),True),
        StructField('mail',StringType(),True),
        StructField('companyName',StringType(),True),
        StructField('department',StringType(),True),
        StructField('displayName',StringType(),True),
        StructField('userType',StringType(),True),
        StructField('onPremisesUserPrinciple',StringType(),True),
        StructField('userPrincipalName',StringType(),True),
        StructField('jobTitle',StringType(),True),
        StructField('creationType',StringType(),True),
        StructField('externalUserState',StringType(),True),
        StructField('createdDateTime',StringType(),True),
        StructField('deletedDateTime',StringType(),True),
        StructField('assignedLicenses',ArrayType(assignedLicenses),True),  
        StructField('memberOf',ArrayType(memberOf),True)
    ]
)

usersSchema = StructType(
    [
        StructField('value',ArrayType(users),True),
        #StructField('ADF_PipelineRunId',StringType(),True),
        #StructField('ADF_PipelineTriggerTime',StringType(),True),
        StructField('processedTimeStamp',StringType(),True),
        StructField('processedDate',StringType(),True)
    ]
)

usersInSecurityGroups = StructType(
    [
        #StructField('@odata.type',StringType(),True),
        StructField('id',StringType(),True),
        StructField('businessPhones',StringType(),True),
        StructField('displayName',StringType(),True),
        StructField('givenName',StringType(),True),
        StructField('jobTitle',StringType(),True),
        StructField('mail',StringType(),True),
        StructField('mobilePhone',StringType(),True),
        StructField('officeLocation',StringType(),True),
        StructField('preferredLanguage',StringType(),True),
        StructField('surname',StringType(),True),
        StructField('userPrincipalName',StringType(),True)
    ]
)

usersInSecurityGroupsSchema = StructType(
    [
        #StructField('@odata.context',StringType(),True),
        StructField('value',ArrayType(usersInSecurityGroups),True),
        #StructField('ADF_PipelineRunId',StringType(),True),
        #StructField('ADF_PipelineTriggerTime',StringType(),True),
        StructField('GroupId',StringType(),True),
        StructField('processedTimeStamp',StringType(),True),
        StructField('processedDate',StringType(),True)
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Dictionary

# COMMAND ----------

schemas: dict = {
    'securityGroups': securityGroupsSchema,
    'subscribedSKUs': subscribedSKUsSchema,
    'users': usersSchema,
    'usersInSecurityGroups': usersInSecurityGroupsSchema
}

# COMMAND ----------


