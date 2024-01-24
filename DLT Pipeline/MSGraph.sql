-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ### MSGraph Users

-- COMMAND ----------

CREATE STREAMING TABLE MSGraphUsers_raw AS
SELECT 
    users.*,
    processedTimeStamp,
    TenantId
FROM STREAM read_files("/Volumes/pbimonitor/base/files/MSGraph_users/*", multiLine => true, schema => '
  STRUCT<
    value: ARRAY<
      STRUCT<
        id: STRING,
        mail: STRING,
        companyName: STRING,
        department: STRING,
        displayName: STRING,
        userType: STRING,
        onPremisesUserPrincipal: STRING,
        userPrincipalName: STRING,
        jobTitle: STRING,
        creationType: STRING,
        externalUserState: STRING,
        createdDateTime: STRING,
        deletedDateTime: STRING,
        assignedLicenses: ARRAY<
          STRUCT<
            disabledPlans: STRING,
            skuId: STRING
          >
        >,
        memberOf: ARRAY<
          STRUCT<
            id: STRING,
            deletedDateTime: STRING,
            classification: STRING,
            createdDateTime: STRING,
            creationOptions: STRING,
            description: STRING,
            displayName: STRING,
            expirationDateTime: STRING,
            groupTypes: STRING,
            isAssignableToRole: STRING,
            mail: STRING,
            mailEnabled: STRING,
            mailNickname: STRING,
            membershipRule: STRING,
            membershipRuleProcessingState: STRING,
            onPremiseDomainName: STRING,
            onPremiseLastSyncDate: STRING,
            onPremiseNetBiosName: STRING,
            onPremiseSamAccountName: STRING,
            onPremisesUserPrincipal: STRING,
            onPremiseSecurityIdentifier: STRING,
            onPremiseSyncEnabled: STRING,
            prefferedDataLocation: STRING,
            prefferedLanguage: STRING,
            proxyAddress: STRING,
            resourceBehaviorOptions: STRING,
            resourceProvisioningOptions: STRING,
            securityEnabled: STRING,
            securityIdentifier: STRING,
            theme: STRING,
            visablility: STRING,
            onPremisesProvisioningErrors: STRING,
            roleTemplateId: STRING
          >
        >
      >
    >,
    processedTimeStamp: STRING,
    processedDate: STRING,
    TenantId: STRING
  >
')
LATERAL VIEW explode(value) as users


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Unique Key = `id`

-- COMMAND ----------

CREATE STREAMING TABLE MSGraphUsers_current 

-- COMMAND ----------

APPLY CHANGES INTO LIVE.MSGraphUsers_current
FROM STREAM(LIVE.MSGraphUsers_raw)
KEYS(id)
SEQUENCE BY processedTimeStamp

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### MSGraph Security Groups

-- COMMAND ----------

CREATE STREAMING TABLE MSGraphSecurityGroups_raw AS
 
 SELECT 
    securityGroups.*,
    processedTimeStamp,
    TenantId
    FROM STREAM read_files("/Volumes/pbimonitor/base/files/MSGraph_securityGroups/*",  multiLine=> true, schema => '
  STRUCT<
    value: ARRAY<
      STRUCT<
        id: STRING,
        displayName: STRING,
        description: STRING,
        securityEnabled: STRING,
        groupTypes: ARRAY<STRING>,
        createdDateTime: STRING,
        renewedDateTime: STRING,
        deletedDateTime: STRING
      >
    >,
    processedTimeStamp: STRING,
    processedDate: STRING,
    TenantId: STRING
  >
'
)
    LATERAL VIEW explode(value) as securityGroups
  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Unique Key = `id`

-- COMMAND ----------

CREATE STREAMING TABLE MSGraphSecurityGroups_current 

-- COMMAND ----------

APPLY CHANGES INTO LIVE.MSGraphSecurityGroups_current
FROM STREAM(LIVE.MSGraphSecurityGroups_raw)
KEYS(id)
SEQUENCE BY processedTimeStamp

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### MSGraph Subscribed SKUs

-- COMMAND ----------

CREATE STREAMING TABLE MSGraphSubscribedSKUs_raw AS

  SELECT 
    subscribedSKUs.*,
    processedTimeStamp,
    TenantId
    FROM STREAM read_files("/Volumes/pbimonitor/base/files/MSGraph_subscribedSKUs/*",  multiLine=> true, schema => '
  STRUCT<
    value: ARRAY<
      STRUCT<
        accountName: STRING,
        accountId: STRING,
        appliesTo: STRING,
        capabilityStatus: STRING,
        consumedUnits: STRING,
        id: STRING,
        skuId: STRING,
        skuPartNumber: STRING,
        subscriptionIds: ARRAY<STRING>,
        prepaidUnits: STRUCT<
          enabled: STRING,
          suspended: STRING,
          warning: STRING,
          lockedOut: STRING
        >,
        servicePlans: ARRAY<
          STRUCT<
            servicePlanId: STRING,
            servicePlanName: STRING,
            provisioningStatus: STRING,
            appliesTo: STRING
          >
        >
      >
    >,
    processedTimeStamp: STRING,
    processedDate: STRING,
    TenantId: STRING
  >
'
)
    LATERAL VIEW explode(value) as subscribedSKUs
  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Unique Key = `skuId` (not id)

-- COMMAND ----------

CREATE STREAMING TABLE MSGraphSubscribedSKUs_current 

-- COMMAND ----------

APPLY CHANGES INTO LIVE.MSGraphSubscribedSKUs_current
FROM STREAM(LIVE.MSGraphSubscribedSKUs_raw)
KEYS(skuId)
SEQUENCE BY processedTimeStamp

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### MSGraph Users In Security Groups

-- COMMAND ----------

CREATE STREAMING TABLE MSGraphUsersInSecurityGroups_raw AS

  SELECT 
    usersInSecurityGroups.*,
    processedTimeStamp,
    TenantId
    FROM STREAM read_files("/Volumes/pbimonitor/base/files/MSGraph_usersInSecurityGroups/*",  multiLine=> true, schema => '
  STRUCT<
    value: ARRAY<
      STRUCT<
        id: STRING,
        businessPhones: STRING,
        displayName: STRING,
        givenName: STRING,
        jobTitle: STRING,
        mail: STRING,
        mobilePhone: STRING,
        officeLocation: STRING,
        preferredLanguage: STRING,
        surname: STRING,
        userPrincipalName: STRING
      >
    >,
    GroupId: STRING,
    processedTimeStamp: STRING,
    processedDate: STRING,
    TenantId: STRING
  >
'
)
    LATERAL VIEW explode(value) as usersInSecurityGroups
  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Unique Keys = `id`

-- COMMAND ----------

CREATE STREAMING TABLE MSGraphUsersInSecurityGroups_current 

-- COMMAND ----------

APPLY CHANGES INTO LIVE.MSGraphUsersInSecurityGroups_current
FROM STREAM(LIVE.MSGraphUsersInSecurityGroups_raw)
KEYS(id)
SEQUENCE BY processedTimeStamp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Dim Tables
-- MAGIC

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Users

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimUsers_msGraph
AS (
  SELECT * EXCEPT (assignedLicenses, memberOf)
  FROM LIVE.MSGraphUsers_current
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimAssignedLicenses_msGraph
AS (
  SELECT id AS UserId, json.*, TenantId
  FROM LIVE.MSGraphUsers_current
  LATERAL VIEW explode(assignedLicenses) AS json
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimMemberOf_msGraph
AS (
  SELECT id AS UserId, json.*, TenantId
  FROM LIVE.MSGraphUsers_current
  LATERAL VIEW explode(memberOf) AS json
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Security Groups

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimSecurityGroups_msGraph
AS (
  SELECT *
  FROM LIVE.MSGraphSecurityGroups_current
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Subscribed SKUs

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimSubscribedSKUs_msGraph
AS (
SELECT * EXCEPT (prepaidUnits)
, prepaidUnits.*
FROM LIVE.MSGraphSubscribedSKUs_current
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimSubscriptionIds_msGraph
AS (
  SELECT id AS subscribedSKUId, exploded AS subscriptionIds, TenantId
  FROM LIVE.MSGraphSubscribedSKUs_current
  LATERAL VIEW explode(subscriptionIds) AS exploded
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Users In Security Groups

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimUsersInSecurityGroups_msGraph
AS (
  SELECT *
  FROM LIVE.MSGraphUsersInSecurityGroups_current
)
