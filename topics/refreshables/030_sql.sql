-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW refreshables
AS (
  SELECT *
  FROM transformed.refreshables
  WHERE processedTimeStamp = (
    SELECT MAX(processedTimeStamp)
    FROM transformed.refreshables
  )
)
