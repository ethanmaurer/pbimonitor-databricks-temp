-- Databricks notebook source
CREATE TABLE dimDatasets_catalog_copy LOCATION '/mnt/data/transformed/dimDatasets_catalog'
AS 
SELECT * FROM pbimonitor.transformed.dimDatasets_catalog
