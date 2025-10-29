-- Databricks notebook source
CREATE SCHEMA Farhan

-- COMMAND ----------

CREATE TABLE Farhan.khantbl
(
  Id int,
  salary int
)
USING DELTA
LOCATION "/FileStore/data/deltatable"

-- COMMAND ----------

INSERT INTO Farhan.khantbl
VALUES
(4,100),
(5,200),
(6,250)

-- COMMAND ----------

OPTIMIZE delta.`/FileStore/data/deltatable` ZORDER BY (Id)

-- COMMAND ----------

