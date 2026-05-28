-- Runs first (alphabetically before schema.sql) during postgres init.
-- Creates the Airflow metadata database inside the same postgres instance.
CREATE DATABASE airflow;
