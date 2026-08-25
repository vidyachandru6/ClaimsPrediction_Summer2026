USE DATABASE VYTALIZE_0171;
USE SCHEMA VYTALIZE_0171.DEV_SCHEMA;
-- Creates an Azure Blob Storage integration for the 0171 container
-- Co-authored with CoCo
CREATE OR REPLACE STORAGE INTEGRATION azure_blob_0171_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = 'a668a203-a0d1-4646-8965-ee410604058b'
  STORAGE_ALLOWED_LOCATIONS = ('azure://vytalize.blob.core.windows.net/0171/');

DESCRIBE INTEGRATION azure_blob_0171_int;

CREATE OR REPLACE STAGE VYTALIZE_0171_stage
  URL = 'azure://vytalize.blob.core.windows.net/0171/'
  STORAGE_INTEGRATION = azure_blob_0171_int
  FILE_FORMAT = claims_csv_format;

LIST @VYTALIZE_0171_stage;

