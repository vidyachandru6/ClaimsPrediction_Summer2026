-- Step 1: Run the show command
SHOW COLUMNS IN TABLE VYTALIZE_0171.DEV_SCHEMA.CLMHDRS_TABLE;

-- Step 2: Extract just the column headers as an array list
SELECT ARRAY_AGG("column_name") AS column_headers 
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));


select cast(OMC_CLM_ID as integer) as OMC_CLM_ID
    from VYTALIZE_0171.DEV_SCHEMA.CLMDTLS_TABLE;
select cast(OMC_CLM_ID as integer) as OMC_CLM_ID
    from VYTALIZE_0171.DEV_SCHEMA.CLMHDRS_TABLE;

select * from VYTALIZE_0171.DEV_SCHEMA.CLMDTLS_TABLE
    order by OMC_CLM_ID;
    
select * from VYTALIZE_0171.DEV_SCHEMA.CLMHDRS_TABLE
    order by OMC_CLM_ID;

SELECT COUNT(*) AS TOTAL_ROWS 
FROM VYTALIZE_0171.DEV_SCHEMA.CLMDTLS_TABLE;

SELECT COUNT(*) AS TOTAL_ROWS 
FROM VYTALIZE_0171.DEV_SCHEMA.CLMHDRS_TABLE;

create or replace table VYTALIZE_0171.DEV_SCHEMA.clmdtlshdrs as
select cd.OMC_CLM_ID,
    cd.OMC_CLM_DTLS_ID,
    cd.LINE_NUM,
    cd.INC_FROM,
    cd.INC_TO,
    cd.ICD_CD,
    cd.OMC_ICD_RISK_NBR,
    cd.CPT_CD,
    cd.SUB_TYPE,
    cd.CLMS_CHRG,
    cd.PAID_AMT,
    ch.MBR_ID,
    from VYTALIZE_0171.DEV_SCHEMA.CLMDTLS_TABLE as cd
    inner join VYTALIZE_0171.DEV_SCHEMA.CLMHDRS_TABLE as ch
    on cd.OMC_CLM_ID = ch.OMC_CLM_ID;

SELECT COUNT(*) AS TOTAL_ROWS 
FROM VYTALIZE_0171.DEV_SCHEMA.clmdtlshdrs;


select cast(MBR_ID as string) as MBR_ID,
    from VYTALIZE_0171.DEV_SCHEMA.clmdtlshdrs;  -- Note: this table is created by a prior statement in this file

select cast(MBR_ID as string) as MBR_ID,
       cast(FST_NAME as string) as FST_NAME,
       cast(MDL_NAME as string) as MDL_NAME,
       cast(LST_NAME as string) as LST_NAME
    from VYTALIZE_0171.DEV_SCHEMA.PTDTLS_TABLE;
    
create or replace table VYTALIZE_0171.DEV_SCHEMA.clmpt as
select cdh.OMC_CLM_ID,
    cdh.MBR_ID,
    cdh.LINE_NUM,
    cdh.INC_FROM,
    cdh.INC_TO,
    cdh.ICD_CD,
    cdh.OMC_ICD_RISK_NBR,
    cdh.CPT_CD,
    cdh.SUB_TYPE,
    cdh.CLMS_CHRG,
    cdh.PAID_AMT,
    pt.FST_NAME,
    pt.MDL_NAME,
    pt.LST_NAME,
    pt.PT_GNDR,
    pt.PT_DOB,
    pt.PT_ST,
    pt.pt_ZIP
    from VYTALIZE_0171.DEV_SCHEMA.clmdtlshdrs as cdh
    inner join VYTALIZE_0171.DEV_SCHEMA.PTDTLS_TABLE as pt
    on cdh.MBR_ID = pt.MBR_ID;


SELECT COUNT(*) AS TOTAL_ROWS 
FROM VYTALIZE_0171.DEV_SCHEMA.clmpt;
