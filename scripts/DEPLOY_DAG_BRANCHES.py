from datetime import timedelta
from snowflake.core import Root
from snowflake.core.task import Cron
from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation, DAGTaskBranch
from snowflake.snowpark.context import get_active_session
from snowflake.ml.registry import Registry
from snowflake.snowpark import Session

#session = get_active_session()

print(f"Importing done")

# 1. Define the branch evaluation logic in Python
def NewData_check(session: Session) -> str:
    result = session.sql("CALL CHECK_NEW_UPLOAD()").collect()
    return result

def DataDrift_check(session: Session) -> str:
    result = session.sql(f""" select *
        FROM {database_name}.{schema_name}.TRAININGFEATURESPSI
        CASE 
            WHEN GREATEST(PT_AGE, PT_ZIP,ICDCD_NUMCODED, CLM_DIS_RISK_NBR,SUBCD_NBR) > 0.5 THEN TRUE
            ELSE FALSE
        END AS exceeds_threshold
        FROM {database_name}.{schema_name}.TRAININGFEATURESPSI
        """).collect()
    return result

def TrainedModel_check(session: Session) -> str:
    reg = Registry(
    session=session, 
    database_name=database_name, 
    schema_name=model_schema
    )
    df_models = reg.show_models()
    if len(df_models)>0:
        filtered_models = df_models[df_models['name'].str.contains(ModelName, case=False, na=False)]

    if (len(filtered_models)>0):
        result = True
    else:
        result = False
    return result

def ModelPredictions_check(session: Session) -> str:

    results= session.sql(f"SHOW TABLES LIKE 'ClmCostPredictions_2026' IN {database_name}.{schema_name}").collect()
            
    if len(results)>0:
        # file exists
        result = True
    else:
        result = False
    return result

print(f"Branching logic functions definitions parsed successfully")

# Create the tasks using the DAG API
def main(session: Session, database_name: str, schema_name: str, notebook_project_name: str) -> str:
    # Set the environment context
#    session.use_schema(f"{database_name}.{schema_name}")
    print(f"Before environment variables")
    warehouse_name = "OMC_DATA_XSMALL"
    dag_name = "VYTALIZE_CLMCOST_PREDICTION_NESTEDBRANCH_DAG"
    compute_pool = "SYSTEM_COMPUTE_POOL_CPU"
    runtime = "V2.5-CPU-PY3.12"
    artifact_repository = "SNOWFLAKE.SNOWPARK.PYPI_SHARED_REPOSITORY"
    ModelName = "XGBRegressor_clmsPredmodel_0171"
    
    
    
    # 2. Build the Directed Acyclic Graph (DAG)
    # Ensure use_func_return_value=True so the DAG engine reads the returned string
    with DAG (
        "Data_Model_Conditional_dag", schedule = timedelta(days=1), warehouse=warehouse_name, 
        use_func_return_value=True
    ) as dag:
        # Branching task
        """dag tasks with substring 'dataprep' specify that the flow is for dataprep"""
        print(f"{dag_name}")
        task_dataprep_sqljoins = DAGTask("RUN_MODEL_PREDICTIONS_TESTDATAS_TASK", definition=f'''
            EXECUTE NOTEBOOK PROJECT {database_name}.{schema_name}.{notebook_project_name}
                MAIN_FILE = 'sql_joins.sql'
                COMPUTE_POOL = {compute_pool}
                RUNTIME = '{runtime}'
                QUERY_WAREHOUSE = {warehouse_name}
                ARTIFACT_REPOSITORIES = ({artifact_repository})
                ARGUMENTS = '--database-name {database_name} --schema-name {schema_name}'
            ''', warehouse=warehouse_name)
        print(f"Dataprep sql joins task parsed successfully")
        task_dataprep_numcoding = DAGTask("NUMBERCODING_TASK", definition=f'''
            EXECUTE NOTEBOOK PROJECT {database_name}.{schema_name}.{notebook_project_name}
                MAIN_FILE = 'NumCoding.ipynb'
                COMPUTE_POOL = {compute_pool}
                RUNTIME = '{runtime}'
                QUERY_WAREHOUSE = {warehouse_name}
                ARGUMENTS = '--database-name {database_name} --schema-name {schema_name}'
            ''', warehouse=warehouse_name)
        print(f"Dataprep  task parnumbercoding parsed successfully")
        task_train = DAGTask("TRAIN_MODEL_ON_NEWDATA", definition=f'''
            EXECUTE NOTEBOOK PROJECT {database_name}.{schema_name}.{notebook_project_name}
                MAIN_FILE = 'Model_train.ipynb'
                COMPUTE_POOL = {compute_pool}
                RUNTIME = '{runtime}'
                QUERY_WAREHOUSE = {warehouse_name}
                ARTIFACT_REPOSITORIES = ({artifact_repository})
                ARGUMENTS = '--database-name {database_name} --schema-name {schema_name}'
            ''', warehouse=warehouse_name)
        print(f"Model train task parsed successfully")
        task_test = DAGTask(
            name = "TestModel",
            definition = f'''
            EXECUTE NOTEBOOK PROJECT {database_name}.{schema_name}.{notebook_project_name}
                MAIN_FILE = 'Model_test.ipynb'
                COMPUTE_POOL = {compute_pool}
                RUNTIME = '{runtime}'
                QUERY_WAREHOUSE = {warehouse_name}
                ARTIFACT_REPOSITORIES = ({artifact_repository})
                ARGUMENTS = '--database-name {database_name} --schema-name {schema_name}'
            ''', warehouse=warehouse_name
        )
        
        task_datadriftcalc = DAGTask(
            name = "CalculateDataDrift",
            definition = f"""
            EXECUTE NOTEBOOK PROJECT {database_name}.{schema_name}.{notebook_project_name}
                MAIN_FILE = 'DataDriftDriftDetection.ipynb'
                COMPUTE_POOL = {compute_pool}
                RUNTIME = '{runtime}'
                QUERY_WAREHOUSE = {warehouse_name}
                ARTIFACT_REPOSITORIES = ({artifact_repository})
                ARGUMENTS = '--database-name {database_name} --schema-name {schema_name}'
            """, warehouse=warehouse_name
        )
        
        
        NEWDATA_task = DAGTaskBranch(name = "NewData_check", 
                               definition = NewData_check,
                               warehouse = warehouse_name
        )
        
        DATADRIFT_task = DAGTask(name = "DataDrift_check",
                                 definition = DataDrift_check,
                                 warehouse = warehouse_name
        )
        
        TRAINEDMODEL_task = DAGTaskBranch(name="TrainedModel_check",
                                   definition = TrainedModel_check,
                                   warehouse = warehouse_name
        )
        
        MODELPREDICTIONS_task = DAGTask(name="ModelPredictions_check",
                                   definition = ModelPredictions_check,
                                   warehouse = warehouse_name
        )
        print(f"ModelPredictions table check task parsed successfully")
        # Build the new data flow task flow graph
        TRAINEDMODEL_task >> [task_test, task_datadriftcalc, DATADRIFT_task]
        NEWDATA_task >> [task_dataprep_sqljoins, task_dataprep_numcoding, task_train, TRAINEDMODEL_task]
        DATADRIFT_task >> [task_train, TRAINEDMODEL_task]

    schema = Root.databases["my_db"].schemas["my_schema"]
    op = DAGOperation(schema)
    op.deploy(dag, mode=CreateMode.or_replace)

            # For local debugging
    if __name__ == "__main__":
        import sys
        from snowflake.snowpark.context import get_active_session
    
        # Get a Snowpark session (works in notebook, local, and CI/CD)
        # Note: Session is intentionally never closed to avoid issues in notebooks
        session = get_active_session()
if __name__ == "__main__":
    import sys
    from session_utils import get_snowpark_session

    # Get a Snowpark session (works in notebook, local, and CI/CD)
    # Note: Session is intentionally never closed to avoid issues in notebooks
    session = get_snowpark_session()

    if len(sys.argv) > 3:
        print(main(session, sys.argv[1], sys.argv[2], sys.argv[3]))
    else:
        print("Usage: python DEPLOY_DAG_BRANCHES.py <database> <schema> <notebook_project>")
        
        
