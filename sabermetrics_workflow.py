import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 12, 02)
}

staging_dataset = 'sabermetrics_workflow_staging'
modeled_dataset = 'sabermetrics_workflow_modeled'

bq_query_start = 'bq query --use_legacy_sql=false '

create_jBat_sql =  'create or replace table ' + modeled_dataset + '''.jBat as
                    select distinct row_number() over (partition BY Year) as index, Year, City, Team, TeamID, LName, FName, 
                    safe_cast(Avg as FLOAT64) as Avg, safe_cast(SLG as FLOAT64) as SLG, 
                    safe_cast(OBP as FLOAT64) as OBP, safe_cast(RiSP as FLOAT64) as RiSP, 
                    G, AB, R, H, _2B, _3B, HR, RBI, BB, HBP, SO, SB, GIDP, safe_cast(PA as INT64) as PA, 
                    Sac, E, PlayerID, Uniform, safe_cast(ARISP as FLOAT64) as ARISP, 
                    safe_cast(SF as INT64) as SF, safe_cast(SH as INT64) as SH, 
                    safe_cast(DP as INT64) as DP, Bats, LG, Throws, safe_cast(RC_27 as FLOAT64) as RC_27, 
                    safe_cast(A as INT64) as A, safe_cast(Pos1 as INT64) as Pos1, Pos,  
                    safe_cast(D_G as INT64) as D_G , safe_cast( PO as INT64) as PO , 
                    safe_cast(CS as INT64) as CS, "dummy" as playerpk from ''' + staging_dataset + '.jBatters order by Year' 

update_jBat_sql1 = 'UPDATE ' + modeled_dataset + '''.jBat
                    SET playerpk = concat(cast(LName as string), cast(FName as string), cast(Team as string), cast(Year as string), cast(index as string))
                    WHERE FName is not null ''' 

update_jBat_sql2 = 'UPDATE ' + modeled_dataset + '''.jBat
                    SET playerpk = concat(cast(LName as string), cast(Team as string), cast(Year as string), cast(index as string))
                    WHERE FName is null ''' 

update_jBat_sql3 = 'UPDATE ' + modeled_dataset + '''.jBat
                    SET playerpk = concat(cast(FName as string), cast(Team as string), cast(Year as string), cast(index as string))
                    WHERE LName is null '''



with models.DAG(
        'sabermetrics_workflow',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    create_staging_dataset = BashOperator(
            task_id='create_staging_dataset',
            bash_command='bq --location=US mk --dataset ' + staging_dataset)
    
    create_modeled_dataset = BashOperator(
            task_id='create_modeled_dataset',
            bash_command='bq --location=US mk --dataset ' + modeled_dataset)
    
    load_npbb = BashOperator(
            task_id='load_npbb',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.jBatters \
                         "gs://sabermetrics_bucket_ajn873/raw/npbb.csv"',
            trigger_rule='one_success')
   
    split = DummyOperator(
            task_id='split',
            trigger_rule='all_done')

    split_jBat = DummyOperator(
            task_id='split_student',
            trigger_rule='all_done')
    
    create_jBat = BashOperator(
            task_id='create_jBat',
            bash_command=bq_query_start + "'" + create_jBat_sql + "'", 
            trigger_rule='one_success')
    
    update_jBat1 = BashOperator(
            task_id='update_jBat1',
            bash_command=bq_query_start + "'" + update_jBat_sql1 + "'", 
            trigger_rule='one_success')
    update_jBat2 = BashOperator(
            task_id='update_jBat2',
            bash_command=bq_query_start + "'" + update_jBat_sql2 + "'", 
            trigger_rule='one_success')
    update_jBat3 = BashOperator(
            task_id='update_jBat3',
            bash_command=bq_query_start + "'" + update_jBat_sql3 + "'", 
            trigger_rule='one_success')
    
    jBat_beam = BashOperator(
            task_id='jBat_beam',
            bash_command='python/home/jupyter/airflow/dags/jBat_single.py')
    
    #jBat_dataflow = BashOperator(
    #        task_id='jBat_dataflow',
    #        bash_command='python/home/jupyter/airflow/dags/jBat_cluster.py')
        
 
    
    create_staging_dataset >> create_modeled_dataset >> split
    split >> load_npbb >> create_jBat >> update_jBat1 >> update_jBat2 >> update_jBat3 >> split_jBat
    split_jBat >> jBat_beam
    
       