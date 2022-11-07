from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import utc
from datetime import datetime, timedelta
import json
from django.db import connection
from dataclasses import dataclass
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from crate import client
import environ

scheduled_jobs_map = {}

def schedule_jobs(scheduler):
    jobs = retrieve_jobs_to_schedule()
    for job in jobs: 
        add_job_if_applicable(job, scheduler)
        update_job_if_applicable(job, scheduler)
    print(datetime.utcnow())
    print("refreshed scheduled jobs")

def retrieve_jobs_to_schedule():
    # with connection.cursor() as cursor:
    #     cursor.execute("select * from offloading_trackers")
    #     records = cursor.fetchall()
    #     print(records)

    #print(records)
    records = []

    #[(1, 1, 'sensors_data', 'dummy', 'my test', datetime.datetime(2022, 10, 28, 14, 7, 10, tzinfo=datetime.timezone.utc), datetime.datetime(2022, 10, 28, 14, 17, 10, tzinfo=datetime.timezone.utc), False, 'every hour at 10 mins', datetime.datetime(2022, 10, 28, 14, 7, 10, tzinfo=datetime.timezone.utc), datetime.datetime(2022, 10, 28, 14, 7, 10, tzinfo=datetime.timezone.utc))]

    result = []
    for row in records:
        #0 15 10 * * ? 2005  Fire at 10:15 AM every day during the year 2005
        #.format('a', 'b', 'c')
        cron_expression = '0 {} {} {} {} ? {}'.format(row[6].minute, row[6].hour, row[6].day, row[6].month, row[6].year)
        #'0 17 14 28 10 ? 2022'

        #cron_expression = '0 46 15 28 10 ? 2022'
        #print(cron_expression)

        #'cron_expression': '0 {} {} {} {} ? {}'.format(row[6].minute, row[6].hour, row[6].day, row[6].month, row[6].year)
        next_iteration = datetime.utcnow() + timedelta(minutes = 5)
        #flat_array = {'id': row[0], 'current_datetime': row[5], 'next_iteration': row[6], 'cron_expression': cron_expression}
        flat_array = {'version': 1, 'id': row[0], 'current_datetime': row[5], 'next_iteration': next_iteration, 'cron_expression': cron_expression}
        # print(row[6].hour)
        # print(row[6].minute, )
        result.append(flat_array)
    
    #print(result)
    result = [{'target_table': "dummy1", 'version': 1, 'id': 1, 'current_datetime': datetime.utcnow(), 'next_iteration': datetime.utcnow() + timedelta(seconds = 10), 'cron_expression': 'fsd'},
            {'target_table': "dummy2", 'version': 1, 'id': 2, 'current_datetime': datetime.utcnow(), 'next_iteration': datetime.utcnow() + timedelta(minutes = 5), 'cron_expression': 'fsd'},
            {'target_table': "dummy3", 'version': 1, 'id': 3, 'current_datetime': datetime.utcnow(), 'next_iteration': datetime.utcnow() + timedelta(minutes = 7), 'cron_expression': 'fsd'}]
    d = result
    # with open('/Users/bandanapandey/work/data_crew/Data-Cruncher/post_processing/post_processing/jobs.json') as f:
    #     d = json.load(f)
    #print(d)
    return d    

def add_job_if_applicable(job, scheduler): 
    job_id = str(job['id'])
    if (job_id not in scheduled_jobs_map):
        scheduled_jobs_map[job_id] = job
        # datetime = datetime.utcnow()
        # print(datetime)
        print("current time")
        print(datetime.utcnow())
        print("next_iteration")
        datetime1 = job['next_iteration']
        print(datetime1)
        scheduler.add_job(lambda: execute_job(job), 'date', run_date=datetime1, id=job_id)
        #scheduler.add_job(lambda: execute_job(job), CronTrigger.from_crontab(job['cron_expression'], timezone='UTC'), id=job_id)
        
        print("added job with id: " + str(job_id))

def update_job_if_applicable(job, scheduler):
    job_id = str(job['id'])
    if (job_id not in scheduled_jobs_map):
        return

    last_version = scheduled_jobs_map[job_id]['version']
    current_version = job['version']
    if (current_version != last_version):
        scheduled_jobs_map[job_id]['version'] = current_version
        scheduler.remove_job(job_id)
        scheduler.add_job(lambda: execute_job(job), 'date', run_date=datetime1, id=job_id)
        #scheduler.add_job(lambda: execute_job(job), CronTrigger.from_crontab(job['cron_expression'], timezone='UTC'), id=job_id)
        print("updated job with id: " + str(job_id))

def execute_job(job):
    print("executing job with id: " + str(job['id']))
    write_dataset(job['target_table'], job['current_datetime'], job['next_iteration'])
    print(datetime.utcnow())

def start():
    scheduler = BackgroundScheduler(timezone=utc)
    scheduler.add_job(lambda: schedule_jobs(scheduler), 'interval', seconds=5, next_run_time=datetime.utcnow(), id='scheduler-job-id')
    scheduler.start()
#input()

def write_dataset(dataset_name, curr_exec_time, next_iteration_time):
    # with connection.cursor() as cursor:
    #     cursor.execute("select inserted_timestamp, org_id, project_id, sensor_id, parameters, inserted_at, EXTRACT(YEAR FROM DATE_TRUNC('year', inserted_timestamp)), EXTRACT(MONTH FROM DATE_TRUNC('month', inserted_timestamp)), EXTRACT(DAY FROM DATE_TRUNC('day', inserted_timestamp)) from acqdat_sensors_data limit 2 offset 0")
    #     records = cursor.fetchall()
    # print(records)

    env = environ.Env()
    environ.Env.read_env()

    url = "{}/_sql".format(env('CRATE_DB_URL'))

    con = client.connect(url)
    cursor = con.cursor()
    if not curr_exec_time:
        conditional = "where inserted_timestamp <= '{}'".format(next_iteration_time)
    else:
        conditional = "where inserted_timestamp > '{}' and inserted_timestamp <= '{}'".format(curr_exec_time, next_iteration_time)

    query = "select data, data_type, inserted_at, inserted_timestamp, org_id, param_name, param_uuid, project_id, sensor_id, EXTRACT(MONTH FROM DATE_TRUNC('month', inserted_timestamp)), EXTRACT(YEAR FROM DATE_TRUNC('year', inserted_timestamp)), EXTRACT(DAY FROM DATE_TRUNC('day', inserted_timestamp)) from sensors_data {}".format(conditional)
    print(query)
    cursor.execute(query)
    records = cursor.fetchall()
    
    print(records)

    result = []
    for row in records:
        flat_array = [row[4], row[7], row[8], row[6], datetime.fromtimestamp(row[3]/1000), row[0], row[1], datetime.fromtimestamp(row[2]/1000), row[10], row[9], row[11]]
        result.append(flat_array)

    print(result)
    dataframe = pd.DataFrame(result, columns=['org_id', 'project_id', 'sensor_id', 'parameter_uuid', 'inserted_timestamp', 'value', 'data_type', 'inserted_at', 'year', 'month', 'day'])

    sensor_table = pa.Table.from_pandas(dataframe)
    pq.write_to_dataset(sensor_table, root_path=dataset_name, partition_cols=(['year', 'month', 'day']))
