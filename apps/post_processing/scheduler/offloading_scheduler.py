from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.jobstores.redis import RedisJobStore
from pytz import utc
from datetime import datetime, timedelta
from django.db import connection
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from crate import client
import environ
from decimal import Decimal

scheduled_jobs_map = {}

env = environ.Env()
environ.Env.read_env()

executors = {
  'default': ThreadPoolExecutor(20),
  'processpool': ProcessPoolExecutor(5)
}
job_defaults = {
  'max_instances': 3
}
jobstores = {
  'default': RedisJobStore(jobs_key='offloading_scheduler.jobs', run_times_key='offloading_scheduler.running_jobs', host=env('REDIS_HOST'), port=env('REDIS_PORT'))
}

scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults, timezone=utc)

def schedule_jobs():
  jobs = retrieve_jobs_to_schedule()
  for job in jobs: 
    add_job_if_applicable(job, scheduler)
    update_job_if_applicable(job, scheduler)
  print(datetime.utcnow())
  print("refreshed scheduled jobs")


def retrieve_jobs_to_schedule():
  with connection.cursor() as cursor:
    query = "select uuid, source_table, target_table, current_execution_time, next_schedule_time, schedule_interval, is_periodic, timestamp_col, fields, version from offloading_trackers where next_schedule_time IS NOT NULL and status = 1"
    cursor.execute(query)
    records = cursor.fetchall()
  
  return records 

def add_job_if_applicable(job, scheduler):
  print("inside add_job_if_applicable") 
  
  job_id = str(job[0])
  
  if (job_id not in scheduled_jobs_map):
    scheduled_jobs_map[job_id] = list(job)
    next_iteration = job[4]
    
    print(next_iteration)
    scheduler.add_job(execute_job, 'date', args=[job], run_date=next_iteration, id=job_id)
    
    print("added job with id: " + str(job_id))

def update_job_if_applicable(job, scheduler):
  print("inside update_job_if_applicable")
  
  job_id = str(job[0])
  
  if (job_id not in scheduled_jobs_map):
    return

  next_iteration = job[4]
  last_version = scheduled_jobs_map[job_id][9]
  current_version = job[9]

  if (current_version != last_version):
    #print(scheduler.get_jobs())
    scheduled_jobs_map[job_id][9] = current_version

    if scheduler.get_job(job_id):
      scheduler.remove_job(job_id)
    
    scheduler.add_job(execute_job, 'date', args=[job], run_date=next_iteration, id=job_id)
    
    print("updated job with id: " + str(job_id))

def execute_job(job):
  print("executing job with id: " + str(job[0]))
  write_dataset(job)
  print(datetime.utcnow())

def listener(event):
  if event.exception:
    print('The job crashed :(')
  else:
    print('The job worked :)')

def start():
    #scheduler.listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
  scheduler.add_job(schedule_jobs, trigger='interval', seconds=5, next_run_time=datetime.utcnow(), id='scheduler-job-id')
  scheduler.start()

def write_dataset(job):
  dataset_name = job[2]
  is_periodic = job[6]
  current_execution_time = job[3]
  next_schedule_time = job[4]
  timestamp_col = job[7]
  print("existing version" + str(job[9]))
  new_version = (job[9] + Decimal(0.1))
  print("new_version version" + str(new_version))

  url = "{}/_sql".format(env('CRATE_DB_URL'))

  con = client.connect(url)
  cursor = con.cursor()


  if is_periodic:
    if not current_execution_time:
      conditional = "where {} <= '{}'".format(timestamp_col, next_schedule_time)
    else:
      conditional = "where {} > '{}' and {} <= '{}'".format(timestamp_col, current_execution_time, timestamp_col, next_schedule_time)
    current_execution_time = next_schedule_time
    # next_schedule_time = next_schedule_time + timedelta(hours = 1)
    next_schedule_time = next_schedule_time + timedelta(minutes = 10)
  else:
    conditional = "where {} <= '{}'".format(timestamp_col, next_schedule_time)
    current_execution_time = next_schedule_time
    next_schedule_time = None

  
  print("current_execution_time")
  print(current_execution_time)
  print("next_schedule_time")
  print(next_schedule_time)

  extract_columns = job[8]

  if timestamp_col in extract_columns:
    extract_columns.remove(timestamp_col)

  headers = [timestamp_col, 'year', 'month', 'day'] + extract_columns

  print("headers")
  print(headers)

  extract_columns = (','.join(extract_columns))

  query = "select {}/1000, EXTRACT(MONTH FROM DATE_TRUNC('month', {})), EXTRACT(YEAR FROM DATE_TRUNC('year', {})), EXTRACT(DAY FROM DATE_TRUNC('day', {})), {} from sensors_data {} limit 20".format(timestamp_col, timestamp_col, timestamp_col, timestamp_col, extract_columns, conditional)

  print(query)
  cursor.execute(query)
  result = cursor.fetchall()

  dataframe = pd.DataFrame(result, columns=headers)

  sensor_table = pa.Table.from_pandas(dataframe)
  pq.write_to_dataset(sensor_table, root_path=dataset_name, partition_cols=(['year', 'month', 'day']))

  if next_schedule_time:
    update_qry =  "UPDATE offloading_trackers SET current_execution_time = '{}', next_schedule_time = '{}', version = {} WHERE uuid = '{}';".format(current_execution_time, next_schedule_time, new_version, job[0])
  else:
    update_qry = "UPDATE offloading_trackers SET current_execution_time = '{}', version = {}, next_schedule_time = NULL WHERE uuid = '{}';".format(current_execution_time, new_version, job[0])

  print("update_query")
  print(update_qry)

  with connection.cursor() as cursor:
    cursor.execute(update_qry)
    #print(cursor.rowcount)
