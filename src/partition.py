from dataclasses import dataclass
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import s3fs
import os

s3 = s3fs.S3FileSystem(key=os.environ['S3_ACCESS_KEY'], secret=os.environ['S3_SECRET_KEY'])


from db_reader import create_connection

sensor_schema = pa.schema([
    ('org_id', pa.int32()),
    ('project_id', pa.int32()),
    ('sensor_id', pa.int32()),
    ('parameter_uuid', pa.string()),
    ('inserted_timestamp', pa.timestamp('ms')),
    ('value', pa.float32()),
    ('inserted_at', pa.timestamp('ms'))
])


if __name__ == '__main__':
    connection = create_connection("acqdat_core_dev", "postgres", "postgres", "188.166.230.56", "5431")

    cursor = connection.cursor()

    query = "select inserted_timestamp, org_id, project_id, sensor_id, parameters, inserted_at, EXTRACT(YEAR FROM DATE_TRUNC('year', inserted_timestamp)), EXTRACT(MONTH FROM DATE_TRUNC('month', inserted_timestamp)), EXTRACT(DAY FROM DATE_TRUNC('day', inserted_timestamp)) from acqdat_sensors_data limit 200000 offset 100000"

    cursor.execute(query)
    records = cursor.fetchall()

    result = []
    for row in records:
        flat_array = []
        for key in row[4]:
            flat_array = [ row[1], row[2], row[3], key['uuid'], row[0].replace(tzinfo=None), key['value'], row[5].replace(tzinfo=None), row[6], row[7], row[8]]
        result.append(flat_array)

    dataframe = pd.DataFrame(result, columns=['org_id', 'project_id', 'sensor_id', 'parameter_uuid', 'inserted_timestamp', 'value', 'inserted_at', 'year', 'month', 'day'])

    sensor_table = pa.Table.from_pandas(dataframe)

    pq.write_to_dataset(sensor_table, root_path='s3://postproc/partitioned_dataset_new', filesystem=s3, partition_cols=(['year', 'month', 'day']))

