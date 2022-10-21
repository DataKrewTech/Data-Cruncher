from dataclasses import dataclass
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds


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

    #print(records)

    result = []
    for row in records:
        flat_array = []
        for key in row[4]:
            flat_array = [ row[1], row[2], row[3], key['uuid'], row[0].replace(tzinfo=None), key['value'], row[5].replace(tzinfo=None), row[6], row[7], row[8]]
        result.append(flat_array)

    dataframe = pd.DataFrame(result, columns=['org_id', 'project_id', 'sensor_id', 'parameter_uuid', 'inserted_timestamp', 'value', 'inserted_at', 'year', 'month', 'day'])
    #print(dataframe)

    sensor_table = pa.Table.from_pandas(dataframe)

    # Writing into single paruqt file 
    # pq.write_table(sensor_table, 'data/sensor_data.parquet', version='1.0')

    # Partitioning Parquet files based on Project ID
    # ds.write_dataset(sensor_table, "./partitioned_dataset", format="parquet",
    #              partitioning=ds.partitioning(pa.schema([("year", pa.int16()), ("month", pa.int16()), ("day", pa.int16())])))

    pq.write_to_dataset(sensor_table, root_path='partitioned_dataset_new', partition_cols=(['year', 'month', 'day']))
