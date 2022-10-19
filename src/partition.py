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
    query = "select * from acqdat_sensors_data limit 100000"

    cursor.execute(query)
    records = cursor.fetchall()

    result = []
    for row in records:
        flat_array = []
        for key in row[4]:
            flat_array = [ row[1], row[2], row[3], key['uuid'], row[0].replace(tzinfo=None), float(key['value']), row[5].replace(tzinfo=None)]
        result.append(flat_array)

    dataframe = pd.DataFrame(result, columns=['org_id', 'project_id', 'sensor_id', 'parameter_uuid', 'inserted_timestamp', 'value', 'inserted_at'])
    # print(dataframe)

    sensor_table = pa.Table.from_pandas(dataframe)

    # Writing into single paruqt file 
    pq.write_table(sensor_table, 'data/sensor_data.parquet', version='1.0')

    # Reading from same parquet file 
    table = pq.read_table("data/sensor_data.parquet")

    # Partitioning Parquet files based on Project ID
    ds.write_dataset(sensor_table, "./partitioned", format="parquet",
                 partitioning=ds.partitioning(pa.schema([("project_id", pa.int16())])))

    # Reading Partitioned Data - All together
    dataset = ds.dataset("./partitioned", format="parquet")
    table = dataset.to_table()
