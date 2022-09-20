import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from db_reader import create_connection

sensor_schema = pa.schema([
    ('inserted_timestamp', pa.timestamp('s',  tz='UTC')),
    ('org_id', pa.int32()),
    ('project_id', pa.int32()),
    ('sensor_id', pa.int32()),
    ('parameter_uuid', pa.string()),
    ('value', pa.float32()),
    ('inserted_at', pa.timestamp('s',  tz='UTC'))
])


if __name__ == '__main__':
    connection = create_connection("acqdat_core_dev", "postgres", "postgres", "188.166.230.56", "5431")

    cursor = connection.cursor()
    query = "select * from acqdat_sensors_data limit 10"

    cursor.execute(query)
    records = cursor.fetchall()
    dataframe = pd.DataFrame(records)
    print(dataframe)
