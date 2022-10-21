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

  # dataset = pq.ParquetDataset('partitioned_dataset_new/', filters=[['year', '=', 2021], ['month', '=', 4], ['day', '=', 21]])
  # df = dataset.read(
  #     columns=['sensor_id', 'parameter_uuid', 'value']
  # )

  # print(df)

  fp = pq.read_table(
    source='partitioned_dataset_new/',
    use_threads=True,
    columns=['sensor_id', 'parameter_uuid', 'value'],
    filters=[['year', '=', 2021], ['month', '=', 4], ['day', '=', 21]]
  )
  df = fp.to_pandas()

  print(df)
