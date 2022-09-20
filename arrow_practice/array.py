import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq


days = pa.array([1,12,17,23,28], type = pa.int8())
months = pa.array([1,3,5,7,1], type = pa.int8)

years = pa.array([1990, 2000, 1995, 1995], type = pa.int16())
