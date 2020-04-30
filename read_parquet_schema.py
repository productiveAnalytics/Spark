#!/usr/bin/env python

###
### Author : LChawathe
###

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as paq
t = paq.read_table('test-data/flight_2020-04-27_part_0.parquet')
df = t.to_pandas()
print(pa.Schema.from_pandas(df))
print(df)