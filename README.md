# weixin_bulkload
This is a full project for uploading big data on HBase.
The data consists of about 3 billion heartbeat records through WeChat, all of which comes to be a structured datum, waits for a bulkload. 
## Overview
* Create presplitted table: Given number of regions, start key and end key to save HBase from fierce splitting regions afterwards.
* Create HFile by MapReduce, which contains my own business logic.
* Load Hfile into HBase, which can also be done by cmd.

## RowKey processing
First of the time I only use timestamp and user fields to compose a rk.
Afterwards I added 0 at lac and cellid's head to apend a fixed-length key after raw rk, which also avoids rk repetition. 
Finally I keep the raw length with `|` seprating each field, and salt it according to the number of regions, for a write balance consideration.

## Map-side join
In this project I need to select the data, whose latitude and longitude included in a lookup file, so a map-side join seems good for me.
A `setup` method is overriden under Map class, responsible of initializing my lookup file from HDFS. If you want to keep the data for the use in 
map or reduce, setup using **distributed cache** is a good idea.

## Map & Reduce
In fact we only need to override the map one, formatted like `Mapper<LongWritable, Text, ImmutableBytesWritable, Put>`.
After map producing the qualfied HFile, we can specify the reducer using `PutSortReducer.class` or `KeyValueSortReducer.class` to sort the put results.
