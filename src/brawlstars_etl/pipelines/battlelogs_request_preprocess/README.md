# Pipeline battlelogs_request_preprocess

> *Note:* This is a `README.md` boilerplate generated using `Kedro 0.18.4`.

# Overview
The pipeline has two nodes: `battlelogs_request` and `battlelogs_filter`. The 
`battlelogs_request` node extracts battlelogs data from the Brawlstars API 
asynchronously by executing futures objects. 

The function requests data for the first `n` players, where `n` is a parameter that 
can be defined by the user. If no `n` is  provided, the player tags list is split 
into batches of 10 and requests are sent for each batch. The extracted data is 
concatenated into a structured Pandas DataFrame.

The column names are normalized by replacing dots with underscores. The 
`battlelogs_filter` node filters the data by selecting specific columns and sorting 
& filtering the data by player, timestamp and cohort.
