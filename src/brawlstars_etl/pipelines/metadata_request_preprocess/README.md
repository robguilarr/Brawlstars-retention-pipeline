# Pipeline - Metadata Request Preprocess

> *Note:* This is a `README.md` boilerplate generated using `Kedro 0.18.4`.

# Overview

This pipeline includes two nodes: `players_info_request` and `metadata_preparation`. The 
first node extracts metadata from the Brawlstars API for a list of players with 
given tags.

It retrieves the metadata asynchronously using a list of futures objects that 
consist of async threads.

The second node preprocesses the metadata into a format suitable for analysis with 
Spark, adapting its format to match a given DDL schema and removing unnecessary columns.
