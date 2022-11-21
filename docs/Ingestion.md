# Ingestion

Our ingestion pulls data from 3 different data sets: 
- A data set of deaths of french citizens
- Location of active nuclear plants
- Location of active thermal plants

## Plant location data

This one was relatively simple, we firs pulled the dataset information as a json file in the database provider (data.gouv.fr), then we extracted the resource in the format we needed (csv) using a python operator.

## Death data

The data for deaths was more complicated, as new files are added every now and then, so we must pull every file from the dataset. This is why we first pull the data set information and store all its resources in a json file. Another python operator then parses the resources and downloads them. 
We decided to put a limit to the amount of files that can be pulled at once to make our testing go faster, but in a real world scenario it would be wise to pull everything.

## Keeping track of downloaded data

This has yet to be done, for now, all data will be ingested, but it would be wise to see what data we already have, to save time and network resources