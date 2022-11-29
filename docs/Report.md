# Report for the data engineering project

Matthieu ROUX

Ewen CHAILLAN

## Motivation

We wanted to use the data of the deaths of the french citizens from data-gouv. So with the data of the thermal and nuclear plants also from data-gouv
we thought of crossing them to discover if there is more deaths around power plants in France.


## 1. Data Ingestion

Our ingestion pulls data from 4 different data sets:

- A data set of deaths of french citizens
- A data set with the location of active nuclear plants in France
- A data set with the location of active thermal plants in France
- A data set crossing the crossing the INSEE code of a city with it gps coordinates

All those data set are available on the public gouvernement's site: https://www.data.gouv.fr/fr/datasets/

For the deaths data we had to first curl from the API a JSON document which is like a "library" listing all the files with deaths data, each one listing the deaths for a certain period of time.
Then we had to get their url from the JSON to curl the raw data (for all of them or the latests). We obtained txt file(s) containing our raw data.

For the other data sets it was simpler. In fact, we had to curl a JSON too from the API but it was only a document listing the different link to curl our data in csv, JSON or GEOJSON etc...
So we chose the curl url to get csv files and we stored them like that.

## 2. Data Staging

First of all we decided to make a simple version of the data staging. In fact, we only used pandas to extract our data from the csv files, then we transformed it and saved it again in different csv file.
This system as simple to implement and made us more aware of the different problem we might encounter. 

We then decided to implement persistency. That's why we decided to store our data in different databases built ont docker volumes. So for the different data for the power plants and the cities we did not change anything because the staging was quite simple.
However for the deaths data we used redis to store our data during the different phase of the wrangling.

Eventually we transformed our pipeline saving data in csv file in a pipeline creating different tables and storing data in a Postgres Database. The fact that we decided to use a relational database over NoSql databases is that the use that we want to make of our data need joins and relational operations.

## 3. Production phase


