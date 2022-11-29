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

### 1.2 Death data ingestion

The death data is deposited in an unconventional format: every 2 months or so, a txt file with all new deaths of french citizens is uploaded. This means that if we wanted to get every death, we would have to query multiple files.
The list of all death files can be obtained by querying the information of the death dataset. This list was then stored in a JSON document. The JSON format was chosen because it is the same format as the API's response.
Then with the json file we can extract the URLs for every death file and pull them.

Each death file contains about 50k deaths so we decided to limit the amount of files downloaded to 2 by default. However, if you desire to turn your computer into a heater, you can increase this limit to your heart's desire.

We noticde that a nice improvement that we could implement with more time, is a tool that lets us select a range of time, in order to ingest all deaths within a period of time.

### 1.3 Death data ingestion

For the other data sets it was simpler. We curled the same JSON from the API and pulled the latest dataset available in csv format.

We chose to save the data in CSV because it is easy to import in pandas for wrangling, and is easy to read in vscode.

## 2. Data Staging

Our staging pipeline represents the main part of our pipeline. It is supposed to clean the data and store it into a nice SQL database, so that it can be used by the production overlords.

### 2.1 Development process
First of all we decided to make a simple version of the data staging. In fact, we only used pandas to extract our data from the csv files, then we transformed it and saved it again in different csv file.
This system as simple to implement and made us more aware of the different problem we might encounter. 

We then decided to implement persistency. That's why we decided to store our data in postgres qnd redis built on docker volumes. 

Eventually we transformed our pipeline saving data in csv file in a pipeline creating different tables and storing data in a Postgres Database. The fact that we decided to use a relational database over NoSql databases is that the use that we want to make of our data need joins and relational operations. SQL databases are also adapted to the usage we had in mind for production.

### 2.2 Data cleaning and wrangling

The nuclear and thermal plant data was already very clean so only a minimum amount of cleaning was done:
- Removal of duplicate
- Dropping columns deemed unnecessary
- Merging nuclear and thermal plants

 The entire db was then stored in one sql table called `power_plants`


The death data was more of a challenge, we had to go through the docmentation to see where was what. Below you can find an example of what one row of data looks like:
```txt
THERON*JEANNINE EMILIENNE MARIA/                                                21925040451582TRIGNY                                                      202208040100719                             

```
_It's not really obvious here that death date and location are stored in the last group of digits in the row_

This also means we had to blindly believe that the data was perfect, if it wasn't we would just drop it.
 We removed names because they brought nothing of value to us and increased the privacy of the deceased.

 The second part of wrangling was pairing the insee code of the death to a real location with a longitude and a latitude. This was done by getting the coordinates of insee codes given by the governement, and mapping them to the ones found in the deaths. Missing codes would lead to the death cata being dropped.

 The entire db was then stored in one sql table called `deaths`
### 2.2 Architectural choices
- We decided to drop all invalid death entries, because we already have enough. Plus, we found out most invalid deaths are linked to a death in a foreign country, which is out of the scope of this project (we only care about deaths in france since it is where our power plants are).
- Deaths are never updated. You can only die once, and we assumed that mistakes on deaths do not get updated frequently. A few deaths that did not happen on the proper date or in the proper place won't have a big impact on our study we believe.
- Unlike deaths, we update all the power plants every time. This is mainly because there aren't that many, and who knows, the latest data might update some values, such as the power it generates, or the name.
- Dirty data is cached in redis, clean data in postgres, we could have used mongo to store the data, but redis fullfilled all our needs, while remaining lighter.
- Some data joins were made in python, they could have been done in SQL, but we found it easier to do it in python, and it is not a big deal since we are not dealing with huge amounts of data. If we had more time, we would have done it in SQL.
- Our production data is stored in postgres, because most of our data is relational.



## 3. Production phase

### 3.1 Features and usage
### 3.2 Using Neo4J
The id of displaying the data on Neo4J was pitched to us during the presentation. But after the loss of one weekend of progress, and difficulties conneting to neo4j, the idea was dropped and we decided to focus on the jupyter notebook.
## 4. Difficulties

### 4.1 Data colletion difficulties:
- The data was easily available so it was the easiest part of the project, lucky us!

### 4.2 Config difficulties:
- Connecting postgres to airflow took us some time, and we werre not able to configure the connection without asking people to do it though the UI
- We also had issues connecting our project to Neo4J leading us to drop the Neo4J features we had in mind for production

### 4.3 Hardware realted difficulties:

- Ewen broke his computer so he had to access it remotely. Sometimes the computer would turn off, and it would be quite hard to turn it back on when ewen was in lyon (the computer stays in paris). This would sometimes lead to a lot of progress lost when he would not push.
- Matthieu's laptop struggles with docker, sometimes the pipeline would take 10 minutes to run, making debugging painfully slow


