# Financial-Analysis-Using-COVID-Data
CMPT 726 Big Data Project

## Step 1
Create the schema for all the tables in cassandra.<br/>
We will be using the following tables for our project.

Create keyspace for the project<br/>
**CREATE KEYSPACE dataflix WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };**

Create the following tables
Covid19 data related tables:

**CREATE TABLE dataflix.covid19_cases_us (<br/>
    date date,<br/>
    city text,<br/>
    province text,<br/>
    new_cases_us int,<br/>
    total_cases_us int,<br/>
    PRIMARY KEY (date, city, province)<br/>
);<br/>**

**CREATE TABLE dataflix.covid19_deaths_us (<br/>
    date date,<br/>
    city text,<br/>
    province text,<br/>
    new_deaths_us int,<br/>
    total_deaths_us int,<br/>
    PRIMARY KEY (date, city, province)<br/>
);<br/>**

**CREATE TABLE dataflix.covid19_cases_global (<br/>
    date date,<br/>
    country text,<br/>
    new_cases_global int,<br/>
    total_cases_global int,<br/>
    PRIMARY KEY (date, country)<br/>
);<br/>**

**CREATE TABLE dataflix.covid19_deaths_global (<br/>
    date date,<br/>
    country text,<br/>
    new_deaths_global int,<br/>
    total_deaths_global int,<br/>
    PRIMARY KEY (date, country)<br/>
);<br/>**

**CREATE TABLE dataflix.covid19_recovered_cases_global (<br/>
    date date,<br/>
    country text,<br/>
    new_recovered_global int,<br/>
    total_recovered_global int,<br/>
    PRIMARY KEY (date, country)<br/>
);<br/>**

Stock market Related table:

**CREATE TABLE dataflix.stocks (<br/>
    stock_type text,<br/>
    date date,<br/>
    close float,<br/>
    high float,<br/>
    low float,<br/>
    open float,<br/>
    volume float,<br/>
    PRIMARY KEY (stock_type, date)<br/>
);<br/>**

Cryptocurrencies market related table:

**CREATE TABLE dataflix.crypto (<br/>
    cryptotype text,<br/>
    date date,<br/>
    close float,<br/>
    high float,<br/>
    low float,<br/>
    open float,<br/>
    volume float,<br/>
    PRIMARY KEY (cryptotype, date)<br/>
);<br/>**

Foreign Exchange Market realted table:

**CREATE TABLE dataflix.forex (<br/>
    forex_type text,<br/>
    date date,<br/>
    close float,<br/>
    high float,<br/>
    low float,<br/>
    open float,<br/>
    volume float,<br/>
    PRIMARY KEY (forex_type, date)<br/>
)**<br/>

Commodities market realted table:

**CREATE TABLE dataflix.commodities (<br/>
    type text,<br/>
    market text,<br/>
    date date,<br/>
    close float,<br/>
    high float,<br/>
    low float,<br/>
    open float,<br/>
    volume float,<br/>
    PRIMARY KEY (type, market, date)<br/>
)**<br/>

## Step 2
We can execute the dataflix.sh file to run the entire project. Command to execute dataflix.sh file:<br/>
**sh dataflix.sh** <br/>

It does the following things: <br/>
1. It runs all the spark jobs and saves the data in the cassandra db. All the files like crypto.py, stocks.py, commodities.py and forex.py will perform etl and save all the respective data in cassandra tables created as there is a for loop inside the script which will run all the spark jobs and save the data for the respective datasets into the cassandra Db with dataflix as keyspace and crypto, stocks, commodities and forex tables. <br/>
2. The second part of the script will train the model by fetching the data from the cassandra db, after which we perform further etl and aggregate functions as per the requirements of the ml models. After the etl and aggregate operations are performed, the cleaned data is saved and used for visualization using Tableau. After training and validating the model, the model is saved in `model_train` folder for predictions.
3. The report and power point presentation for visualizing the results can be found here:
 [Report PDF](https://github.com/aadityapanchal/Financial-Analysis-Using-COVID-Data/blob/main/Final%20Report.pdf)
ppt:


## The project uses following directories

The source directory has the following files and folders:<br/>

**dataflix.sh**

**datasets**<br/>
This directory has the datasets for covid19 data, stocks, forex, commodities and crypto data in their respective directories.<br/>

**insert_to_cassandra**<br/>
This has individual python files like covid.py(to save covid data), crypto.py(to save crypto data), etc to run spark jobs that saves data into cassandra db.<br/>

**load_data**<br/>
This directory has files to load the data from cassandra tables. After loading, etl and aggregate operations are performed and then the dataframe is saved as csv file for visualization using Tableau.<br/> 

**model_train**<br/>
This directory has the python model that trains and saves the machine learning models and used the load_data directory to get the data for training and validating the model.

After running the shell script file, 2 new folders named, "cleaned_data"  that contains the data after ETL and aggregate operations and "model_train" which has all the machine learning_models.
