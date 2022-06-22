# Analyze market data : stocks and more

There are several tools to analyze financial data, visualize data and develop strategies using different portfolios based on analysis results. 
## Structure

* **index/**. Analyzing scripts (main).
* **functions/**. Analyzing auxiliary functions.
* **plots/**. Plotting functions.
* **utils/**. Other functions (database, superset etc)
* **reports/**. Plots and reports.
* **data/**. Data files in csv or json.
* **config/**. Configuration files.
  * **config.properties**. External API's URL's and crendentials.
  * **config.yaml**. All the configuration for the application.
* **logs/**. Log files and logs configuration.
* **assets/**.  SQL, bash, and docker scripts, and saved models.
  * **models/**. Saved models
  * **scripts/**. Bash scripts and SQL scripts.
  * **superset/**. Apache Superset  dashboards backup and Apache Superset configuration files.

## Deployment

Properties file called **config.properties** must edited to add your credendials for the external API's , as shown in **example_config.properties**.
<br>
###Run docker. 
As me you can use a external disk to store your data because it can grow until hundreds of GB.
```
DISKNAME=<your disk name>
DIRNAME=<folder to store the database in disk name>
PASSWORD= <password to use as root in the database>
cd assets/scripts/docker
./run_container.sh
```
###Create databases:
```
cd assets/scripts/sql_tables
./init.sh
```
Change the database user and password used by the application in  **config/config.yaml**.

### Superset:

You can follow the steps to install Apache Superset using docker in the official web page: https://superset.apache.org/docs/installation/installing-superset-using-docker-compose.

### Requirements

```
pip install -r requirements.txt
```
## Updating data 
Data is updated from external API's using  scripts in **utils/get_new_data**. You can configure how to run that scrips and with hich frequency (daily or lower).
An example if given in **get_all_data.sh** using a bash script.
```
cd utils/get_new_data 
./get_all_data.sh 
```
## Features ⚙️
 
* ARIMA, prophet and VAR models to predict prizes, macroeconomic info and fundamental results.
* Visualizations using Apache Superset.
* Logs.
* Data updated daily from external API's.
* Database handling with huge amounts of stocks prizes, forex, macroeconomic data from and stocks fundamental.
* Portfolio and strategies creation based in analysis results.
## Build with 🛠️

* [Python]
* [Docker]
* [Apache Superser]



## Autors ✒️
Manuel Paz Pintor



---
⌨️ (https://github.com/ManuPaz) 😊
