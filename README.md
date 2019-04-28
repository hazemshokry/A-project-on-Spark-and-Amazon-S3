### A project at Udacity Data Engineering Nanodegree

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to.

Data source is divided by two main sources, songs meta-data and logs of what users are listening (user activity) which they are currently stored as JSON files.

#### Let's see an example for both sources:
##### Songs meta-data:
![Songs meta-data!](/images/songs.png "Songs meta-data")

##### Logs:
![Logs!](/images/logs.png "Logs")


The company goal is to query this data sets and find some analytics and insights. One example is to counts songs that are played this month for a specific artist.

## Solution
![Architecture!](/images/arch.png "Architecture")


##### Step1: Design  star schema 
![Entity Relationship Diagram!](/images/diagram.png "Entity Relationship Diagram")

##### Step2: Loading data from S3 Bucket into Spark

##### Step3: Processing data using Spark to create star schema designed tables to be ready for analytics.

##### Step4: Find some insights about the result data.
In the dashboard.ipynb we can find some visualization and statistcs about users who used the app.

Example: Top users who used the app after getting their names from users table
![top_users!](/images/top_users2.png "Top Users")

#### Project Files:
1. complete_etl.ipynb: a jupyter notebook that do the full etl (Creating tables and inserting data). You can run them row by row to fully understand the full process.
2. etl.py: The actual python script file which will process all Json files and datasets from S3 bucket.
3. dwh.cfg: A config file that contains all your S3 buckets AWS credentials..

#### How to run the project:
1. Put the requiered credentials (in the same order) in dwh.cfg file.
2. run etl.py

