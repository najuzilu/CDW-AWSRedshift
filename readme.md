# Building a Cloud Data Warehouse with AWS Redshift

In this project, we will move Sparkify's processes and data onto the cloud. Specifically, we will build ETL pipelines that extract data from S3 and stage them in Redshift, while transforming the data into a set of dimensional tables to allow Sparkify's analytical team to explore user song preferences and find insights.

### Data Overview

We will work with two datasets that reside in S3. Here are the S3 links for each dataset:
* Song data: `s3://udacity-dend/song_data`
* Log data: `s3://udacity-dend/log_data`
    * Log data JSON path: `s3://udacity-dend/log_json_path.json`.

The song dataset contains a subset of the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID like so:

```text
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

This is what the content of each JSON file looks like:
```json
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

The log dataset contains simulated app activity logs from a music streaming app based on configuration settings. The log files are partitioned by year and month, like so:

```text
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

Here is an example of what the data in the log file looks like.
![2018-11-12-events](./2018-11-12-events.png)

### Table Schema Design

To minimize moving back and forth between nodes of a clusters and optimize querying, we will use an `ALL` distribution style for dimension tables which are generally not large, such as `songs` and `artists` tables. The fact table schema is too big to be distributed using an `ALL` diststyle; therefore we will use a `KEY` distribution style which will allow all rows having similar `user_id` values to be placed in the same slice.

A useful feature to help optimize querying are sorting keys. Upon loading the data into respective tables, rows are sorted before distribution to slices. Sorting keys minimize querying time since each node already Has contiguous ranges of rows based on the sorting key.


### Staging Tables

We will create two staging tables where we will temporarily store input data from S3 which will then be used to populate the fact and dimension tables. The staging tables have the following schema:

**Table**: `staging_events` -- records from S3 events log
| field name  | field type | field attribute(s) |
| ----------- | ---------- | ------------------ |
| artist | VARCHAR | |
| auth | VARCHAR | |
| firstName | VARCHAR | |
| gender | CHAR | |
| itemInSession | INT |  |
| lastName | VARCHAR | |
| length | FLOAT | |
| level | VARCHAR | |
| location | VARCHAR | |
| method | VARCHAR | |
| page | VARCHAR | |
| registration | TIMESTAMP | |
| sessionId | INT |  |
| song | VARCHAR | |
| status | INT | |
| ts | TIMESTAMP | |
| userAgent | VARCHAR | |
| userId | INT | |

**Table**: `staging_songs` -- records from S3 song log
| field name  | field type | field attribute(s) |
| ----------- | ---------- | ------------------ |
| num_songs | INT | |
| artist_id | VARCHAR | |
| artist_latitude | FLOAT | |
| artist_longitude | FLOAT | |
| artist_location | VARCHAR | |
| artist_name | VARCHAR | |
| song_id | VARCHAR | |
| title | VARCHAR | |
| duration | FLOAT | |
| year | INT | |

#### Fact Table

We'll use the song and log datasets to create a _star_ schema. The _fact table_ which we will name `songplays` will record log data associated with event data such as song plays and will include the following field names, types, and attributes:

**Table**: `songplays` -- records in event data associated with song plays
| field name  | field type | field attribute(s) |
| ----------- | ---------- | ------------------ |
| songplay_id | INT  | IDENTITY(0,1) PRIMARY KEY, distkey |
| start_time  | TIMESTAMP  |  NOT NULL        |
| user_id     | INT        | NOT NULL, sortkey     |
| level       | VARCHAR       |  |
| song_id     | VARCHAR       | NOT NULL |
| artist_id   | VARCHAR       | NOT NULL |
| session_id  | INT        | NOT NULL |
| location    | VARCHAR       |  |
| user_agent  | VARCHAR       |  |

_Note_: Redshift does not support `SERIAL` for auto increment id. Instead, we can obtain an auto-increment for an integer by using `IDENTITY(<seed>, <step>)`.

#### Dimension Tables

We will setup four dimensional tables: `users`, `songs`, `artists`, and `time`. They each contain the following field names, types, and attributes.

**Table**: `users` -- collection of users in the app
| field name  | field type | field attribute(s) |
| ----------- | ---------- | ------------------ |
| user_id     | INT | PRIMARY KEY, sortkey |
| first_name  | VARCHAR | |
| last_name   | VARCHAR | |
| gender      | CHAR | |
| level       | VARCHAR | |


**Table**: `songs` -- collection of songs in music database
| field name  | field type | field attribute(s) |
| ----------- | ---------- | ------------------ |
| song_id     | VARCHAR | PRIMARY KEY, sortkey |
| title       | VARCHAR | NOT NULL |
| artist_id   | VARCHAR | NOT NULL |
| year        | INT | |
| duration    | FLOAT | |

**Table**: `artists` -- collection of artists in music database
| field name  | field type | field attribute(s) |
| ----------- | ---------- | ------------------ |
| artist_id   | VARCHAR | PRIMARY KEY, sortkey |
| name        | VARCHAR | NOT NULL |
| location    | VARCHAR | |
| latitude    | FLOAT | |
| longitude   | FLOAT | |

**Table**: `time` -- records timestamps of records in `songplays` broken down into specific units
| field name  | field type | field attribute(s) |
| ----------- | ---------- | ------------------ |
| start_time  | TIMESTAMP | PRIMARY KEY, sortkey |
| hour        | INT | |
| day         | INT | |
| week        | INT | |
| month       | INT | |
| year        | INT | |
| weekday     | INT | |

#### Visual  Representation

![image1.jpeg](./image1.jpeg)

### Creating Tables

Prior to creating the tables, we must go through the steps of creating and connecting to a Redshift cluster. We will do this by first loading all the relevant data warehouse parameters from `dwh.cfg` file which has the following structure:

```ini
[AWS]
KEY=<secret-key>
SECRET=<secret-secret>
SCHEMA_NAME=<schema_name>

[DWH]
DWH_CLUSTER_TYPE=multi-node
DWH_NUM_NODES=4
DWH_NODE_TYPE=dc2.large

DWH_IAM_ROLE_NAME=<iam_role_name>
DWH_CLUSTER_IDENTIFIER=<cluster_identifier>
DWH_DB=<db_name>
DWH_DB_USER=<db_user>
DWH_DB_PASSWORD=<db_password>
DWH_PORT=<port>
DWH_POLICY_ARN=arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess

[S3]
LOG_DATA=s3://udacity-dend/log_data
LOG_JSONPATH=s3://udacity-dend/log_json_path.json
SONG_DATA=s3://udacity-dend/song_data
```

We will create a database that has optimized design by utilizing the functionality of the database schema. Optimizing the table design will allow for better query performance and will minimize shuffling. We will name this database schema and make sure that all SQL statements will be executed in the context of this schema. The database schema is created through the `create_dist_schema` method.

Next, we'll setup IAM and Redshift clients, EC2 resource and retrieve the role ARN. The method `get_role_arn` used to retrieve the role ARN can be found under `create_tables.py`.

```python
iam = boto3.client("iam",
    region_name="us-west-2",
    aws_access_key_id=KEY,
    aws_secret_access-key=SECRET
)
redshift = boto3.client(
    "redshift",
    region_name="us-west-2",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)
ec2 = boto3.resource(
    "ec2",
    region_name="us-west-2",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)

roleArn = get_role_arn(iam, DWH_POLICY_ARN, DWH_IAM_ROLE_NAME)
```

Now that we have the relevant information needed, we can create the Redshift cluster, extract the cluster's endpoint address, and open an incoming TCP port to access the cluster endpoint.

```python
redshift.create_cluster(
    ClusterType=DWH_CLUSTER_TYPE,
    NodeType=DWH_NODE_TYPE,
    NumberOfNodes=int(DWH_NUM_NODES),
    DBName=DWH_DB,
    ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
    MasterUsername=DWH_DB_USER,
    MasterUserPassword=DWH_DB_PASSWORD,
    IamRoles=[roleArn],
)

dwh_endpoint = clusterProp["Endpoint"]["Address"]
vpcId = clusterProp["VpcId"]

vpc = ec2.Vpc(id=vpcId)
defaultSg = list(vpc.security_groups.all())[0]

defaultSg.authorize_ingress(
    GroupName=defaultSg.group_name,
    CidrIp="0.0.0.0/0",
    IpProtocol="TCP",
    FromPort=int(DWH_PORT),
    ToPort=int(DWH_PORT),
)
```

Next, we'll connect to the cluster.

```python
conn = psycopg2.connect(
    f"host={dwh_endpoint} dbname={DWH_DB} user={DWH_DB_USER} \
    password={DWH_DB_PASSWORD} port={DWH_PORT}"
)

cur = conn.cursor()
```

We'll drop any tables if they already exist, and recreate them. The queries to drop and create tables reside in `sql_queries.py`. The following SQL syntax is used:

```sql
DROP TABLE IF EXISTS <name>;
CREATE TABLE IF NOT EXISTS <name> (<fieldName1 type>, <fieldName2 type>, ..., <fieldAttribute1>, <fieldAttribute2>, ...);
```

The steps detailed above can be execute by running `create_tables.py` on your terminal, like so:

```bash
python create_tables.py
```

_Note_: A test method is introduced to make sure the tables were created successfully and the table status is printed. See `test_tables` method for detailed information.

### ETL Pipelines

Next, we will build ETL pipelines and implement them in `etl.py` to
1. load data from S3 to staging tables on Redshift and
2. load data from staging tables to analytics tables on Redshift.

To make sure that data has been inserted properly, we will run analytic queries to compare the results with the expected results. In order to keep AWS charges at a minimum, as a very last step, we will delete the Redshift cluster.

#### Populating Staging Tables

To load data from S3 to Redshift, we can utilize the SQL `copy` command. The syntax for copying the events and songs data from S3 into Redshift staging tables is as follows

```sql
copy <SCHEMA_NAME>.staging_events from '<S3_PATH>'
iam_role '<IAM_ROLE>'
json '<JSON_PATH>'
timeformat 'epochmillisecs'
region 'us-west-2';
```

```sql
copy <SCHEMA_NAME>.staging_songs from '<S3_PATH>'
iam_role '<IAM_ROLE>'
json 'auto'
region 'us-west-2';
```

#### Populating Fact and Dimension Tables

To insert records from staging tables, we follow a similar query syntax `"INSERT INTO <tableName>, (<fieldName1>, <fieldName2>, ...) SELECT (<fieldName1>, <fieldName2>,...) FROM <stageTableName>"`.

**SQL syntax for inserting records in `songplays`**:

```sql
INSERT INTO songplays
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    staging_events.ts as start_time,
    staging_events.userId as user_id,
    staging_events.level as level,
    staging_songs.song_id as song_id,
    staging_songs.artist_id as artist_id,
    staging_events.sessionId as session_id,
    staging_events.location as location,
    staging_events.userAgent as user_agent
FROM staging_events
JOIN staging_songs
ON (staging_events.artist=staging_songs.artist_name AND
    staging_events.song=staging_songs.title
WHERE staging_events.page='NextSong'
);
```

**SQL syntax for inserting records in `users`**:

```sql
INSERT INTO users
    (user_id, first_name, last_name, gender, level)
SELECT
    DISTINCT(staging_events.userId) as user_id,
    staging_events.firstName as first_name,
    staging_events.lastName as last_name,
    staging_events.gender as gender,
    staging_events.level as level
FROM staging_events
WHERE user_id IS NOT NULL AND staging_events.page='NextPage';
```

**SQL syntax for inserting records in `songs`**:

```sql
INSERT INTO songs
    (song_id, title, artist_id, year, duration)
SELECT
    DISTINCT(staging_songs.song_id) as song_id,
    staging_songs.title as title,
    staging_songs.artist_id as artist_id,
    staging_songs.year as year,
    staging_songs.duration as duration
FROM staging_songs;
```

**SQL syntax for inserting records in `artists`**:

```sql
INSERT INTO artists
    (artist_id, name, location, latitude, longitude)
SELECT
    DISTINCT(staging_songs.artist_id) as artist_id,
    staging_songs.artist_name as name,
    staging_songs.artist_location as location,
    staging_songs.artist_latitude as latitude,
    staging_songs.artist_longitude as longitude
FROM staging_songs;
```

**SQL syntax for inserting records in `time`**:

```sql
INSERT INTO time
    (start_time, hour, day, week, month, year, weekday)
SELECT
    DISTINCT(songplays.start_time) as start_time,
    EXTRACT(hour from songplays.start_time) as hour,
    EXTRACT(day from songplays.start_time) as day,
    EXTRACT(week from songplays.start_time) as week,
    EXTRACT(month from songplays.start_time) as month,
    EXTRACT(year from songplays.start_time) as year,
    EXTRACT(dow from songplays.start_time) as weekday
FROM songplays;
```

#### Testing ETL Pipelines

Once the tables have been populated, we will run a query for each table to make sure the table is populated. The results from each query are printed on the terminal when the `etl.py` file is executed.

#### Execute ETL Pipeline

Finally, to execute all the ETL pipelines, run the following on your command line:

```bash
python etl.py
```

### Error Handling

Both, the `create_tables.py` and `etl.py` use `logging` to document any exception handling on the terminal. If the script runs into any errors, the warning will be displayed on the user's terminal and will either terminate the program or continue to the next iteration. See both files for detailed information.

## Authors

Yuna Luzi - @najuzilu
