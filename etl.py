import logging
import configparser
import psycopg2
import boto3
import psycopg2.extensions as psycopg2Ext
from sql_queries import copy_table_queries, insert_table_queries


logger = logging.getLogger(__name__)


def load_staging_tables(
    SCHEMA_NAME: str,
    S3_LOG_DATA: str,
    S3_LOG_JSONPATH: str,
    S3_SONG_DATA: str,
    roleArn: str,
    cur: psycopg2Ext.cursor,
    conn: psycopg2Ext.connection,
) -> None:
    """
    Description: Load partitoned data into the cluster.

    Arguments:
        SCHEMA_NAME (str): schema
        S3_LOG_DATA (str): log data path in S3
        S3_LOG_JSONPATH (str): jsonpath in S3
        S3_SONG_DATA (str): song data path in S3
        roleArn (str): IAM role ARN
        s3_buckets (Tuple[str]): tuple containing addresses of S3 buckets
        cur (psycopg2Ext.cursor): cursor object
        conn (psycopg2Ext.connection): connection object

    Returns:
        None
    """
    queries = [
        copy_table_queries[0].format(
            SCHEMA_NAME, S3_LOG_DATA, roleArn, S3_LOG_JSONPATH
        ),
        copy_table_queries[1].format(SCHEMA_NAME, S3_SONG_DATA, roleArn),
    ]

    print("Copying data from S3 to staging Redshift tables...")
    for query in queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            msg = f"ERROR: Could not copy table with query: {query}"
            logger.warning(msg, e)
            continue


def insert_tables(cur: psycopg2Ext.cursor, conn: psycopg2Ext.connection) -> None:
    """
    Description: Insert data from staging tables to final tables.

    Arguments:
        cur (psycopg2Ext.cursor): cursor object
        conn (psycopg2Ext.connection): connection object

    Returns:
        None
    """
    print("Inserting data from staging to final tables...")
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            msg = f"ERROR: Could not insert data into table with query: {query}"
            logger.warning(msg, e)
            return


def test_queries(
    SCHEMA_NAME: str, cur: psycopg2Ext.cursor, conn: psycopg2Ext.connection
) -> None:
    """
    Description: Test queries to make sure data is successfully inserted.

    Arguments:
        SCHEMA_NAME (str): schema
        cur (psycopg2Ext.cursor): cursor object
        conn (psycopg2Ext.connection): connection object

    Returns:
        None
    """
    for query in insert_table_queries:
        tbl_name = query[query.find("INTO") + len("INTO") : query.find("(")].strip()
        test_query = f"SELECT * FROM {tbl_name} LIMIT 5;"

        print(f"\n==================== TEST -- {tbl_name}  ====================")
        print(f"Query: `{test_query}`")

        try:
            cur.execute(test_query)
        except psycopg2.Error as e:
            msg = f"Could not query table `{tbl_name}`"
            logger.warning(msg, e)
            conn.commit()
            continue

        try:
            data = cur.fetchall()
        except psycopg2.Error as e:
            msg = f"Could not fetch data from table `{tbl_name}`"
            logger.warning(msg, e)
            conn.commit()
            continue

        for row in data:
            print(row)

    conn.commit()
    return


def main() -> None:
    """
    Description: Setup appropriate AWS clients (IAM role and Redshift),
        connect to Redshift cluster, load data from S3 to staging tables
        in Redshift, and insert data from staging tables to final tables.
        Lastly, delete Redshift cluster, detach IAM policy role and
        delete IAM role.

    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    # Load DWH Params from file
    KEY = config.get("AWS", "KEY")
    SECRET = config.get("AWS", "SECRET")
    SCHEMA_NAME = config.get("AWS", "SCHEMA_NAME")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("DWH", "DWH_DB")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH", "DWH_PORT")
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    DWH_POLICY_ARN = config.get("DWH", "DWH_POLICY_ARN")

    S3_LOG_DATA = config.get("S3", "LOG_DATA")
    S3_LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
    S3_SONG_DATA = config.get("S3", "SONG_DATA")

    # setup iam client
    iam = boto3.client(
        "iam",
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )

    redshift = boto3.client(
        "redshift",
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )

    clusterProp = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)[
        "Clusters"
    ][0]

    # cluster endpoint
    dwh_endpoint = clusterProp["Endpoint"]["Address"]

    # connect to cluster
    try:
        conn = psycopg2.connect(
            f"host={dwh_endpoint} dbname={DWH_DB} user={DWH_DB_USER} \
            password={DWH_DB_PASSWORD} port={DWH_PORT}"
        )
    except psycopg2.Error as e:
        msg = "ERROR: Could not make connection to dwh."
        logger.warning(msg, e)
        return

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        msg = "ERROR: Could not get cursor to sparkify database."
        logger.warning(msg, e)
        return

    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)["Role"]["Arn"]

    # load staging tables
    load_staging_tables(
        SCHEMA_NAME, S3_LOG_DATA, S3_LOG_JSONPATH, S3_SONG_DATA, roleArn, cur, conn
    )

    # insert from staging to fact/dim tables
    insert_tables(cur, conn)

    # test queries
    test_queries(SCHEMA_NAME, cur, conn)

    conn.close()

    print("Deleting cluster...")

    # delete cluster
    redshift.delete_cluster(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True
    )
    clusterProp = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)[
        "Clusters"
    ][0]
    clusterStatus = clusterProp["ClusterStatus"]

    while clusterStatus == "deleting":
        try:
            clusterProp = redshift.describe_clusters(
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
            )["Clusters"][0]
            clusterStatus = clusterProp["ClusterStatus"]
        except Exception:
            break

    print(f"Cluster deleted successfully.")

    # detach role policy and delete role
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn=DWH_POLICY_ARN)
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)


if __name__ == "__main__":
    main()
