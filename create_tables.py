import logging
import psycopg2
import configparser
import boto3
import json
import botocore
import psycopg2.extensions as psycopg2Ext

from sql_queries import create_table_queries, drop_table_queries

logger = logging.getLogger(__name__)


def get_role_arn(
    iam: botocore.client, DWH_POLICY_ARN: str, DWH_IAM_ROLE_NAME: str
) -> str:
    """
    Description: Create IAM role that make Redshift able to
        access S3 bucket in ReadOnly mode, attaches policy to
        IAM role and gets the IAM role ARN.

    Arguments:
        iam (botocore.client): IAM client
        DWH_POLICY_ARN (str): policy ARN
        DWH_IAM_ROLE_NAME (str): name of IAM role

    Returns:
        string containing IAM role ARN
    """
    try:
        iam.create_role(
            Path="/",
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift cluster to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
    except Exception as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            pass
        else:
            msg = "ERROR: Could not create IAM Role."
            logger.warning(msg, e)
            return

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn=DWH_POLICY_ARN,)[
        "ResponseMetadata"
    ]["HTTPStatusCode"]

    return iam.get_role(RoleName=DWH_IAM_ROLE_NAME)["Role"]["Arn"]


def create_dist_schema(
    DWH_DB_USER: str,
    schema_name: str,
    cur: psycopg2Ext.cursor,
    conn: psycopg2Ext.connection,
) -> None:
    """
    Description: Create distribution schema and set search path
        to the schema name.

    Arguments:
        DWH_DB_USER (str): db user name to restrict authorization
        schema_name (str): schema
        cur (psycopg2Ext.cursor): cursor object
        conn (psycopg2Ext.connection): connection object

    Returns:
        None
    """

    queries = [
        f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;",
        f"CREATE SCHEMA IF NOT EXISTS {schema_name} authorization {DWH_DB_USER };",
        f"SET search_path TO {schema_name};",
    ]

    for query in queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            msg = f"ERROR: Issue dropping/creating schema."
            logger.warning(msg, e)
            return
        conn.commit()


def drop_tables(cur: psycopg2Ext.cursor, conn: psycopg2Ext.connection) -> None:
    """
    Description: Drop each table using queries in
        `drop_table_queries` list.

    Arguments:
        cur (psycopg2Ext.cursor): cursor object
        conn (psycopg2Ext.connection): connection object

    Returns:
        None
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            msg = f"ERROR: Could not drop table with query: {query}"
            logger.warning(msg, e)
            return
        conn.commit()


def create_tables(cur: psycopg2Ext.cursor, conn: psycopg2Ext.connection) -> None:
    """
    Description: Create each table using the queries in
        `create_table_queries` list.

    Arguments:
        cur (psycopg2Ext.cursor): cursor object
        conn (psycopg2Ext.connection): connection object

    Returns:
        None
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            msg = f"ERROR: Could not create table with query: {query}"
            logger.warning(msg, e)
            return
        conn.commit()


def test_tables(cur: psycopg2Ext.cursor, conn: psycopg2Ext.connection) -> None:
    """
    Description: Test table status to make sure tables exists.

    Arguments:
        cur (psycopg2Ext.cursor): cursor object
        conn (psycopg2Ext.connection): connection object

    Returns:
        None
    """
    print("\n==================== TEST -- table status  ====================")

    for query in create_table_queries:
        tbl_name = query[query.find("EXISTS") + len("EXISTS") : query.find("(")].strip()
        query = f"""select exists(select * from information_schema.tables
            where table_name='{tbl_name}')"""

        try:
            cur.execute(query)
        except psycopg2.Error as e:
            msg = f"ERROR: Could not retrieve table info with query: {query}"
            logger.warning(msg, e)
            return
        conn.commit()

        try:
            tbl_status = cur.fetchone()[0]
        except psycopg2.Error as e:
            msg = f"ERROR: Could not fetch table status for table: {tbl_name}"
            logger.warning(msg, e)
            return

        print(f"Table '{tbl_name}' exists status: {tbl_status}.")


def main():
    """
    Description: Setup appropriate AWS clients (IAM role and Redshift),
        create a new Redshift cluster, connect to cluster and create
        tables. At the very end, it checks to see if tables exists and
        prints their respective status.

    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    # Load DWH Params from file
    KEY = config.get("AWS", "KEY")
    SECRET = config.get("AWS", "SECRET")
    SCHEMA_NAME = config.get("AWS", "SCHEMA_NAME")

    DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("DWH", "DWH_DB")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH", "DWH_PORT")
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    DWH_POLICY_ARN = config.get("DWH", "DWH_POLICY_ARN")

    # setup iam and redshift clients
    iam = boto3.client(
        "iam",
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )

    roleArn = get_role_arn(iam, DWH_POLICY_ARN, DWH_IAM_ROLE_NAME)

    redshift = boto3.client(
        "redshift",
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )

    # setup ec2
    ec2 = boto3.resource(
        "ec2",
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )

    print("Creating Redshift cluster...")

    # create Redshift cluster
    try:
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
    except Exception as e:
        if e.response["Error"]["Code"] == "ClusterAlreadyExists":
            pass
        else:
            msg = "ERROR: Could not create a Redshift cluster."
            logger.warning(msg, e)
            return

    # get cluster status
    clusterProp = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)[
        "Clusters"
    ][0]

    clusterStatus = clusterProp["ClusterStatus"]

    while clusterStatus == "creating":
        clusterProp = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
        )["Clusters"][0]
        clusterStatus = clusterProp["ClusterStatus"]

    print(f"\nCluster created successfully. Cluster status='{clusterStatus}'")

    # cluster endpoint
    dwh_endpoint = clusterProp["Endpoint"]["Address"]

    # open incoming TCP port to access cluster endpoint
    vpcId = clusterProp["VpcId"]
    try:
        vpc = ec2.Vpc(id=vpcId)
        defaultSg = list(vpc.security_groups.all())[0]

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT),
        )
    except Exception as e:
        if e.response["Error"]["Code"] == "InvalidPermission.Duplicate":
            pass
        else:
            msg = "ERROR: Could not open incoming TCP port."
            logger.warning(msg, e)
            return

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

    print("Creating schema...")
    # create schema
    create_dist_schema(DWH_DB_USER, SCHEMA_NAME, cur, conn)
    print("Schema successfully created!")

    # drop tables
    drop_tables(cur, conn)

    print("Creating tables...")
    # create tables
    create_tables(cur, conn)
    print("Tables successfully created!")

    # test to check if tables were created
    test_tables(cur, conn)

    # close connection
    conn.close()


if __name__ == "__main__":
    main()
