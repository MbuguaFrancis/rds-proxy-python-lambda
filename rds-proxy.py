import json
import boto3
from os import environ
import pymysql

client = boto3.client('rds')  # get the rds object


def create_connection_token():
    
    # get the required parameters to create a token
    region = environ.get('region') # get the region
    hostname = environ.get('rds_endpoint')  # get the rds proxy endpoint
    port = environ.get('port')  # get the databse port
    username = environ.get('username')  # get the database username
    
    # generate the authentication token -- temporary password
    token = client.generate_db_auth_token(
        DBHostname=hostname,
        Port=port,
        DBUsername=username,
        Region=region
        )
    
    return token


def db_ops():
    # get the temporary password
    token = create_connection_token()
    try:
        # create a connection object
        connection = pymysql.connect(
            host=environ.get('your rds proxy endpoint'), # getting the rds proxy endpoint from the environment variables
            user=environ.get('your database user'), # get the database user from the environment variables
            password=token,
            db=environ.get('your database'),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            ssl={"use": True }
        )
    except pymysql.MySQLError as e:
        return e

    return connection

    
def lambda_handler(event, context):
    # TODO implement
    conn = db_ops()
    cursor = conn.cursor()
    query = "SELECT count(*) FROM customer_entity"
    cursor.execute(query)
    results = cursor.fetchmany(4)
    
    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }
