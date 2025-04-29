# Gets around the SSO issue to connect also possible with RSA key but this was easier to resolve for quick testing

# %pip install snowflake-connector-python
# %pip install snowflake-sqlalchemy

from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import snowflake.connector

conn = snowflake.connector.connect(
    account='XXXXX.azure',
    user='XXXXX',
    authenticator='externalbrowser',
    database='DWH',
    schema='XXX',
    warehouse='general_compute',
    role='XXXX'
)

query = "SELECT * FROM TABLE"


df = spark.read.format("snowflake").options(**{
    "sfURL": "XXXX.azure",
    "sfDatabase": "DWH",
    "sfSchema": "XXXX",
    "sfWarehouse": "general_compute",
    "sfRole": "XXXX",
    "sfAuthenticator": "oauth",
    "sfUser": "david.draper@.co.uk",
    "sfToken":conn.rest.token #uses rest token from the connector to connect
}).option("query", query).load()

display(df)
