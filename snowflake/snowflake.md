on mac:
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out privatekeydatabricks.p8 -nocrypt
openssl rsa -in privatekeydatabricks.p8 -pubout -out privatekeydatabricks.pem

in snowflake:
create user databricksaccess;
GRANT ROLE XXX TO user databricksaccess;

ALTER USER databricksaccess SET RSA_PUBLIC_KEY='......';


using databricks cli:
databricks fs mkdirs dbfs:/keys/
databricks fs cp privatekeydatabricks.p8 dbfs:/keys/privatekeydatabricks.p8


conneciton for spark:
dbutils.fs.cp("dbfs:/keys/privatekeydatabricks.p8", "file:/tmp/privatekeydatabricks.p8")
with open("/tmp/privatekeydatabricks.p8", "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
        backend=default_backend()
    )
private_key_pem = private_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
).decode("utf-8")

private_key_hex64 = re.sub(r"-----.*-----|\n", "", private_key_pem)

query = "SELECT * FROM DWH.EDGEN_EDUISAED0048.STUDENT_STUDENT limit 10"

df = spark.read.format("snowflake").options(**{
    "sfURL": ".snowflakecomputing.com",
    "sfDatabase": "DWH",
    "sfSchema": "ED",
    "sfWarehouse": "general_compute",
    "sfRole": "sysadmin",
    "pem_private_key": private_key_hex64,
    "sfUser": "databricksaccess",
}).option("query", query).load()

display(df)




