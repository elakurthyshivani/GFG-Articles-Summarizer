# GFG Articles Summarizer

## Prerequisites

### Installations using `pip`

These can be installed before running the Python notebooks using the following commands. If not these commands are present as required in the Python notebooks and can be installed when running these notebooks. I'm working on 3.3.0 version of Spark.

```bash
pip install bs4 nltk opendatasets spacy
pip install cassandra-driver
pip install "pyspark==3.3.0"
pip install --user https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.0.0/en_core_web_sm-3.0.0.tar.gz 
```

### Setup Requirements

**1. Python**:
Check if it is installed.
```bash
python --version
```

**2. Java**:
Check if Java is installed.
```bash
javac -version
```
If not, then install it using the following commands.
```bash
sudo apt-get install openjdk-8-jdk
```

**3.** Download the compatible version of `spark-cassandra-connector_2.12` and `spark-cassandra-connector-assembly_2.12` jar files (with pyspark version) into the desired directory:
```
wget https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.3.0/spark-cassandra-connector_2.12-3.3.0.jar
wget https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.3.0/spark-cassandra-connector-assembly_2.12-3.3.0.jar
```
To check the version compatibility of the connector - [https://github.com/datastax/spark-cassandra-connector#version-compatibility](https://github.com/datastax/spark-cassandra-connector#version-compatibility). I've using `3.3.0` version of the connector for PySpark version `3.3.0`.

**4.** I'm using service-specific credentials to connect to Amazon Keyspaces. 

**a.** Download the Starfield digital certificate to connect to Amazon Keyspaces.
```
curl https://certs.secureserver.net/repository/sf-class2-root.crt -O
```
You can refer more regarding this at [https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html#using_java_driver.BeforeYouBegin](https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html#using_java_driver.BeforeYouBegin).

**b.** Convert this certificate into `cassandra_truststore.jks` file. It will ask you to create a password. We need this password in `application.conf` file that we need to create next.
```
openssl x509 -outform der -in sf-class2-root.crt -out temp_file.der
keytool -import -alias cassandra -keystore cassandra_truststore.jks -file temp_file.der
```
You can refer more regarding this at [https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html#using_java_driver.BeforeYouBegin](https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html#using_java_driver.BeforeYouBegin).

**5.** Create `application.conf` file to connect with service-specific credentials.
```
datastax-java-driver {

    basic.contact-points = [ "cassandra.us-east-2.amazonaws.com:9142"]
    
    basic.load-balancing-policy {
        class = DefaultLoadBalancingPolicy
        local-datacenter = us-east-2
    }
    
    basic.request {
        consistency = LOCAL_QUORUM
    }
    advanced {
        auth-provider = {
            class = PlainTextAuthProvider
            username = "<your-service-credentials-username>"
            password = "<your-service-credentials-password>"
            aws-region = "us-east-2"
        }

        ssl-engine-factory {
            class = DefaultSslEngineFactory
            truststore-path = "<path-to>/cassandra_truststore.jks"
            truststore-password = "<your-password>"
            hostname-validation = false
        }

        metadata = {
            schema {
                 token-map.enabled = true
            }
        }
    }
}
```
Update in the above file with your AWS Region, your service-specific credentials, path and password to your `cassandra_truststore.jks` file. The path value for `cassandra_truststore.jks` file should be the path from your python notebook in which you will use.
To know more about this file - [https://docs.aws.amazon.com/keyspaces/latest/devguide/spark-tutorial-step3.html#appconfig.ssc](https://docs.aws.amazon.com/keyspaces/latest/devguide/spark-tutorial-step3.html#appconfig.ssc).

**6.** You can connect to Apache Cassandra in Python using the following code. This creates a session connection to the keyspace that is secured by TLS.
```python
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

ssl_context=SSLContext(PROTOCOL_TLSv1_2)
ssl_context.load_verify_locations('<path-to>/sf-class2-root.crt')
ssl_context.verify_mode=CERT_REQUIRED
exec_profile=ExecutionProfile(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
auth_provider=PlainTextAuthProvider(username='<your-username>', 'password=<your-password>')

cluster=Cluster(['cassandra.us-east-2.amazonaws.com'], 
                ssl_context=ssl_context, 
                auth_provider=auth_provider, 
                execution_profiles={EXEC_PROFILE_DEFAULT: exec_profile}, 
                port=9142)
session=cluster.connect()
```
Update your AWS region, path to the certificate file and your service-specific credentials. You can look at an example code at [https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/python/example_code/keyspaces/query.py](https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/python/example_code/keyspaces/query.py).

**7.** To connect to Amazon Keyspaces using PySpark programatically, use the following configuration.
```python
spark=SparkSession.builder.appName("AppName")\
    .config("spark.files", "<path-to>/application.conf")\
    .config("spark.jars", ".<path-to>/spark-cassandra-connector_2.12-3.3.0.jar,"
                            "<path-to>/spark-cassandra-connector-assembly_2.12-3.3.0.jar")\
    .getOrCreate()

spark.conf.set("spark.cassandra.connection.config.profile.path", "application.conf")
spark.conf.set("spark.cassandra.connection.ssl.clientAuth.enabled", "true")
spark.conf.set("spark.cassandra.connection.ssl.enabled", "true")
```
To read the data from a table in Keyspaces,
```python
articles=spark.read\
  .format("org.apache.spark.sql.cassandra")\
  .options(table="GFGArticles", keyspace="GFGArticles")\
  .load()

articles.show(5)
```
