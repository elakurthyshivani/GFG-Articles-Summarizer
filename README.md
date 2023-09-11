# GFG Articles Summarizer

## Prerequisites

### Installations using `pip`

These can be installed before running the Python notebooks using the following commands. If not these commands are present as required in the Python notebooks and can be installed when running these notebooks. I'm working on 3.3.0 version of Spark.

```bash
pip install azure.storage.blob
pip install bs4 nltk opendatasets spacy
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

**3.** Download the compatible version of azure-cosmosdb-spark jar file (with pyspark version) into the desired directory:
```
wget https://repo1.maven.org/maven2/com/azure/cosmos/spark/azure-cosmos-spark_3-3_2-12/4.21.1/azure-cosmos-spark_3-3_2-12-4.21.1.jar
```
Cosmos DB Spark Connectors are available at [https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/cosmos/azure-cosmos-spark_3_2-12/docs/quick-start.md](https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/cosmos/azure-cosmos-spark_3_2-12/docs/quick-start.md).

To check the version compatibility of the connector - [https://github.com/Azure/azure-sdk-for-java/tree/main/sdk/cosmos/azure-cosmos-spark_3-3_2-12](https://github.com/Azure/azure-sdk-for-java/tree/main/sdk/cosmos/azure-cosmos-spark_3-3_2-12). I've using `4.21.0` version of Cosmos DB Spark Connector 3 for PySpark version `3.3.0`.

**4.** Use the jar files in the PySpark shell environment:
```
pyspark --jars ../jar-files/azure-cosmos-spark_3-3_2-12-4.21.1.jar
```
To use it in Jupyter notebook:
```
spark=SparkSession.builder.appName("SparkConnectors")\
    .config("spark.jars", "../jar-files/azure-cosmos-spark_3-3_2-12-4.21.1.jar")\
    .getOrCreate()
```

Configuration reference for Cosmos DB using PySpark: [https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/cosmos/azure-cosmos-spark_3_2-12/docs/configuration-reference.md](https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/cosmos/azure-cosmos-spark_3_2-12/docs/configuration-reference.md).
