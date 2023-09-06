# GFG Articles Summarizer

## Prerequisites

### Installations using `pip`

These can be installed before running the Python notebooks using the following commands. If not these commands are present as required in the Python notebooks and can be installed when running these notebooks.

```bash
pip install azure.storage.blob
pip install nltk, spacy, pyspark
pip install bs4 opendatasets
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
**3. Spark** and **Hadoop**:
Download the latest version of the following Spark and Hadoop binaries. I'm extracting these files into `hadoop` folder.
```bash
wget https://dlcdn.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-without-hadoop.tgz
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xvf spark-3.4.1-bin-without-hadoop.tgz -C ../hadoop
tar -xvf hadoop-3.3.6.tar.gz -C ../hadoop
```
**4.** Path for `JAVA_HOME`:
Run the following command and the path up to `/jre` represents `JAVA_HOME`.
```bash
update-alternatives --display java
```
**5.** Edit the following file and add the environment configurations:
```bash
vi ~/.profile
```
```
export SPARK_HOME=Users/selak001/Dependencies/hadoop/spark-3.4.1-bin-without-hadoop
export HADOOP_HOME=Users/selak001/Dependencies/hadoop/hadoop-3.3.6
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre
```
To reflect these changes, run the following command:
```bash
source ~/.profile
```
**6. PySpark**:
```bash
pip install pyspark
pip install pyarrow
```
**7.** Go to the folder of the extracted Spark files and go into `conf/`. Edit the following file:
```bash
vi spark-env.sh
```
```
export SPARK_DIST_CLASSPATH=$(Users/selak001/Dependencies/hadoop/hadoop-3.3.6/bin/hadoop classpath)
```
**8.** Check if PySpark is configured correctly:
```bash
sudo su
# pyspark
```

### Installing and configuring dependencies for Azure Blob Storage

**1.** Download the latest version of the following jar files into the desired directory: 
```
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.6/hadoop-azure-3.3.6.jar
wget https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar
```

**2.** Use the jar files in the PySpark shell environment:
```
pyspark --jars jar-files/hadoop-azure-3.3.6.jar,jar-files/azure-storage-8.6.6.jar
```
