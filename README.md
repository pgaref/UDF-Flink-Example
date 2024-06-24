# UDF-Java

This repository includes a sample UDF function which checks the percentage of  match between two strings.

## Set up Pre-requisites
```
export TUTORIAL_HOME=<Tutorial directory>/UDF-Flink-JWrinker
```

# Generate JAR 

1. To generate the necessary JAR File for upload via the CLI, execute the command below.


```
mvn clean package

```

NOTE : You can write your custom logic on <b>public double eval() </b>function based on your usecase by removing the existing code


# Register the JAR File

To register the JAR file generated after executing the `mvn clean package` command on the CC organization, start by logging into the Confluent CLI and then use the following command.

```
confluent flink artifact create 
your-artifact-name
--artifact-file path-to-jar-file.JAR
```

Upon successful execution of the command, it will produce a plugin ID and version ID, which should resemble the following format.

```
+----------------+----------------+
| Name           | artifact-name    |
| Plugin ID      | ccp-xxxxxxxxxxx  |
| Version ID     | ver-xxxxxxxxxxx  |
| Content Format | JAR              |
+----------------+----------------+

```
Utilize the command below to display a list of all uploaded artifacts.

```
confluent flink artifact list

```

# Using UDF on FlinkSQL

Once you've uploaded the JAR using the CLI, proceed to the Flink SQL editor to register the UDF within your specific Flink workspace with the following command.

```
CREATE FUNCTION ISO_TO_MIN_PANOS 
AS 'io.confluent.flink.table.modules.remoteudf.DurationToMinutesFunction' 
USING JAR 'confluent-artifact://ccp-ewz1qj/ver-42o21y';
```

Specific example for ISO to Minutes UDF:

```
CREATE FUNCTION ISO_TO_MIN_PANOS 
AS 'io.confluent.flink.table.modules.remoteudf.DurationToMinutesFunction' 
USING JAR 'confluent-artifact://ccp-ewz1qj/ver-42o21y';
```

```
CREATE FUNCTION ISO_TO_MIN_PANOS 
AS 'io.confluent.flink.table.modules.remoteudf.DurationToMinutesFunction' 
USING JAR 'confluent-artifact://ccp-ewz1qj/ver-42o21y';
```

```
select ISO_TO_MIN_PANOS(`duration`) from (values 'P1D', 'P5D') as duration (duration);
```


NOTE: Replace the class with your actual class that contains the eval function.

Now that the UDF is successfully registered, you can utilize the command below to display a list of all UDF functions:

```
SHOW USER FUNCTIONS;
```

Here is the Sample command which uses UDF function that we have just created

```
select regionid, FUNCTION UDF_NAME(gender,"MAALE") from sample data
```


![image](https://github.com/Gokuldev-PS/UDF-Flink-JWrinker/assets/132561683/dc072c92-4e80-4c8f-bb43-10f4dcc85553)



