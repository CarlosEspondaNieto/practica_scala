# Datio Archetype Template for Spark projects. 

# README for ExampleSparkJob

## Remove the next section and fill me with documentation 

# Getting started with the new project
## First Compilation

Make a clean install in the root directory of the proyect

```bash
mvn clean install
```

Streaming jobs needs to set the `duration` parameter (In main class ExampleSparkJob) for a success compilation.

The testAT are commented, for run testAT uncomment the next line in `RunCukesTest.scala` file
```
@CucumberOptions(features = Array(
  //"src/test/resources/acceptanceTests/ExampleSparkJob.feature"
)
```

## First Execution

Go to your IDE run configurations window and set the next configuration:
* Enable the maven profile `run-local`
* Set the next VM Option =>  `-Dspark.master=local[*]`
* Set the main class => `com.datio.jobcustom.ExampleSparkJob`
* Set as program argument a valid path to a configuration file (not empty)


Generated with spark-job-archetype version 1.0.4