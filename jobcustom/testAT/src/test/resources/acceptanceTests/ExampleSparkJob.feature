@ActivateSegment
Feature: Feature for ExampleSparkJob

  Scenario: Test ExampleSparkJob should result OK
    Given a config file path in HDFS hdfs://hadoop:9000/application.conf
    When execute ExampleSparkJob in Spark


  Scenario Outline: Test custom implementations result OK
    Given a config file path in HDFS hdfs://hadoop:9000/<pathConfig>.conf
    When execute ExampleSparkJob in Spark


    Examples:
        | pathConfig     |
        | application |

  Scenario: end the word count
    Given create a file output

