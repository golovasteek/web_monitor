# Web Monitor
Web Monitor is a toolset designed for distributed monitoring of webpages availability presistenly storing the check results in PostgreSQL.

# Quick Start
In order to use Web Monitor you need Kafka and PostgreSQL installations availble. You can either use own installation or use one of Cloud-based solutions (see Amazon RDS, Amazon MSK, or aiven.io), setting up these services is out scope for this documetn.

## Configuration 
The example configuration of kafka and postgresql can be found in `config/web_monitor.yaml`. Note that acces certificates and passwords have to be stored in the files, and only file names are specified in the configuration.

### URLs to monitor
The list of pages to monitor is specified in the same configuration file. List of pages contains monitoring configuration for each page.
Page configuration is currently just url to check, and check period in seconds

    pages:
        - url: https://example.com
          period: 5
        - url: https://github.com
          period: 30

---
**NOTE**

Currently period parameters are ignored, and checks are done with fixed interval of 1 second

---

## Execution
Web Monitor includes two service executables:

* `web_checker.py` - agent, which checks sites availability and publishes every check result to kafka topic.
* `kafka_pg_transfer.py` - service, which listens for kafka topic and commits all check results into PostgreSQL database.


## Design Notes

The Web Monitor toolset was design with modularity in mind. The idea is, that there is a data producers and consumers. The data producers are configured by `sink` parameter, which should be a data consumer. Data consumer could be any callable object, which is called with every item of the data.
