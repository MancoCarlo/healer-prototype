# HEALER: A Data Lake Architecture for Healthcare

This repository contains the files necessary to instantiate the Docker containers for building the proof of concept of our healthcare data lake architecture solution HEALER (**HE**althcare d**A**ta **L**ak**E** a**R**chitecture).
HEALER is a data lake architecture designed to effectively perform data ingestion, data storage, and data access with the aim of providing a single central repository for efficient storage of different types of healthcare data, both structured and unstructured.

## Contents

Under the folder `architecture`, the three subfolders `apache-nifi-compose`, `hdfs-compose` and `spark-cluster-compose` contain the necessary files to instantiate and run their corresponding component of the data lake architecture: ingestion, storage and processing/analysis.
They each contain a `.yml` file to build the respective Docker container: after cloning the repository locally, you can build the respective containers by running the command `docker compose build` from inside each folder.

The folder `code` contains the Python scripts executed to evaluate the proof of concept.
In particular, these scripts perform a variety of simple, yet typical data-related tasks for healthcare data architectures, such as raw structured/unstructured data ingestion and processing of waveforms, in addition to querying relational data and detecting keywords from natural language text.
