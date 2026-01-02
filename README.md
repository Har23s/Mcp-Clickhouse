
<img width="1150" height="769" alt="Screenshot 2025-11-06 172431" src="https://github.com/user-attachments/assets/04da1629-f4e1-48d8-84e7-f4d9f121c12b" />
The Datagpt project includes ClickHouse Data Migration and Analysis project focuses on migrating data from an on-premise SQL Server to ClickHouse using PySpark (on Databricks) and FastAPI. The main objective is to enable fast, cloud-based analytics through automated ETL and Change Data Capture (CDC) processe.

In this project, data is first extracted from SQL Server using PySpark, then transformed and cleaned within Databrickss. The processed data is sent to ClickHouse through a FastAPI endpoint, where it becomes available for analytical queries and visualization through tools such as Grafana or Apache Superset. This migration ensures high performance, real-time analytics, and scalability for enterprise dataset.

The technology stack includes SQL Server as the source system, Azure Databricks with PySpark for ETL and CDC processing, FastAPI for secure data transfer, ClickHouse as the analytical data warehouse, and Grafana or Superset for visualization.

The project structure consists of separate folders for notebooks (data extraction, transformation, and loading scripts), configuration files (SQL Server and ClickHouse connection details), API scripts (FastAPI endpoint and utilities), and analysis resources (queries and dashboard templates).

To run the projectt, thee repository can be cloned from GitHub. After updating the configuration files with database details, dependencies installed, and the ETL script can be executed to begin the data migration process. Once the data is loaded into ClickHouse, user can perform SQL-based analysis such as department-wise salary summaries or build interactive dashboard.
