# Media Distributors ETL and Data Analysis Project

This repository contains two internal projects aimed at building and analyzing a data pipeline for Media Distributors, Inc., a company managing film and music sales. The projects focus on designing, implementing, and analyzing a database and data warehouse to provide insights into sales, revenue, and customer metrics.

## Table of Contents
- [Overview](#overview)
- [Project Structure](#project-structure)
- [Technologies Used](#technologies-used)
- [Internal Projects](#internal-projects)
  - [1. DB ETL](#1-db-etl)
  - [2. Datawarehouse ETL](#2-datawarehouse-etl)

---

## Overview

Media Distributors, Inc. manages film and music sales through separate systems. This project integrates these systems into a unified view for business analysis and acquisition readiness. The repository includes:
- ETL pipelines for operational databases and a data warehouse.
- Business analysis reports for key metrics like revenue, customer distribution, and sales trends.

---

## Project Structure

- **DB ETL**
  - `createDB.PractI.AdhikariT.R`
  - `loadDB.PractI.AdhikariT.R`
  - `testDBLoading.PractI.AdhikariT.R`
  - `configBusinessLogic.PractI.AdhikariT.R`
  - `RevenueReport.PractI.AdhikariT.Rmd`
  - `designDBSchema.PractI.AdhikariT.Rmd`
  - `deleteDB.PractI.AdhikariT.R`

- **Datawarehouse ETL**
  - `createStarSchema.PractII.AdhikariT.R`
  - `loadAnalyticsDB.PractII.AdhikariT.R`
  - `BusinessAnalysis.PractII.AdhikariT.Rmd`
  - `BusinessAnalysis.PractII.AdhikariT.html`
  - `deleteDB.PractII.AdhikariT.R`

- `README.md`


---

## Technologies Used

- **R**: For ETL pipelines, database interactions, and data analysis.
- **MySQL**: Operational database for transactional data.
- **SQLite**: Lightweight database for testing and analytics.
- **kableExtra**: For generating formatted tables in reports.
- **ggplot2**: For visualizing trends and metrics.
- **Markdown**: For documentation and reporting.


# DB ETL

This project focuses on designing and managing the operational database:

- **Schema Design**: Normalized schema in 3NF.
- **ETL Pipeline**: Scripts to load and test data.
- **Reports**: Revenue analysis by restaurant, year, and trends.

---

# Datawarehouse ETL

This project builds a star schema data warehouse for analytics:

- **Star Schema Design**: Fact and dimension tables for OLAP.
- **ETL Pipeline**: Scripts to load data from operational databases.
- **Business Analysis**: Key metrics like sales revenue, customer distribution, and film vs. music revenue.

---

# Author

**Tejas Adhikari**  
