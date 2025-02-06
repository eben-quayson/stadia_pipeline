# Stadium Data Pipeline Project  

## Project Overview  
This project uses Apache Airflow to build an automated data pipeline that extracts, transforms, and loads (ETL) data about football stadiums from a Wikipedia page into a PostgreSQL database. The data is then visualized in Power BI to provide key insights about global football stadiums.

---

## Table of Contents  
- [Technologies Used](#technologies-used)  
- [Architecture](#architecture)  
- [Getting Started](#getting-started)  
  - [Prerequisites](#prerequisites)  
  - [Installation](#installation)  
- [DAG Tasks](#dag-tasks)  
- [Configuration](#configuration)  
- [Database Schema](#database-schema)  
- [Power BI Dashboard](#power-bi-dashboard)  
- [Running the Project](#running-the-project)  
- [Improvements](#improvements)  

---

## Technologies Used  
- **Apache Airflow** for workflow orchestration  
- **Pandas** for data manipulation  
- **BeautifulSoup** for web scraping  
- **PostgreSQL** for database storage  
- **SQLAlchemy** for database interaction  
- **Power BI** for data visualization  

---

## Architecture  
1. **Data Extraction:** Extracts football stadium data from Wikipedia.  
2. **Data Transformation:** Cleans and structures the extracted data.  
3. **Data Loading:** Stores the transformed data in a PostgreSQL table.  
4. **Data Visualization:** Power BI connects to PostgreSQL to visualize key insights.

---

## Getting Started  

### Prerequisites  
- Python 3.8+  
- PostgreSQL Database  
- Airflow installed and set up  
- Power BI Desktop  

### Installation  
1. Clone the repository:  
   ```bash
   git clone <https://github.com/eben-quayson/stadia_pipeline.git>
   cd stadium-pipeline


