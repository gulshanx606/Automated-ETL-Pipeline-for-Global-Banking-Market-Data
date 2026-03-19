# 🚀 Automated ETL Pipeline for Global Banking Market Data

## 📌 Overview

This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline using Python.
It extracts data of the top 10 largest banks in the world based on market capitalization, transforms it into multiple currencies, and loads it into storage systems.

---

## ⚙️ Tech Stack

* Python
* Pandas
* BeautifulSoup
* SQLite (SQL)
* CSV
* Logging

---

## 🔄 ETL Workflow

### 🔹 Extract

* Scraped data from Wikipedia using BeautifulSoup
* Extracted bank names and market capitalization

### 🔹 Transform

* Converted USD values into GBP, EUR, INR
* Cleaned and formatted the dataset

### 🔹 Load

* Saved data into CSV file
* Loaded data into SQLite database

### 🔹 SQL Analysis

* Top 5 largest banks
* Average market capitalization
* Top bank identification

---

## 📁 Project Files

* `banks_project.py` → Main ETL script
* `Largest_banks_data.csv` → Processed dataset
* `code_log.txt` → Execution logs

---

## 🚀 Features

* End-to-end ETL pipeline
* Web scraping
* Data transformation
* SQL queries
* Logging system

---

## 👨‍💻 Author

**Gulshan Kumar**
