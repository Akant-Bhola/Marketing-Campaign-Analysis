# Marketing-Campaign-Analysis - Databricks (PySpark + SQL)

This repository contains a complete **Marketing Campaign Analytics** project built using
**Databricks Community Edition**, **PySpark**, and **SQL**.

The project performs KPI engineering, exploratory analysis, SQL benchmarking, and advanced visualizations to evaluate campaign performance across channels, segments, and duration categories.

---

## 🔍 Project Overview

The dataset includes detailed campaign information:

* Impressions, Clicks, CTR
* Conversion Rate
* Acquisition Cost
* ROI
* Channel Used
* Customer Segment
* Duration (4 categorical day ranges)

The notebook covers:

* KPI engineering
* Feature derivation
* Exploratory analysis (EDA)
* ROI classification
* Channel & segment comparison
* SQL-based campaign ranking and filtering
* Advanced trend and performance visualizations

---

## 📁 Repository Structure

```
marketing-campaign-analysis/
│
├── README.md
│
├── notebooks/
│   ├── Marketing_Campaign_Analysis.dbc
│   ├── marketing_Campaign_Analysis.py
│   ├── Marketing_Campaign_Analysis.html
│   ├── Marketing_Campaign_Analysis.ipynb
│   └── Marketing_Campaign_Analysis_report.pdf   
│
├── marketing_campaign_dataset.zip

```

---

## 📄 Notebook Viewing 

GitHub’s preview for **`.ipynb` notebooks does not always render interactive and HTML-rich Databricks content correctly**.

For the best viewing experience:

### ✅ Recommended:

* **Download the `.html` version** and open it in your browser
* **Import the `.dbc` file directly into Databricks** (preserves all formatting, SQL, and visualizations)

### 📌 Additional:

A **PDF file with merged screenshots** of the full Databricks notebook is also included for quick reference.

---

## 🚀 How to Run the Project

### **1. In Databricks Community Edition**

1. Log in
2. Go to **Workspace → Import**
3. Upload the `.dbc` or `.ipynb` file
4. Upload dataset to **Files**
5. Attach to a cluster
6. Run cells sequentially

### **2. Local (Optional)**

Run `.ipynb` locally:

```bash
pip install pyspark pandas matplotlib
```

---

## 📈 Summary of Insights

* Identified high-ROI vs low-ROI campaigns
* Highlighted expensive segments and inefficient channels
* Detected low-CTR campaigns even with high impressions (SQL)
* Mapped performance by duration categories
* Visualized channel, audience, and cost interactions
* Created polished visuals suitable for reporting and presentations

---

## 👤 **Author**

**Akant Bhola**  
Data Analyst  
📧 **Email:** akantbhola.AB@gmail.com  
🔗 **LinkedIn:** https://www.linkedin.com/in/akant-bhola/  

---

