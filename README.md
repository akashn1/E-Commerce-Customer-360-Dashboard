# E-Commerce-Customer-360-Dashboard
📌 Overview
This project demonstrates an end-to-end Microsoft Fabric Data Engineering workflow by building an E-Commerce Customer 360 Dashboard. It was created to gain hands-on experience with the Microsoft Fabric ecosystem and understand how its core components work together—from data ingestion to business reporting.
The project uses PySpark for data transformation, Delta Tables for optimized storage, and Power BI for interactive analytics.
🚀 Project Architecture
Parquet Files
      │
      ▼
Microsoft Fabric Lakehouse
      │
      ▼
PySpark Data Cleaning & Transformation
      │
      ▼
Delta Tables
      │
      ▼
Customer 360 Data Model
      │
      ▼
Power BI Dashboard
🛠️ Tech Stack
Microsoft Fabric
Fabric Lakehouse
PySpark
Delta Lake
Power BI
Parquet Files
📂 Dataset
The project uses five datasets:
Customers
Orders
Payments
Support Tickets
Web Activities
⚙️ Workflow
1. Data Ingestion
Loaded Parquet files into Microsoft Fabric Lakehouse.
Created Delta tables for efficient storage.
2. Data Transformation
Performed data cleaning using PySpark:
Removed duplicate records
Handled missing values
Standardized date formats
Standardized text values
Corrected data types
3. Customer 360 Model
Integrated multiple datasets to create a unified customer view.
4. Dashboard Development
Built an interactive Power BI dashboard to analyze:
Customer information
Sales performance
Payment insights
Support ticket analysis
Customer web activity
📊 Dashboard Features
Customer Overview
Sales Analysis
Payment Summary
Support Ticket Insights
Customer Activity Tracking
📁 Repository Structure
📦 E-Commerce-Customer360-Fabric
│
├── Data/
│   ├── customers.parquet
│   ├── orders.parquet
│   ├── payments.parquet
│   ├── support_tickets.parquet
│   └── web_activities.parquet
│
├── Notebooks/
│   ├── Data_Ingestion.ipynb
│   ├── Data_Cleaning.ipynb
│   └── Customer360_Model.ipynb
│
├── Dashboard/
│   └── ECommerce_Customer360.pbix
│
├── Images/
│   └── dashboard.png
│
└── README.md
🎯 Learning Outcomes
Through this project, I gained practical experience with:
Microsoft Fabric Workspace
Lakehouse architecture
Spark Notebooks
PySpark transformations
Delta Tables
Data Modeling
Power BI integration
End-to-end Data Engineering workflow
📷 Dashboard Preview
(Add a screenshot of your dashboard here.)
![Dashboard](Images/dashboard.png)
🚀 Future Improvements
Implement Medallion Architecture (Bronze, Silver, Gold)
Build incremental data pipelines
Add Data Factory pipelines
Automate notebook execution
Integrate Real-Time Analytics
Expand dashboard with advanced KPIs
👨‍💻 Author
Akash Nath
If you found this project helpful, feel free to ⭐ the repository and connect with me on LinkedIn.
