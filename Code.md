# 💻 Microsoft Fabric Notebook Code

This document contains the complete notebook code used to build the **E-Commerce Customer 360 Dashboard** using **Microsoft Fabric**.

---

# 📌 Project Workflow

1. Import Required Libraries
2. Read Parquet Files from Fabric Lakehouse
3. Store Raw Data as Delta Tables
4. Clean Customer Data
5. Clean Orders Data
6. Clean Payments Data
7. Clean Support Ticket Data
8. Clean Web Activity Data
9. Load Silver Tables
10. Build Customer 360 Gold Layer
11. Preview Final Dataset

---

# 1️⃣ Import Required Libraries

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
```

---

# 2️⃣ Read Parquet Files from Microsoft Fabric Lakehouse

```python
path = "abfss://your file path/"

customers_raw = spark.read.parquet(path + "customers.parquet")
orders_raw = spark.read.parquet(path + "orders.parquet")
payments_raw = spark.read.parquet(path + "payments.parquet")
support_raw = spark.read.parquet(path + "support_tickets.parquet")
web_raw = spark.read.parquet(path + "web_activities.parquet")
```

---

# 3️⃣ Store Raw Data as Delta Tables

```python
customers_raw.write.format("delta").mode("overwrite").saveAsTable("customers")

orders_raw.write.format("delta").mode("overwrite").saveAsTable("orders")

payments_raw.write.format("delta").mode("overwrite").saveAsTable("payments")

support_raw.write.format("delta").mode("overwrite").saveAsTable("support_tickets")

web_raw.write.format("delta").mode("overwrite").saveAsTable("web_activities")
```

---

# 4️⃣ Customer Data Cleaning

```python
customers_clean = (
    customers_raw
    .withColumn("email", lower(trim(col("EMAIL"))))
    .withColumn("name", initcap(trim(col("name"))))
    .withColumn(
        "gender",
        when(lower(col("gender")).isin("f", "female"), "Female")
        .when(lower(col("gender")).isin("m", "male"), "Male")
        .otherwise("Other")
    )
    .withColumn(
        "dob",
        to_date(regexp_replace(col("dob"), "/", "-"))
    )
    .withColumn("location", initcap(col("location")))
    .dropDuplicates(["customer_id"])
    .dropna(subset=["customer_id", "email"])
)

customers_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_customers")
```

---

# 5️⃣ Orders Data Cleaning

```python
orders_clean = (
    orders_raw
    .withColumn(
        "order_date",
        when(col("order_date").rlike("^\\d{4}/\\d{2}/\\d{2}$"),
             to_date(col("order_date"), "yyyy/MM/dd"))
        .when(col("order_date").rlike("^\\d{2}-\\d{2}-\\d{4}$"),
              to_date(col("order_date"), "dd-MM-yyyy"))
        .when(col("order_date").rlike("^\\d{8}$"),
              to_date(col("order_date"), "yyyyMMdd"))
        .otherwise(
            to_date(col("order_date"), "yyyy-MM-dd")
        )
    )
    .withColumn("amount", col("amount").cast(DoubleType()))
    .withColumn(
        "amount",
        when(col("amount") < 0, None).otherwise(col("amount"))
    )
    .withColumn("status", initcap(col("status")))
    .dropna(subset=["customer_id", "order_date"])
    .dropDuplicates(["order_id"])
)

orders_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_orders")
```

---

# 6️⃣ Payments Data Cleaning

```python
payments_clean = (
    payments_raw
    .withColumn(
        "payment_date",
        to_date(regexp_replace(col("payment_date"), "/", "-"))
    )
    .withColumn(
        "payment_method",
        initcap(col("payment_method"))
    )
    .replace(
        {"creditcard": "Credit Card"},
        subset=["payment_method"]
    )
    .withColumn(
        "payment_status",
        initcap(col("payment_status"))
    )
    .withColumn(
        "amount",
        col("amount").cast(DoubleType())
    )
    .withColumn(
        "amount",
        when(col("amount") < 0, None).otherwise(col("amount"))
    )
    .dropna(
        subset=["customer_id", "payment_date", "amount"]
    )
)

payments_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_payments")
```

---

# 7️⃣ Support Tickets Data Cleaning

```python
support_clean = (
    support_raw
    .withColumn(
        "ticket_date",
        to_date(regexp_replace(col("ticket_date"), "/", "-"))
    )
    .withColumn(
        "issue_type",
        initcap(trim(col("issue_type")))
    )
    .withColumn(
        "resolution_status",
        initcap(trim(col("resolution_status")))
    )
    .replace(
        {"NA": None, "": None},
        subset=["issue_type", "resolution_status"]
    )
    .dropDuplicates(["ticket_id"])
    .dropna(subset=["customer_id", "ticket_date"])
)

support_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_support")
```

---

# 8️⃣ Web Activity Data Cleaning

```python
web_clean = (
    web_raw
    .withColumn(
        "session_time",
        to_date(regexp_replace(col("session_time"), "/", "-"))
    )
    .withColumn(
        "page_viewed",
        lower(col("page_viewed"))
    )
    .withColumn(
        "device_type",
        initcap(col("device_type"))
    )
    .dropDuplicates(["session_id"])
    .dropna(
        subset=["customer_id", "session_time", "page_viewed"]
    )
)

web_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_web")
```

---

# 9️⃣ Load Silver Tables

```python
cust = spark.table("silver_customers").alias("c")

orders = spark.table("silver_orders").alias("o")

payments = spark.table("silver_payments").alias("p")

support = spark.table("silver_support").alias("s")

web = spark.table("silver_web").alias("w")
```

---

# 🔟 Build Customer 360 Gold Layer

```python
customer360 = (
    cust
    .join(orders, "customer_id", "left")
    .join(payments, "customer_id", "left")
    .join(support, "customer_id", "left")
    .join(web, "customer_id", "left")
    .select(
        col("c.customer_id"),
        col("c.name"),
        col("c.email"),
        col("c.gender"),
        col("c.dob"),
        col("c.location"),

        col("o.order_id"),
        col("o.order_date"),
        col("o.amount").alias("order_amount"),
        col("o.status"),

        col("p.payment_method"),
        col("p.payment_status"),
        col("p.amount").alias("payment_amount"),

        col("s.ticket_id"),
        col("s.issue_type"),
        col("s.ticket_date"),
        col("s.resolution_status"),

        col("w.page_viewed"),
        col("w.device_type"),
        col("w.session_time")
    )
)

customer360.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_customer360")
```

---

# 1️⃣1️⃣ Preview Final Dataset

```python
display(customer360.limit(5))
```

---

# 📊 Project Summary

✔ Read raw Parquet files from Microsoft Fabric Lakehouse

✔ Created Delta Tables for optimized storage

✔ Cleaned and transformed data using PySpark

✔ Built Silver Layer tables

✔ Integrated multiple datasets into a Customer 360 Gold Layer

✔ Designed an interactive Power BI dashboard for business insights

---

## 🛠️ Tech Stack

- Microsoft Fabric
- Fabric Lakehouse
- PySpark
- Delta Lake
- Power BI
- Parquet Files
- Customer 360 Data Model
