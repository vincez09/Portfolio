# Portfolio
[Data Cleaning](notebook/global_retailer.ipynb)
```python
import subprocess
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

path = "C:/Users/Vincent/Documents/Project Portfolio/Global_retailer_dataset/"
database = path + 'database.sqlite'
```


```python
conn = sqlite3.connect(database)
c = conn.cursor()

tables_to_create = ['Customers', 'Exchange_Rates', 'Products', 'Sales', 'Stores', 'Data_Dictionary']

for table in tables_to_create:
    # Drop the table if it exists
    csv_file = path + f"{table}.csv"
    df = pd.read_csv(csv_file, encoding='latin1')
    try:
        c.execute(f"DROP TABLE IF EXISTS {table}")
        df.to_sql(f"{table}", conn, if_exists='replace', index=False)
        conn.commit()
    except sqlite3.OperationalError as e:
        print(f"Error: {e}")
        conn.rollback()
        

# Verify the table creation by listing all tables in the database
tables = pd.read_sql("SELECT * FROM sqlite_master WHERE type='table';", conn)
print(tables)


```

        type             name         tbl_name  rootpage  \
    0  table        Customers        Customers         2   
    1  table   Exchange_Rates   Exchange_Rates         5   
    2  table         Products         Products         7   
    3  table            Sales            Sales         3   
    4  table           Stores           Stores        13   
    5  table  Data_Dictionary  Data_Dictionary        14   
    
                                                     sql  
    0  CREATE TABLE "Customers" (\n"CustomerKey" INTE...  
    1  CREATE TABLE "Exchange_Rates" (\n"Date" TEXT,\...  
    2  CREATE TABLE "Products" (\n"ProductKey" INTEGE...  
    3  CREATE TABLE "Sales" (\n"Order Number" INTEGER...  
    4  CREATE TABLE "Stores" (\n"StoreKey" INTEGER,\n...  
    5  CREATE TABLE "Data_Dictionary" (\n"Table" TEXT...  
    


```python
# Convert column "Unit Price USD" to Float and remove '$'
c.execute("ALTER TABLE Products ADD COLUMN Unit_Price FLOAT")
c.execute('UPDATE Products SET Unit_Price = SUBSTR("Unit Price USD", 2)')
c.execute('ALTER TABLE Products DROP COLUMN "Unit Price USD"')
conn.commit()
```


```python
# Convert column "Unit Cost USD" to Float and remove '$'
c.execute("ALTER TABLE Products ADD COLUMN Unit_Cost FLOAT")
c.execute('UPDATE Products SET Unit_Cost = SUBSTR("Unit Cost USD", 2)')
c.execute('ALTER TABLE Products DROP COLUMN "Unit Cost USD"')
conn.commit()
```


```python
# Checking null values in Products Table

pd.read_sql('''
SELECT *
FROM Products
WHERE
    ProductKey IS NULL OR
    "Product Name" IS NULL OR
    Brand IS NULL OR
    Color IS NULL OR
    SubcategoryKey IS NULL OR
    CategoryKey IS NULL OR
    Category IS NULL OR
    Unit_Price IS NULL OR
    Unit_Cost IS NULL;''', conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ProductKey</th>
      <th>Product Name</th>
      <th>Brand</th>
      <th>Color</th>
      <th>SubcategoryKey</th>
      <th>Subcategory</th>
      <th>CategoryKey</th>
      <th>Category</th>
      <th>Unit_Price</th>
      <th>Unit_Cost</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>




```python
# Checking duplicate rows in Products Table

pd.read_sql('''
SELECT
    productkey,
    "product name",
    brand,
    color,
    subcategorykey,
    subcategory,
    categorykey,
    category,
    unit_price,
    unit_cost,
    COUNT(*) AS Count
FROM Products
GROUP BY
    productkey,
    "product name",
    brand,
    color,
    subcategorykey,
    subcategory,
    categorykey,
    category,
    unit_price,
    unit_cost
HAVING COUNT(*) > 1;''', conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ProductKey</th>
      <th>Product Name</th>
      <th>Brand</th>
      <th>Color</th>
      <th>SubcategoryKey</th>
      <th>Subcategory</th>
      <th>CategoryKey</th>
      <th>Category</th>
      <th>Unit_Price</th>
      <th>Unit_Cost</th>
      <th>Count</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>




```python
# Clean Customers Table

# Convert Birthday data type to Date

c.execute('ALTER TABLE Customers ADD COLUMN Birthdate DATE')
c.execute('''
UPDATE Customers
SET Birthdate = (
    CASE
            WHEN LENGTH(Birthday) = 8 THEN SUBSTR(Birthday, 5) || '-0' || SUBSTR(Birthday, 1, 1) || '-0' || SUBSTR(Birthday, 3, 1)
            WHEN LENGTH(Birthday) = 9 THEN 
                CASE
                    WHEN SUBSTR(Birthday, 2, 1) = '/' THEN SUBSTR(Birthday, 6) || '-0' || SUBSTR(Birthday, 1, 1) || '-' || SUBSTR(Birthday, 3, 2)
                    ELSE SUBSTR(Birthday, 6) || '-' || SUBSTR(Birthday, 1, 2) || '-0' || SUBSTR(Birthday, 4, 1)
                END
            WHEN LENGTH(Birthday) = 10 THEN SUBSTR(Birthday, 7) || '-' || SUBSTR(Birthday, 1, 2) || '-' || SUBSTR(Birthday, 4, 2)
        END)''')
c.execute('ALTER TABLE Customers DROP COLUMN Birthday')
conn.commit()
```


```python
# Checking Null Values in Customers Table

pd.read_sql('''
SELECT *
FROM Customers
WHERE CustomerKey IS NULL OR
    Gender IS NULL OR
    Name IS NULL OR
    City IS NULL OR
    'State Code' IS NULL OR
    State IS NULL OR
    'Zip Code' IS NULL OR
    Country IS NULL OR
    Continent IS NULL OR
    Birthdate IS NULL;''', conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>CustomerKey</th>
      <th>Gender</th>
      <th>Name</th>
      <th>City</th>
      <th>State Code</th>
      <th>State</th>
      <th>Zip Code</th>
      <th>Country</th>
      <th>Continent</th>
      <th>Birthdate</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>




```python
# Checking Duplicate Rows in Customers Table

pd.read_sql('''
SELECT 
    customerkey,
    gender,
    name,
    city,
    "state code",
    state,
    "zip code",
    country,
    continent,
    birthdate,
    COUNT(*) AS Count
FROM Customers
GROUP BY
    customerkey,
    gender,
    name,
    city,
    "state code",
    state,
    "zip code",
    country,
    continent,
    birthdate
HAVING COUNT(*) > 1;''', conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>CustomerKey</th>
      <th>Gender</th>
      <th>Name</th>
      <th>City</th>
      <th>State Code</th>
      <th>State</th>
      <th>Zip Code</th>
      <th>Country</th>
      <th>Continent</th>
      <th>Birthdate</th>
      <th>Count</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>




```python
# Convert Order Date data type to Date in Sales Table

c.execute('ALTER TABLE Sales ADD COLUMN Order_Date DATE')
c.execute('''
UPDATE Sales
SET Order_Date = (
    CASE
            WHEN LENGTH("Order Date") = 8 THEN SUBSTR("Order Date", 5) || '-0' || SUBSTR("Order Date", 1, 1) || '-0' || SUBSTR("Order Date", 3, 1)
            WHEN LENGTH("Order Date") = 9 THEN 
                CASE
                    WHEN SUBSTR("Order Date", 2, 1) = '/' THEN SUBSTR("Order Date", 6) || '-0' || SUBSTR("Order Date", 1, 1) || '-' || SUBSTR("Order Date", 3, 2)
                    ELSE SUBSTR("Order Date", 6) || '-' || SUBSTR("Order Date", 1, 2) || '-0' || SUBSTR("Order Date", 4, 1)
                END
            WHEN LENGTH("Order Date") = 10 THEN SUBSTR("Order Date", 7) || '-' || SUBSTR("Order Date", 1, 2) || '-' || SUBSTR("Order Date", 4, 2)
        END)''')
c.execute('ALTER TABLE Sales DROP COLUMN "Order Date"')
conn.commit()
```


```python
# Convert Delivery Date data type to Date in Sales Table

c.execute('ALTER TABLE Sales ADD COLUMN Delivery_Date DATE')
c.execute('''
UPDATE Sales
SET Delivery_Date = (
    CASE
            WHEN LENGTH("Delivery Date") = 8 THEN SUBSTR("Delivery Date", 5) || '-0' || SUBSTR("Delivery Date", 1, 1) || '-0' || SUBSTR("Delivery Date", 3, 1)
            WHEN LENGTH("Delivery Date") = 9 THEN 
                CASE
                    WHEN SUBSTR("Delivery Date", 2, 1) = '/' THEN SUBSTR("Delivery Date", 6) || '-0' || SUBSTR("Delivery Date", 1, 1) || '-' || SUBSTR("Delivery Date", 3, 2)
                    ELSE SUBSTR("Delivery Date", 6) || '-' || SUBSTR("Delivery Date", 1, 2) || '-0' || SUBSTR("Delivery Date", 4, 1)
                END
            WHEN LENGTH("Delivery Date") = 10 THEN SUBSTR("Delivery Date", 7) || '-' || SUBSTR("Delivery Date", 1, 2) || '-' || SUBSTR("Delivery Date", 4, 2)
        END)''')
c.execute('ALTER TABLE Sales DROP COLUMN "Delivery Date"')
conn.commit()
```


```python
# Checking Null Values in Sales Table

pd.read_sql('''
SELECT *
FROM Sales
WHERE
    ("order number" IS NULL OR
    "line item" IS NULL OR
    order_date IS NULL OR
    delivery_date IS NULL OR
    customerkey IS NULL OR
    storekey IS NULL OR 
    productkey IS NULL OR
    quantity IS NULL OR
    "currency code" IS NULL) AND
    storekey != 0;''', conn)

# Based on the results, delivery date is null if it was bought on store or storekey != 0 while storekey = 10 is an online
# store where delivery of product has to be made.
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Order Number</th>
      <th>Line Item</th>
      <th>CustomerKey</th>
      <th>StoreKey</th>
      <th>ProductKey</th>
      <th>Quantity</th>
      <th>Currency Code</th>
      <th>Order_Date</th>
      <th>Delivery_Date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>366000</td>
      <td>1</td>
      <td>265598</td>
      <td>10</td>
      <td>1304</td>
      <td>1</td>
      <td>CAD</td>
      <td>2016-01-01</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>366004</td>
      <td>1</td>
      <td>1107461</td>
      <td>38</td>
      <td>163</td>
      <td>6</td>
      <td>GBP</td>
      <td>2016-01-01</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>366004</td>
      <td>2</td>
      <td>1107461</td>
      <td>38</td>
      <td>1529</td>
      <td>2</td>
      <td>GBP</td>
      <td>2016-01-01</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>366005</td>
      <td>1</td>
      <td>844003</td>
      <td>33</td>
      <td>421</td>
      <td>4</td>
      <td>EUR</td>
      <td>2016-01-01</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>366007</td>
      <td>1</td>
      <td>2035771</td>
      <td>43</td>
      <td>1617</td>
      <td>1</td>
      <td>USD</td>
      <td>2016-01-01</td>
      <td>None</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>49714</th>
      <td>2243025</td>
      <td>1</td>
      <td>1909290</td>
      <td>49</td>
      <td>1128</td>
      <td>2</td>
      <td>USD</td>
      <td>2021-02-20</td>
      <td>None</td>
    </tr>
    <tr>
      <th>49715</th>
      <td>2243025</td>
      <td>2</td>
      <td>1909290</td>
      <td>49</td>
      <td>2511</td>
      <td>2</td>
      <td>USD</td>
      <td>2021-02-20</td>
      <td>None</td>
    </tr>
    <tr>
      <th>49716</th>
      <td>2243026</td>
      <td>1</td>
      <td>1737466</td>
      <td>49</td>
      <td>58</td>
      <td>6</td>
      <td>USD</td>
      <td>2021-02-20</td>
      <td>None</td>
    </tr>
    <tr>
      <th>49717</th>
      <td>2243028</td>
      <td>1</td>
      <td>1728060</td>
      <td>66</td>
      <td>1584</td>
      <td>3</td>
      <td>USD</td>
      <td>2021-02-20</td>
      <td>None</td>
    </tr>
    <tr>
      <th>49718</th>
      <td>2243030</td>
      <td>1</td>
      <td>1216913</td>
      <td>43</td>
      <td>632</td>
      <td>3</td>
      <td>USD</td>
      <td>2021-02-20</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
<p>49719 rows × 9 columns</p>
</div>




```python
# Checking Duplciate rows in Sales Table
pd.read_sql('''
SELECT
    "order number",
    "line item",
    order_date,
    delivery_date,
    customerkey,
    storekey,
    productkey,
    quantity,
    "currency code",
    COUNT(*) AS Count
FROM Sales
GROUP BY 
    "order number",
    "line item",
    order_date,
    delivery_date,
    customerkey,
    storekey,
    productkey,
    quantity,
    "currency code"
HAVING COUNT(*) > 1;''', conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Order Number</th>
      <th>Line Item</th>
      <th>Order_Date</th>
      <th>Delivery_Date</th>
      <th>CustomerKey</th>
      <th>StoreKey</th>
      <th>ProductKey</th>
      <th>Quantity</th>
      <th>Currency Code</th>
      <th>Count</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>




```python
# Convert Open Date data type to Date

c.execute('ALTER TABLE Stores ADD COLUMN Open_Date DATE')
c.execute('''
UPDATE Stores
SET Open_Date = (
    CASE
            WHEN LENGTH("Open Date") = 8 THEN SUBSTR("Open Date", 5) || '-0' || SUBSTR("Open Date", 1, 1) || '-0' || SUBSTR("Open Date", 3, 1)
            WHEN LENGTH("Open Date") = 9 THEN 
                CASE
                    WHEN SUBSTR("Open Date", 2, 1) = '/' THEN SUBSTR("Open Date", 6) || '-0' || SUBSTR("Open Date", 1, 1) || '-' || SUBSTR("Open Date", 3, 2)
                    ELSE SUBSTR("Open Date", 6) || '-' || SUBSTR("Open Date", 1, 2) || '-0' || SUBSTR("Open Date", 4, 1)
                END
            WHEN LENGTH("Open Date") = 10 THEN SUBSTR("Open Date", 7) || '-' || SUBSTR("Open Date", 1, 2) || '-' || SUBSTR("Open Date", 4, 2)
        END)''')
c.execute('ALTER TABLE Stores DROP COLUMN "Open Date"')
conn.commit()
```


```python
# Checking Duplicate rows on Stores Table

pd.read_sql('''
SELECT
    storekey,
    country,
    state,
    "square meters",
    open_date,
    COUNT(*) AS Count
FROM Stores
GROUP BY
    storekey,
    country,
    state,
    "square meters",
    open_date 
HAVING Count(*) > 1;''', conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>StoreKey</th>
      <th>Country</th>
      <th>State</th>
      <th>Square Meters</th>
      <th>Open_Date</th>
      <th>Count</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>




```python
# Update Data Dictionary for Column names that was changed
# Update Birthday to Birthdate
c.execute('''
UPDATE Data_Dictionary
SET Field = 'Birthdate'
WHERE Field = "Birthday"''')

# Update Unit Cost USD to Unit_Cost
# Update Unit Price USD to Unit_Price
c.execute('''
UPDATE Data_Dictionary
SET Field = 'Unit_Cost'
WHERE Field = "Unit Cost USD"''')
c.execute('''
UPDATE Data_Dictionary
SET Field = 'Unit_Price'
WHERE Field = "Unit Price USD"''')

# Update Open Date to Open_Date
c.execute('''
UPDATE Data_Dictionary
SET Field = 'Open_Date'
WHERE Field = "Open Date"''')

conn.commit()

```


```python
# Query 10 Most Profitable Products
top10_products_df = pd.read_sql('''
SELECT
    p.ProductKey,
    p."Product Name",
    SUM(s.Quantity) AS quantity_sold,
    SUM(s.Quantity * p.Unit_Price) AS total_sales,
    SUM(s.Quantity * p.Unit_Cost) AS total_cost_goods,
    SUM(s.Quantity * p.Unit_Price) - SUM(s.Quantity * p.Unit_Cost) AS total_profit
FROM Sales as s
JOIN Products as p
ON s.ProductKey = p.ProductKey
WHERE strftime('%Y', order_date) = (SELECT MAX(strftime('%Y', order_date)) FROM Sales)
GROUP BY p.ProductKey, p."Product Name"
ORDER BY total_profit DESC
LIMIT 10;''', conn)

top10_products_df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ProductKey</th>
      <th>Product Name</th>
      <th>quantity_sold</th>
      <th>total_sales</th>
      <th>total_cost_goods</th>
      <th>total_profit</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>416</td>
      <td>Adventure Works Desktop PC2.33 XD233 Silver</td>
      <td>28</td>
      <td>27132.0</td>
      <td>8989.40</td>
      <td>18142.60</td>
    </tr>
    <tr>
      <th>1</th>
      <td>455</td>
      <td>WWI Desktop PC2.33 X2330 White</td>
      <td>20</td>
      <td>18380.0</td>
      <td>6089.60</td>
      <td>12290.40</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1443</td>
      <td>The Phone Company Touch Screen Phone 1600 TFT-...</td>
      <td>28</td>
      <td>16492.0</td>
      <td>5464.20</td>
      <td>11027.80</td>
    </tr>
    <tr>
      <th>3</th>
      <td>438</td>
      <td>WWI Desktop PC2.33 X2330 Silver</td>
      <td>14</td>
      <td>12866.0</td>
      <td>4262.72</td>
      <td>8603.28</td>
    </tr>
    <tr>
      <th>4</th>
      <td>428</td>
      <td>Adventure Works Desktop PC2.33 XD233 Brown</td>
      <td>10</td>
      <td>9690.0</td>
      <td>3210.50</td>
      <td>6479.50</td>
    </tr>
    <tr>
      <th>5</th>
      <td>423</td>
      <td>Adventure Works Desktop PC2.30 MD230 Black</td>
      <td>19</td>
      <td>11381.0</td>
      <td>5233.74</td>
      <td>6147.26</td>
    </tr>
    <tr>
      <th>6</th>
      <td>433</td>
      <td>Adventure Works Desktop PC2.33 XD233 White</td>
      <td>9</td>
      <td>8721.0</td>
      <td>2889.45</td>
      <td>5831.55</td>
    </tr>
    <tr>
      <th>7</th>
      <td>542</td>
      <td>Proseware Projector 720p DLP56 Black</td>
      <td>10</td>
      <td>9990.0</td>
      <td>4594.00</td>
      <td>5396.00</td>
    </tr>
    <tr>
      <th>8</th>
      <td>1425</td>
      <td>The Phone Company Touch Screen Phone 1600 TFT-...</td>
      <td>15</td>
      <td>7935.0</td>
      <td>2629.05</td>
      <td>5305.95</td>
    </tr>
    <tr>
      <th>9</th>
      <td>444</td>
      <td>WWI Desktop PC2.33 X2330 Black</td>
      <td>8</td>
      <td>7352.0</td>
      <td>2435.84</td>
      <td>4916.16</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Plotting top 10 products profit
plt.figure(figsize=(10, 6))
bar_width = 0.6
plt.barh(top10_products_df['Product Name'], top10_products_df['total_profit'], color='royalblue', height = bar_width)
plt.xlabel('Total Profit')
plt.ylabel('Product Name')
plt.title('Top 10 Products by Profit')
plt.gca().invert_yaxis()  # Invert y-axis to have the highest profit on top
plt.show()
```


    
![png](output_17_0.png)
    



```python
# Top 10 Most Profitable Stores and where it is located
top10_stores_df = pd.read_sql('''
SELECT
    stores.storekey,
    stores.country,
    stores.state,
    SUM(sales.quantity * products.unit_price) AS total_sales,
    SUM(sales.quantity * products.unit_cost) AS total_cost_goods,
    SUM(sales.quantity * products.unit_price) - SUM(sales.quantity * products.unit_cost) AS total_profit
FROM Stores
JOIN Sales 
ON stores.storekey = sales.storekey
JOIN Products
ON sales.productkey = products.productkey
GROUP BY stores.storekey, stores.country, stores.state
ORDER BY total_profit DESC
LIMIT 10;''', conn)

top10_stores_df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>StoreKey</th>
      <th>Country</th>
      <th>State</th>
      <th>total_sales</th>
      <th>total_cost_goods</th>
      <th>total_profit</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>Online</td>
      <td>Online</td>
      <td>8941131.31</td>
      <td>4656427.37</td>
      <td>4284703.94</td>
    </tr>
    <tr>
      <th>1</th>
      <td>45</td>
      <td>United States</td>
      <td>Connecticut</td>
      <td>1080364.07</td>
      <td>551160.78</td>
      <td>529203.29</td>
    </tr>
    <tr>
      <th>2</th>
      <td>54</td>
      <td>United States</td>
      <td>Nebraska</td>
      <td>1074717.82</td>
      <td>560326.32</td>
      <td>514391.50</td>
    </tr>
    <tr>
      <th>3</th>
      <td>65</td>
      <td>United States</td>
      <td>West Virginia</td>
      <td>1019252.80</td>
      <td>507338.28</td>
      <td>511914.52</td>
    </tr>
    <tr>
      <th>4</th>
      <td>9</td>
      <td>Canada</td>
      <td>Northwest Territories</td>
      <td>1063240.26</td>
      <td>555037.19</td>
      <td>508203.07</td>
    </tr>
    <tr>
      <th>5</th>
      <td>50</td>
      <td>United States</td>
      <td>Kansas</td>
      <td>1058325.43</td>
      <td>565545.96</td>
      <td>492779.47</td>
    </tr>
    <tr>
      <th>6</th>
      <td>55</td>
      <td>United States</td>
      <td>Nevada</td>
      <td>1056804.46</td>
      <td>576741.46</td>
      <td>480063.00</td>
    </tr>
    <tr>
      <th>7</th>
      <td>51</td>
      <td>United States</td>
      <td>Maine</td>
      <td>937169.36</td>
      <td>467588.36</td>
      <td>469581.00</td>
    </tr>
    <tr>
      <th>8</th>
      <td>8</td>
      <td>Canada</td>
      <td>Newfoundland and Labrador</td>
      <td>960051.82</td>
      <td>491137.98</td>
      <td>468913.84</td>
    </tr>
    <tr>
      <th>9</th>
      <td>61</td>
      <td>United States</td>
      <td>South Carolina</td>
      <td>996910.36</td>
      <td>531154.25</td>
      <td>465756.11</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Plotting top 10 most profitable stores location by state
plt.figure(figsize=(10, 6))
bar_width = 0.6
plt.barh(top10_stores_df['State'], top10_stores_df['total_profit'], color='royalblue', height = bar_width)
plt.xlabel('Total Profit')
plt.ylabel('Store (State)')
plt.title('Top 10 Most Profitable Stores by State')
plt.gca().invert_yaxis()
plt.gca().xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x/1000)}k'))
plt.show()
```


    
![png](output_19_0.png)
    



```python
pd.read_sql('''
SELECT storekey, COUNT(*)
FROM Stores
GROUP BY storekey''', conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>StoreKey</th>
      <th>COUNT(*)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>1</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>62</th>
      <td>62</td>
      <td>1</td>
    </tr>
    <tr>
      <th>63</th>
      <td>63</td>
      <td>1</td>
    </tr>
    <tr>
      <th>64</th>
      <td>64</td>
      <td>1</td>
    </tr>
    <tr>
      <th>65</th>
      <td>65</td>
      <td>1</td>
    </tr>
    <tr>
      <th>66</th>
      <td>66</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
<p>67 rows × 2 columns</p>
</div>




```python
# Query sales per Country and number of stores
pd.read_sql('''
SELECT
    stores.country,
    stores.storekey,
    SUM(sales.quantity * products.unit_price) AS total_sales,
    SUM(sales.quantity * products.unit_cost) AS total_cost_goods,
    SUM(sales.quantity * products.unit_price) - SUM(sales.quantity * products.unit_cost) AS total_profit
FROM Stores
JOIN Sales 
ON stores.storekey = sales.storekey
JOIN Products
ON sales.productkey = products.productkey
GROUP BY stores.country, stores.storekey
ORDER BY stores.country;''', conn)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Country</th>
      <th>StoreKey</th>
      <th>total_sales</th>
      <th>total_cost_goods</th>
      <th>total_profit</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Australia</td>
      <td>1</td>
      <td>174144.13</td>
      <td>96378.19</td>
      <td>77765.94</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Australia</td>
      <td>2</td>
      <td>9380.01</td>
      <td>5663.38</td>
      <td>3716.63</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Australia</td>
      <td>4</td>
      <td>274023.17</td>
      <td>176103.37</td>
      <td>97919.80</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Australia</td>
      <td>5</td>
      <td>600869.97</td>
      <td>329595.52</td>
      <td>271274.45</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Australia</td>
      <td>6</td>
      <td>434569.23</td>
      <td>220231.02</td>
      <td>214338.21</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Canada</td>
      <td>8</td>
      <td>960051.82</td>
      <td>491137.98</td>
      <td>468913.84</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Canada</td>
      <td>9</td>
      <td>1063240.26</td>
      <td>555037.19</td>
      <td>508203.07</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Canada</td>
      <td>10</td>
      <td>834562.61</td>
      <td>432649.72</td>
      <td>401912.89</td>
    </tr>
    <tr>
      <th>8</th>
      <td>France</td>
      <td>12</td>
      <td>152756.24</td>
      <td>76006.51</td>
      <td>76749.73</td>
    </tr>
    <tr>
      <th>9</th>
      <td>France</td>
      <td>13</td>
      <td>122832.20</td>
      <td>61967.47</td>
      <td>60864.73</td>
    </tr>
    <tr>
      <th>10</th>
      <td>France</td>
      <td>14</td>
      <td>91653.06</td>
      <td>42122.50</td>
      <td>49530.56</td>
    </tr>
    <tr>
      <th>11</th>
      <td>France</td>
      <td>15</td>
      <td>168467.47</td>
      <td>87021.98</td>
      <td>81445.49</td>
    </tr>
    <tr>
      <th>12</th>
      <td>France</td>
      <td>16</td>
      <td>149789.80</td>
      <td>74857.01</td>
      <td>74932.79</td>
    </tr>
    <tr>
      <th>13</th>
      <td>France</td>
      <td>17</td>
      <td>137518.50</td>
      <td>67157.85</td>
      <td>70360.65</td>
    </tr>
    <tr>
      <th>14</th>
      <td>France</td>
      <td>18</td>
      <td>171983.72</td>
      <td>91001.09</td>
      <td>80982.63</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Germany</td>
      <td>19</td>
      <td>440226.86</td>
      <td>233836.39</td>
      <td>206390.47</td>
    </tr>
    <tr>
      <th>16</th>
      <td>Germany</td>
      <td>20</td>
      <td>282046.38</td>
      <td>157064.95</td>
      <td>124981.43</td>
    </tr>
    <tr>
      <th>17</th>
      <td>Germany</td>
      <td>21</td>
      <td>305732.50</td>
      <td>164285.70</td>
      <td>141446.80</td>
    </tr>
    <tr>
      <th>18</th>
      <td>Germany</td>
      <td>22</td>
      <td>515912.98</td>
      <td>270563.22</td>
      <td>245349.76</td>
    </tr>
    <tr>
      <th>19</th>
      <td>Germany</td>
      <td>23</td>
      <td>462545.32</td>
      <td>251582.61</td>
      <td>210962.71</td>
    </tr>
    <tr>
      <th>20</th>
      <td>Germany</td>
      <td>24</td>
      <td>470499.91</td>
      <td>277075.98</td>
      <td>193423.93</td>
    </tr>
    <tr>
      <th>21</th>
      <td>Germany</td>
      <td>26</td>
      <td>233853.62</td>
      <td>115087.19</td>
      <td>118766.43</td>
    </tr>
    <tr>
      <th>22</th>
      <td>Germany</td>
      <td>27</td>
      <td>457761.29</td>
      <td>245294.31</td>
      <td>212466.98</td>
    </tr>
    <tr>
      <th>23</th>
      <td>Italy</td>
      <td>28</td>
      <td>156524.62</td>
      <td>75823.36</td>
      <td>80701.26</td>
    </tr>
    <tr>
      <th>24</th>
      <td>Italy</td>
      <td>29</td>
      <td>688468.68</td>
      <td>372136.63</td>
      <td>316332.05</td>
    </tr>
    <tr>
      <th>25</th>
      <td>Italy</td>
      <td>30</td>
      <td>716001.08</td>
      <td>387203.83</td>
      <td>328797.25</td>
    </tr>
    <tr>
      <th>26</th>
      <td>Netherlands</td>
      <td>31</td>
      <td>307941.76</td>
      <td>149242.04</td>
      <td>158699.72</td>
    </tr>
    <tr>
      <th>27</th>
      <td>Netherlands</td>
      <td>32</td>
      <td>329456.60</td>
      <td>160380.85</td>
      <td>169075.75</td>
    </tr>
    <tr>
      <th>28</th>
      <td>Netherlands</td>
      <td>33</td>
      <td>319828.88</td>
      <td>169307.27</td>
      <td>150521.61</td>
    </tr>
    <tr>
      <th>29</th>
      <td>Netherlands</td>
      <td>34</td>
      <td>326303.95</td>
      <td>164056.76</td>
      <td>162247.19</td>
    </tr>
    <tr>
      <th>30</th>
      <td>Online</td>
      <td>0</td>
      <td>8941131.31</td>
      <td>4656427.37</td>
      <td>4284703.94</td>
    </tr>
    <tr>
      <th>31</th>
      <td>United Kingdom</td>
      <td>36</td>
      <td>716685.59</td>
      <td>345349.57</td>
      <td>371336.02</td>
    </tr>
    <tr>
      <th>32</th>
      <td>United Kingdom</td>
      <td>37</td>
      <td>665957.08</td>
      <td>338498.62</td>
      <td>327458.46</td>
    </tr>
    <tr>
      <th>33</th>
      <td>United Kingdom</td>
      <td>38</td>
      <td>729741.87</td>
      <td>396399.05</td>
      <td>333342.82</td>
    </tr>
    <tr>
      <th>34</th>
      <td>United Kingdom</td>
      <td>39</td>
      <td>681084.17</td>
      <td>366261.12</td>
      <td>314823.05</td>
    </tr>
    <tr>
      <th>35</th>
      <td>United Kingdom</td>
      <td>40</td>
      <td>734403.96</td>
      <td>378540.05</td>
      <td>355863.91</td>
    </tr>
    <tr>
      <th>36</th>
      <td>United Kingdom</td>
      <td>41</td>
      <td>300785.85</td>
      <td>161556.30</td>
      <td>139229.55</td>
    </tr>
    <tr>
      <th>37</th>
      <td>United Kingdom</td>
      <td>42</td>
      <td>725675.13</td>
      <td>365059.07</td>
      <td>360616.06</td>
    </tr>
    <tr>
      <th>38</th>
      <td>United States</td>
      <td>43</td>
      <td>928876.45</td>
      <td>500688.38</td>
      <td>428188.07</td>
    </tr>
    <tr>
      <th>39</th>
      <td>United States</td>
      <td>44</td>
      <td>940862.65</td>
      <td>502677.13</td>
      <td>438185.52</td>
    </tr>
    <tr>
      <th>40</th>
      <td>United States</td>
      <td>45</td>
      <td>1080364.07</td>
      <td>551160.78</td>
      <td>529203.29</td>
    </tr>
    <tr>
      <th>41</th>
      <td>United States</td>
      <td>47</td>
      <td>908256.73</td>
      <td>469789.14</td>
      <td>438467.59</td>
    </tr>
    <tr>
      <th>42</th>
      <td>United States</td>
      <td>48</td>
      <td>847302.98</td>
      <td>421191.22</td>
      <td>426111.76</td>
    </tr>
    <tr>
      <th>43</th>
      <td>United States</td>
      <td>49</td>
      <td>761488.70</td>
      <td>389073.56</td>
      <td>372415.14</td>
    </tr>
    <tr>
      <th>44</th>
      <td>United States</td>
      <td>50</td>
      <td>1058325.43</td>
      <td>565545.96</td>
      <td>492779.47</td>
    </tr>
    <tr>
      <th>45</th>
      <td>United States</td>
      <td>51</td>
      <td>937169.36</td>
      <td>467588.36</td>
      <td>469581.00</td>
    </tr>
    <tr>
      <th>46</th>
      <td>United States</td>
      <td>53</td>
      <td>869709.26</td>
      <td>483389.29</td>
      <td>386319.97</td>
    </tr>
    <tr>
      <th>47</th>
      <td>United States</td>
      <td>54</td>
      <td>1074717.82</td>
      <td>560326.32</td>
      <td>514391.50</td>
    </tr>
    <tr>
      <th>48</th>
      <td>United States</td>
      <td>55</td>
      <td>1056804.46</td>
      <td>576741.46</td>
      <td>480063.00</td>
    </tr>
    <tr>
      <th>49</th>
      <td>United States</td>
      <td>56</td>
      <td>916494.68</td>
      <td>490089.15</td>
      <td>426405.53</td>
    </tr>
    <tr>
      <th>50</th>
      <td>United States</td>
      <td>57</td>
      <td>966825.24</td>
      <td>515035.63</td>
      <td>451789.61</td>
    </tr>
    <tr>
      <th>51</th>
      <td>United States</td>
      <td>59</td>
      <td>976799.81</td>
      <td>545451.68</td>
      <td>431348.13</td>
    </tr>
    <tr>
      <th>52</th>
      <td>United States</td>
      <td>61</td>
      <td>996910.36</td>
      <td>531154.25</td>
      <td>465756.11</td>
    </tr>
    <tr>
      <th>53</th>
      <td>United States</td>
      <td>62</td>
      <td>509374.46</td>
      <td>271380.65</td>
      <td>237993.81</td>
    </tr>
    <tr>
      <th>54</th>
      <td>United States</td>
      <td>63</td>
      <td>627175.71</td>
      <td>331395.17</td>
      <td>295780.54</td>
    </tr>
    <tr>
      <th>55</th>
      <td>United States</td>
      <td>64</td>
      <td>950318.56</td>
      <td>496060.25</td>
      <td>454258.31</td>
    </tr>
    <tr>
      <th>56</th>
      <td>United States</td>
      <td>65</td>
      <td>1019252.80</td>
      <td>507338.28</td>
      <td>511914.52</td>
    </tr>
    <tr>
      <th>57</th>
      <td>United States</td>
      <td>66</td>
      <td>930888.06</td>
      <td>480826.65</td>
      <td>450061.41</td>
    </tr>
  </tbody>
</table>
</div>



## Aggregate Table for Dashboard Generation


```python
# Get Total Sales, Total Profit, Previous Sales per Year per Country

total_sales_df = pd.read_sql('''
WITH total_sales AS (SELECT
    strftime('%Y-%m', order_date) AS Year_month,
    stores.country,
    SUM(sales.quantity * products.unit_price) AS total_sales,
    SUM(sales.quantity * products.unit_price) - SUM(sales.quantity * products.unit_cost) AS total_profit
FROM Sales 
JOIN Products
ON sales.productkey = products.productkey
JOIN Stores
ON stores.storekey = sales.storekey
GROUP BY strftime('%Y-%m', order_date), stores.country
ORDER BY strftime('%Y', order_date), strftime('%m', order_date)
),
previous_sales AS (SELECT
    year_month,
    country,
    total_sales,
    CASE 
        WHEN LAG(total_sales,1) OVER (PARTITION BY country ORDER BY SUBSTR(year_month, 1, 4)) IS NULL THEN 0
        ELSE LAG(total_sales,1) OVER (PARTITION BY country ORDER BY SUBSTR(year_month, 1, 4)) END AS prev_sales,
    total_profit,
    CASE 
        WHEN LAG(total_profit,1) OVER (PARTITION BY country ORDER BY SUBSTR(year_month, 1, 4)) IS NULL THEN 0
        ELSE LAG(total_profit,1) OVER (PARTITION BY country ORDER BY SUBSTR(year_month, 1, 4)) END AS prev_profit
FROM total_sales)

SELECT
    year_month,
    country,
    total_sales,
    prev_sales,
    total_profit,
    prev_profit
FROM previous_sales;''', conn)
```


```python
# Most Profitable and Quantites Sold Product Category per year

product_sales_df = pd.read_sql('''
SELECT
    strftime('%Y-%m', order_date) AS Year_month,
    p."Product Name",
    p.Category,
    p.Subcategory,
    SUM(s.Quantity) AS quantity_sold,
    SUM(s.Quantity * p.Unit_Price) AS total_sales,
    SUM(s.Quantity * p.Unit_Cost) AS total_cost_of_goods,
    SUM(s.Quantity * p.Unit_Price) - SUM(s.Quantity * p.Unit_Cost) AS total_profit
FROM Sales as s
JOIN Products as p
ON s.ProductKey = p.ProductKey
GROUP BY strftime('%Y-%m', order_date), p."Product Name"
ORDER BY strftime('%Y', order_date), strftime('%m', order_date);''', conn)
```


```python
# New Customers per Year

new_customers_df = pd.read_sql('''
WITH first_buy AS (
SELECT
    c.customerkey,
    MIN(s.order_date) AS first_buy_date
FROM Customers as c
JOIN Sales as s
ON c.customerkey = s.customerkey
GROUP BY c.customerkey)

SELECT
    strftime('%Y-%m', first_buy_date) AS Year_month,
    COUNT(CustomerKey) AS New_customers
FROM first_buy
GROUP BY strftime('%Y-%m', first_buy_date);''', conn)
```


```python
# Get Customer Age Distribution

age_df = pd.read_sql('''
WITH age_table AS (SELECT
    s.customerkey,
    s.order_date,
    c.birthdate,
    CAST(strftime('%Y', s.order_date) AS INTEGER) - CAST(strftime('%Y', c.birthdate) AS INTEGER) AS Age
FROM Sales as s
JOIN Customers as c
ON s.customerkey = c.customerkey),

age_groups AS (
SELECT
    CASE
        WHEN Age BETWEEN 0 AND 17 THEN '0-17'
        WHEN Age BETWEEN 18 AND 24 THEN '18-24'
        WHEN Age BETWEEN 25 AND 34 THEN '25-34'
        WHEN Age BETWEEN 35 AND 44 THEN '35-44'
        WHEN Age BETWEEN 45 AND 54 THEN '45-54'
        WHEN Age BETWEEN 55 AND 64 THEN '55-64'
        ELSE '65+'
    END AS age_group,
    customerkey
FROM age_table)

SELECT
    age_group,
    COUNT(customerkey) AS num_customers
FROM age_groups
GROUP BY age_group
ORDER BY CAST(SUBSTR(age_group, 1) AS INTEGER)''', conn)
```

## Export dataframes into csv for dashboard creation


```python
import os

path = "C:/Users/Vincent/Documents/Project Portfolio/Global_retailer_dataset/tables"

export_list = ['total_sales_df', 'product_sales_df', 'new_customers_df', 'age_df']

for tables in export_list:
    df = globals()[tables]
    df.to_csv(os.path.join(path, f"{tables}.csv"), index=False)
print("Exported successfully")
```

    Exported successfully
    


```python

```
