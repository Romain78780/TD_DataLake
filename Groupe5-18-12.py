# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 15:44:35 2024

@author: romai
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect

data_lake_path = "C:\\Users\\smoha\\DataLake2"

customers = pd.read_csv(os.path.join(data_lake_path,
"customers/olist_customers_dataset.csv"))

orders1 = pd.read_csv(os.path.join(data_lake_path,
"orders/olist_orders_dataset.csv"))
orders2 = pd.read_csv(os.path.join(data_lake_path,
"orders/olist_order_items_dataset.csv"))

orders = pd.merge(orders1, orders2, on='order_id', how='inner')
orders["date_id"] = range(1, len(orders) + 1)

payments = pd.read_csv(os.path.join(data_lake_path,
"payments/olist_order_payments_dataset.csv"))

products1 = pd.read_csv(os.path.join(data_lake_path,
"products/olist_products_dataset.csv"))
products2 = pd.read_csv(os.path.join(data_lake_path,
"products/product_category_name_translation.csv"))






products = pd.merge(products1, products2, on='product_category_name', how='inner')

reviews = pd.read_csv(os.path.join(data_lake_path,
"reviews/olist_order_reviews_dataset.csv"))

sellers = pd.read_csv(os.path.join(data_lake_path,
"sellers/olist_sellers_dataset.csv"))
sellers = sellers.drop(columns=['seller_zip_code_prefix'])

products = products.drop(columns=['product_category_name', 'product_name_lenght',
       'product_description_lenght', 'product_photos_qty', 'product_weight_g',
       'product_length_cm', 'product_height_cm', 'product_width_cm'])


prix_moyen = orders.groupby("product_id")["price"].mean().reset_index()
products_final = products.merge(prix_moyen, on="product_id", how="left", suffixes=("", "_moyenne"))



sellers_final = sellers.drop(columns=['seller_state'])

customers_final = customers.drop(columns=['customer_zip_code_prefix','customer_unique_id'])

orders_final = orders.drop(columns=['order_id','customer_id', 'order_status','order_approved_at', 'order_delivered_carrier_date',
       'order_delivered_customer_date', 'order_estimated_delivery_date',
       'order_item_id', 'product_id', 'seller_id', 'shipping_limit_date',
       'price', 'freight_value'])

orders_final["order_purchase_timestamp"] = pd.to_datetime(orders_final["order_purchase_timestamp"], errors="coerce") 

orders_final["month"] = orders_final["order_purchase_timestamp"].dt.month
orders_final["year"] = orders_final["order_purchase_timestamp"].dt.year
orders_final["quarter"] = orders_final["order_purchase_timestamp"].dt.quarter

fact_sales = orders.drop(columns=['order_status','order_approved_at', 'order_delivered_carrier_date',
       'order_delivered_customer_date', 'order_estimated_delivery_date',
       'order_item_id','shipping_limit_date',
       'price', 'freight_value','order_purchase_timestamp'])


fact_sales1 = pd.merge(fact_sales, payments, on='order_id', how='inner')

fact_sales_finals = fact_sales1.drop(columns=['payment_sequential','payment_type','payment_installments'])


dim_order = orders.drop(columns=['customer_id', 'order_purchase_timestamp',
       'order_approved_at', 'order_delivered_carrier_date',
       'order_delivered_customer_date', 'order_estimated_delivery_date',
       'order_item_id', 'product_id', 'seller_id', 'shipping_limit_date',
       'price', 'freight_value', 'date_id'])


#enlever les doublons
customers_final = customers_final.drop_duplicates() 
 #Supprimer les lignes avec des valeurs manquantes 
dim_customers = customers_final.dropna()

#enlever les doublons
orders_final = orders_final.drop_duplicates() 
 #Supprimer les lignes avec des valeurs manquantes 
dim_time = orders_final.dropna()

#enlever les doublons
products_final = products_final.drop_duplicates() 
 #Supprimer les lignes avec des valeurs manquantes 
dim_products = products_final.dropna()

#enlever les doublons
sellers_final = sellers_final.drop_duplicates() 
 #Supprimer les lignes avec des valeurs manquantes 
dim_sellers = sellers_final.dropna()

fact_sales_finals = fact_sales_finals.drop_duplicates() 
 #Supprimer les lignes avec des valeurs manquantes 
fact_sales = fact_sales_finals.dropna()

dim_order = dim_order.drop_duplicates() 
 #Supprimer les lignes avec des valeurs manquantes 
dim_orders = dim_order.dropna()

# Connexion PostgreSQL 
engine = create_engine('postgresql+psycopg2://postgres:28sUp11ViN25cI01@localhost:5432/data') 
try:
    # Test de connexion
    connection = engine.connect()
    
    # Inspecter les tables existantes
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print(f"Tables disponibles : {tables}")
    
    connection.close()  # Fermez la connexion proprement
except Exception as e:
    print(f"Erreur lors de l'interrogation : {e}")
    
# Charger les données dans PostgreSQL 
customers_final.to_sql('dim_customers', engine, if_exists='replace', 
index=False) 

# Charger les données dans PostgreSQL 
products_final.to_sql('dim_products', engine, if_exists='replace', 
index=False) 

# Charger les données dans PostgreSQL 
orders_final.to_sql('dim_time', engine, if_exists='replace', 
index=False) 

# Charger les données dans PostgreSQL 
sellers_final.to_sql('dim_sellers', engine, if_exists='replace', 
index=False) 

# Charger les données dans PostgreSQL 
fact_sales_finals.to_sql('fact_sales', engine, if_exists='replace', 
index=False)

# Charger les données dans PostgreSQL 
dim_orders.to_sql('dim_orders', engine, if_exists='replace', 
index=False)