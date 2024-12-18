🚀 Projet Data Lake & Data Warehouse : Analyse des Ventes
📑 Description
Ce projet met en place un Data Lake organisé pour les fichiers CSV, un pipeline ETL Python pour l'extraction et la transformation des données, ainsi qu'un Data Warehouse avec un schéma SQL optimisé. Des requêtes analytiques ont été effectuées pour extraire des insights significatifs.

🗂️ Structure du Data Lake
Les fichiers CSV sont organisés en dossiers par catégorie :

![image](https://github.com/user-attachments/assets/1791f89d-2e4e-40d2-918a-3bf5317202b4)

Chaque dossier contient des fichiers CSV prêts à être utilisés pour les processus ETL.

⚙️ Script ETL Python
Objectif :
Le script ETL extrait les données des fichiers CSV, les transforme pour respecter le schéma cible du Data Warehouse et les charge dans une base PostgreSQL.

🗄️ Schéma SQL du Data Warehouse
Le schéma relationnel utilisé pour le Data Warehouse est le suivant :


Tables :
dim_orders : order_id, order_status
dim_products : product_id, price, product_category_name_english
dim_customers : customer_id, customer_city, customer_state
dim_sellers : seller_id, seller_city
dim_time : date_id, month, quarter, year, order_purchase_timestamp
fact_sales : order_id, product_id, customer_id, seller_id, date_id, payment_value, order_item_id

![image](https://github.com/user-attachments/assets/92a410d9-9a41-4cfe-adc8-98cc15515354)

