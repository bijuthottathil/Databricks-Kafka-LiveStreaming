# Databricks-Kafka

To stream data from Alpha Vantage and store it in Confluent Kafka , we will be fetching data from Alpha Vantage continuously and send to Kafka in real time. We  will use one Pyspark note book to simulate it
Then we will create Databricks notebooks to consude data from Kafka topic and put in delta tables

Prerequisistes
1) Alpha Vantage API Key   2) Confluent Kafka Cluster     3) PySpark Environment in Databricks

API used in this project is https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=IBM&apikey=demo

Based on the Symbol we provide, we can get  the most recent 100 intraday OHLCV bars by default when the outputsize parameter is not set

![image](https://github.com/user-attachments/assets/3d98972b-8e22-4a68-a7c6-f312214f73ba)

 
To consume this API, we have to generate API Key by visiting https://www.alphavantage.co/support/#api-key  . It is free of cost. In free plan only 5 API request allowed per minute


We are using Unity Catalog to store data in Medallion structure. So we setup ADLS Storage to store Metastore,Catalog, Schema, External tables,Checkpoints and External Volumes. Structure is like below
![image](https://github.com/user-attachments/assets/e79bc015-56a6-4700-b621-6bddd8e3375e)

Catalog and corresponding schema's are created in Databricks like this. All data will be pointing to corresponding external storages defined in Azure ADLS given below

![image](https://github.com/user-attachments/assets/904b5401-57c8-4a4a-a246-09fcdfe118ae)

External locations are defined in Databricks

![image](https://github.com/user-attachments/assets/26b80ace-e0eb-47e4-95b2-20bf940bb8e4)



Created Kafka cluster in Confluent

![image](https://github.com/user-attachments/assets/970de6b6-e357-45cc-9e8b-e0047d95ef80)



![image](https://github.com/user-attachments/assets/e8f70dcc-0a97-4ae5-86a8-7c3627741643)


![image](https://github.com/user-attachments/assets/047eea2a-7925-45e6-9fdd-fa88984e85b0)


Created first topic

![image](https://github.com/user-attachments/assets/753a9c06-801f-4bab-8e09-9833602c6898)


![image](https://github.com/user-attachments/assets/0ea61ec2-0a2e-4be4-9995-76da9c51628e)

Tested with simple message
![image](https://github.com/user-attachments/assets/d5f2f5b8-07c8-4f85-90b3-be0eee1cd827)


Setup new client

![image](https://github.com/user-attachments/assets/604b0f70-daf6-45d2-b590-12aea5c5d379)


Need to create API key next

![image](https://github.com/user-attachments/assets/5fb2768c-f8e8-4f91-be6b-21d5984021a5)

![image](https://github.com/user-attachments/assets/9aabd8e1-74e1-40ad-85fa-404d41d16a95)
![image](https://github.com/user-attachments/assets/b68a3c94-966c-4bb9-933c-ca7efb4c6f2f)


Please make sure to note down below details to use in Databricks notebook to connect to Kafka

![image](https://github.com/user-attachments/assets/e19d4025-f259-4d2a-80bc-2dc076049725)


Instead of keeping few secret details in Key Vault, I used env variables in cluster to keep API Key, Kafka Server and Kafka Topic name. This is not adviced in prod

![image](https://github.com/user-attachments/assets/3556d6c8-391d-4763-a0df-03672e91a557)

