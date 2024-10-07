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


We wll using a  topic to store messages coming from API

![image](https://github.com/user-attachments/assets/8f43783c-af26-45ba-b169-fad2aa2f6eb3)

Topic is created. At present no messages in this topic

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

We will start with notebooks

To load data in Kafka topic, I created one notebook. It will simulate data from API continuously to Topic

Topic is ready. Appropriate configurations to connect to Kafka is mentioned in the notebook

![image](https://github.com/user-attachments/assets/d9a4cc5c-4177-4f52-8ebe-06020c8b8f29)

![image](https://github.com/user-attachments/assets/4e08c177-b200-46b6-9163-82b53e9187fd)

Now you can see data loading to topic from API

![image](https://github.com/user-attachments/assets/92f6bbde-6a82-47e8-8d75-4ecb7041a5ca)

Now check Topic 

![image](https://github.com/user-attachments/assets/5ed81929-f2d3-4ef3-b0f1-dd202646f2a0)

I changed stock symbol to Google and loaded data. You can see more data loading in topic. It is still running

![image](https://github.com/user-attachments/assets/e9d0a268-1cdb-4cd7-b787-eb0f39b13599)

Now we have another Bronze note book I created to load data from Kafka Topic and load it in Bronze layer storage

![image](https://github.com/user-attachments/assets/f1b25412-9d5b-4772-93bb-037f7263c09b)

![image](https://github.com/user-attachments/assets/6164f618-cad0-4f18-b309-7707b8eae666)

![image](https://github.com/user-attachments/assets/f1592787-b8bb-405f-aecb-e2f2fbf16a2b)
![image](https://github.com/user-attachments/assets/2803fe3f-486d-48ff-ab88-33f2889126bd)


![image](https://github.com/user-attachments/assets/40fded0c-b8fb-479a-904a-ce7e3d0cb919)

Now we will get bronze data , cleanup and loading in silver layer

![image](https://github.com/user-attachments/assets/edd2f866-790b-41f2-941b-dde8726f44c4)

![image](https://github.com/user-attachments/assets/b69ff5f7-6c86-4857-b7d3-5ce2694c63a9)

Just noted Kafka topic is still getting data from api

![image](https://github.com/user-attachments/assets/ae8a1548-8bff-470e-9577-5ae4addf4a69)

Silver layer table loading with below data. It is cleaned and column names are changed

![image](https://github.com/user-attachments/assets/70f0a2ae-8d80-43db-a191-aefe37b1c92a)

Now we will focus on Gold layer. Here we will capture aggregated data based on Silver table 

![image](https://github.com/user-attachments/assets/47f7d8df-4960-4c52-b06e-6155fd3259c9)

Mean time always review new tables created under Catalog   Kafka

![image](https://github.com/user-attachments/assets/b011ee35-8277-4e72-9d99-98f664637e1f)

And you can see table data is populating in ADLS storage created in Azure too

![image](https://github.com/user-attachments/assets/6533bbd1-a6f7-48d3-a60c-8cdeae1ef7be)

![image](https://github.com/user-attachments/assets/2d6fa125-4a55-4682-82a6-a569798a1a62)

![image](https://github.com/user-attachments/assets/bb3f2049-253b-4eb7-816d-ebcb00a17445)


Streaming jobs are still running in Bronze Layer

![image](https://github.com/user-attachments/assets/758f91e8-7ae2-49d1-860c-469d51db6953)

After loading pending data, I will interupt reading data from Kafka and stop streaming in Bronze Layer

Final step is to show aggregated data in Power BI

Select Partnor Connect from page and choose power Bi

![image](https://github.com/user-attachments/assets/b0a347fa-7dc6-4c9f-936f-ceb481bc8d39)

![image](https://github.com/user-attachments/assets/44d4490f-ed92-4f3c-9877-740b611c1e5e)


Report file will be downloaded in your system

![image](https://github.com/user-attachments/assets/6b656fbc-3957-48a9-86b1-68b8a96c0af8)

Open this file in PowerBI Desktop

You can see your tables

![image](https://github.com/user-attachments/assets/ebb20b51-84d7-484a-ac3b-3ebd1044f1b4)

![image](https://github.com/user-attachments/assets/4f4d8c83-fedf-49ce-a9cb-52b4e0311bf2)

![image](https://github.com/user-attachments/assets/3a1ce916-a98a-4cb7-8dad-737167a70f56)

By dragging and dropping you can create appropriate visualizations in Power Bi and publish to many destinations

![image](https://github.com/user-attachments/assets/333d3d4b-1431-407a-b743-ddc2b8f56d27)

