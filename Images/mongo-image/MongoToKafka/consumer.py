from kafka import KafkaConsumer
from json import loads
from kafka import TopicPartition
import json
import pandas as pd
# import boto3
# from pandasql import sqldf

if __name__=='__main__':
    consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
    consumer.assign([TopicPartition('orders', 0)])
    print("starting the consumer")
    print('abcd')
    dataframe = pd.DataFrame(columns=['_id','first_name','last_name','email']) # Note that there are now row data inserted.
    i = 0
    # print(dataframe)
    for msg in consumer:
        # print(type(msg.value))
        # messege = json.loads(msg.value)
        # data = messege['_airbyte_data']
        df2 = pd.read_json(msg.value, orient ='index')

        # print(df2[["_airbyte_data"]])
        
        df = df2.iloc[1:2]
        df['index'] = i
        df = df.set_index('index')
        # print(df)

        dataframe = pd.concat([dataframe, df])
        print(dataframe)
        dataframe = dataframe.drop_duplicates(subset=['_id'], keep='last')
        dataframe.to_csv("/home/data.csv")
        # dataframe.drop(columns = ["index"], inplace = True)
        # print(dataframe)
        # df4 = df.append(df2, ignore_index = True)
        # print(df.set_index("index"))
        i+=1


        # df = pd.read_csv("./data.csv")
        # q = """
        # select _ab_cdc_deleted_at, _ab_cdc_log_file, max(_ab_cdc_log_pos) AS _ab_cdc_log_pos, _ab_cdc_updated_at,channel,country,id,order_date,status,store_id,total
        # from df
        # group by id
        # """
        # main_data = sqldf(q,globals())
        # # print(type(main_data))
        # q2 = "select _ab_cdc_deleted_at, _ab_cdc_log_file,_ab_cdc_log_pos, _ab_cdc_updated_at,channel,country,id,order_date,status,store_id,total from main_data group by id having count(id) <=1 AND _ab_cdc_deleted_at is NULL"
        # main_data2 = sqldf(q2,globals())
        # main_data2.to_csv("/home/main_data.csv")

