from datetime import *
import pymongo
from confluent_kafka import Producer
import sys

def error_cb(err):
    print('Error: %s' % err)
def send_kaf():
    props = {
        # Kafka集群在那裡?
        'bootstrap.servers': '10.1.0.87:9092',  # <-- 置換成要連接的Kafka集群
        'error_cb': error_cb                    # 設定接收error訊息的callback函數
    }
    # 步驟2. 產生一個Kafka的Producer的實例
    producer = Producer(props)
    # 步驟3. 指定想要發佈訊息的topic名稱
    topicName1 = 'items'
    # topicName2 = 'items2'
    try:
        producer.produce(topicName1, '肉棒  數量: 3 價格: 90', '商品')
        # producer.produce(topicName2, '八爪椅  數量: 3 價格: 300', '商品2')
        producer.flush()
        print('Send messages to two topic ok')
    except BufferError as e:
        # 錯誤處理
        sys.stderr.write('%% Local producer queue is full ({} messages awaiting delivery): try again\n'
                         .format(len(producer)))
    except Exception as e:
        print(e)
    producer.flush()
def send_kaf2():
    props = {
        'bootstrap.servers': '10.1.0.87:9092',
        'error_cb': error_cb
    }
    producer = Producer(props)

    topicName2 = 'items2'
    try:
        producer.produce(topicName2, '八爪椅  數量: 3 價格: 300', '商品2')
        producer.flush()
        print('Send messages to two topic ok')
    except BufferError as e:
        # 錯誤處理
        sys.stderr.write('%% Local producer queue is full ({} messages awaiting delivery): try again\n'
                         .format(len(producer)))
    except Exception as e:
        print(e)
    producer.flush()

# 主程式進入點
if __name__ == '__main__':

    #此處空白為你的超音波啟動主程式放置區,或是另外包一個function,或是另外開一支主程式等拿取訊息send去mongo

    myclient = pymongo.MongoClient("mongodb+srv://peter:0987602620@cluster0.0qqo9.mongodb.net/ceb101?retryWrites=true&w=majority")
    # myclient = pymongo.MongoClient("mongodb+srv://eks210017:eks210017@cluster0.yrvuu.mongodb.net/mydatabase?retryWrites=true&w=majority")
    mydb = myclient["wow"]
    mycol = mydb["transaction"]
    results = mycol.find({},{"_id":0},sort=[('_id',-1)]).limit(1)
    for result in results:
        a = datetime.strptime(result['date'], "%Y-%m-%d %H:%M:%S") #抓到的時間字串轉時間並給予變數名
        print(a)
        b = datetime.now() #當下時間
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) #為了確認當下時間而給予回饋格式顯示
        if ((b-a).seconds)<5:
            send_kaf()
            print("已成功發送第一筆msg給'items'topic")
            #send_kaf2()
            print("已成功發送第二筆msg給'items2'topic")
        else:
            print("時間差過大,判定查找訊息並不是最新交易訊息")
