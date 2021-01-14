from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import pymongo

# 用來接收從Consumer instance發出的error訊息
def error_cb(err):
    print('Error: %s' % err)


# 轉換msgKey或msgValue成為utf-8的字串
def try_decode_utf8(data):
    if data:
        return data.decode('utf-8')
    else:
        return None


# 指定要從哪個partition, offset開始讀資料
# def my_assign(consumer_instance, partitions):
#     for p in partitions:
#         p.offset = 0
#     print('assign', partitions)
#     consumer_instance.assign(partitions)


def print_sync_commit_result(partitions):
    if partitions is None:
        print('# Failed to commit offsets')
    else:
        for p in partitions:
            print('# Committed offsets for: %s-%s {offset=%s}' % (p.topic, p.partition, p.offset))
def memberin():
    props = {
        'bootstrap.servers': '10.1.0.87:9092',  # Kafka集群在那裡? (置換成要連接的Kafka集群)
        'group.id': 'peter',  # ConsumerGroup的名稱 (置換成你/妳的學員ID)
        'auto.offset.reset': 'latest',  # 是否從這個ConsumerGroup尚未讀取的partition/offset開始讀earliest
        'enable.auto.commit': False,  # 是否啟動自動commit
        'error_cb': error_cb  # 設定接收error訊息的callback函數
    }

    # 步驟2. 產生一個Kafka的Consumer的實例
    consumer = Consumer(props)
    # 步驟3. 指定想要訂閱訊息的topic名稱
    topicName = 'logs'
    # 步驟4. 讓Consumer向Kafka集群訂閱指定的topic
    consumer.subscribe([topicName])

    # 步驟5. 持續的拉取Kafka有進來的訊息
    try:
        while True:
            records_pulled = False  # 用來檢查是否有有效的record被取出來

            # 請求Kafka把新的訊息吐出來
            records = consumer.consume(num_messages=500, timeout=1.0)  # 批次讀取
            if records is None:
                continue

            for record in records:
                # 檢查是否有錯誤
                if record is None:
                    continue
                if record.error():
                    # Error or event
                    if record.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (record.topic(), record.partition(), record.offset()))
                    else:
                        # Error
                        raise KafkaException(record.error())
                else:
                    records_pulled = True

                    # ** 在這裡進行商業邏輯與訊息處理 **
                    # 取出相關的metadata
                    # topic = record.topic()
                    # partition = record.partition()
                    # offset = record.offset()
                    # timestamp = record.timestamp()
                    # 取出msgKey與msgValue
                    # msgKey = try_decode_utf8(record.key())
                    # msgValue = try_decode_utf8(record.value())

                    # 秀出metadata與msgKey & msgValue訊息
                    # print('%s-%d-%d : (%s , %s)' % (topic, partition, offset, msgKey, msgValue))
                    # test_msg = ('%s : %s' % (msgKey,msgValue))
                    print ("會員已登入")
                    consumer.close()
            # 同步地執行commit (Sync commit)
            # if records_pulled:
            #     offsets = consumer.commit(asynchronous=False)
            #     print_sync_commit_result(offsets)
    except KeyboardInterrupt as e:
        sys.stderr.write('Aborted by user\n')
    except Exception as e:
        sys.stderr.write(str(e))

    finally:
        print("已確認那個王八蛋進來了")
        # 步驟6.關掉Consumer實例的連線
        # consumer.close()
def get_items():
    props = {
        'bootstrap.servers': '10.1.0.87:9092',  # Kafka集群在那裡? (置換成要連接的Kafka集群)
        'group.id': 'peter',  # ConsumerGroup的名稱 (置換成你/妳的學員ID)
        'auto.offset.reset': 'latest',  # 是否從這個ConsumerGroup尚未讀取的partition/offset開始讀earliest
        'enable.auto.commit': False,  # 是否啟動自動commit
        'error_cb': error_cb  # 設定接收error訊息的callback函數
    }

    # 步驟2. 產生一個Kafka的Consumer的實例
    consumer = Consumer(props)
    # 步驟3. 指定想要訂閱訊息的topic名稱
    topicName = 'items'
    # 步驟4. 讓Consumer向Kafka集群訂閱指定的topic
    consumer.subscribe([topicName])

    # 步驟5. 持續的拉取Kafka有進來的訊息
    try:
        while True:
            records_pulled = False  # 用來檢查是否有有效的record被取出來

            # 請求Kafka把新的訊息吐出來
            records = consumer.consume(num_messages=500, timeout=1.0)  # 批次讀取
            if records is None:
                continue

            for record in records:
                # 檢查是否有錯誤
                if record is None:
                    continue
                if record.error():
                    # Error or event
                    if record.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (record.topic(), record.partition(), record.offset()))
                    else:
                        # Error
                        raise KafkaException(record.error())
                else:
                    records_pulled = True

                    # ** 在這裡進行商業邏輯與訊息處理 **
                    # 取出相關的metadata
                    topic = record.topic()
                    partition = record.partition()
                    offset = record.offset()
                    timestamp = record.timestamp()
                    # 取出msgKey與msgValue
                    msgKey = try_decode_utf8(record.key())
                    msgValue = try_decode_utf8(record.value())

                    sendmsg_items = {
                        msgKey: msgValue
                    }
                    # client = pymongo.MongoClient(
                    #     "mongodb+srv://peter:0987602620@cluster0.0qqo9.mongodb.net/ceb101?retryWrites=true&w=majority")
                    #
                    # mydb = client.test
                    # mycol = mydb['wow6']
                    # mycol.insert_many([sendmsg_items])

                    # 秀出metadata與msgKey & msgValue訊息
                    # print('%s-%d-%d : (%s , %s)' % (topic, partition, offset, msgKey, msgValue))
                    # test_msg = ("%s" , "%s" %(msgKey,msgValue))
                    print(sendmsg_items)
                    return sendmsg_items

            # 同步地執行commit (Sync commit)
            if records_pulled:
                offsets = consumer.commit(asynchronous=False)
                print_sync_commit_result(offsets)


    except KeyboardInterrupt as e:
        sys.stderr.write('Aborted by user\n')
    except Exception as e:
        sys.stderr.write(str(e))

    finally:
        # 步驟6.關掉Consumer實例的連線
        print("已確認收到第一筆商品訊息")
        # consumer.close()
def get_items2():
    props = {
        'bootstrap.servers': '10.1.0.87:9092',  # Kafka集群在那裡? (置換成要連接的Kafka集群)
        'group.id': 'peter',  # ConsumerGroup的名稱 (置換成你/妳的學員ID)
        'auto.offset.reset': 'latest',  # 是否從這個ConsumerGroup尚未讀取的partition/offset開始讀earliest
        'enable.auto.commit': False,  # 是否啟動自動commit
        'error_cb': error_cb  # 設定接收error訊息的callback函數
    }

    # 步驟2. 產生一個Kafka的Consumer的實例
    consumer = Consumer(props)
    # 步驟3. 指定想要訂閱訊息的topic名稱
    topicName = 'items2'
    # 步驟4. 讓Consumer向Kafka集群訂閱指定的topic
    consumer.subscribe([topicName])

    # 步驟5. 持續的拉取Kafka有進來的訊息
    try:
        while True:
            records_pulled = False  # 用來檢查是否有有效的record被取出來

            # 請求Kafka把新的訊息吐出來
            records = consumer.consume(num_messages=500, timeout=1.0)  # 批次讀取
            if records is None:
                continue

            for record in records:
                # 檢查是否有錯誤
                if record is None:
                    continue
                if record.error():
                    # Error or event
                    if record.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (record.topic(), record.partition(), record.offset()))
                    else:
                        # Error
                        raise KafkaException(record.error())
                else:
                    records_pulled = True

                    # ** 在這裡進行商業邏輯與訊息處理 **
                    # 取出相關的metadata
                    topic = record.topic()
                    partition = record.partition()
                    offset = record.offset()
                    timestamp = record.timestamp()
                    # 取出msgKey與msgValue
                    msgKey = try_decode_utf8(record.key())
                    msgValue = try_decode_utf8(record.value())

                    sendmsg_items2 = {
                        msgKey: msgValue
                    }
                    # client = pymongo.MongoClient(
                    #     "mongodb+srv://peter:0987602620@cluster0.0qqo9.mongodb.net/ceb101?retryWrites=true&w=majority")
                    #
                    # mydb = client.test
                    # mycol = mydb['wow6']
                    # mycol.insert_many([sendmsg_items])

                    # 秀出metadata與msgKey & msgValue訊息
                    # print('%s-%d-%d : (%s , %s)' % (topic, partition, offset, msgKey, msgValue))
                    # test_msg = ("%s" , "%s" %(msgKey,msgValue))
                    print(sendmsg_items2)
                    return sendmsg_items2

            # 同步地執行commit (Sync commit)
            if records_pulled:
                offsets = consumer.commit(asynchronous=False)
                print_sync_commit_result(offsets)


    except KeyboardInterrupt as e:
        sys.stderr.write('Aborted by user\n')
    except Exception as e:
        sys.stderr.write(str(e))

    finally:
        # 步驟6.關掉Consumer實例的連線
        print("已確認收到第二筆商品訊息")
def get_trans():
    props = {
        'bootstrap.servers': '10.1.0.87:9092',  # Kafka集群在那裡? (置換成要連接的Kafka集群)
        'group.id': 'peter',  # ConsumerGroup的名稱 (置換成你/妳的學員ID)
        'auto.offset.reset': 'latest',  # 是否從這個ConsumerGroup尚未讀取的partition/offset開始讀earliest
        'enable.auto.commit': False,  # 是否啟動自動commit
        'error_cb': error_cb  # 設定接收error訊息的callback函數
    }

    # 步驟2. 產生一個Kafka的Consumer的實例
    consumer = Consumer(props)
    # 步驟3. 指定想要訂閱訊息的topic名稱
    topicName = 'transaction'
    # 步驟4. 讓Consumer向Kafka集群訂閱指定的topic
    consumer.subscribe([topicName])

    # 步驟5. 持續的拉取Kafka有進來的訊息
    try:
        while True:
            records_pulled = False  # 用來檢查是否有有效的record被取出來

            # 請求Kafka把新的訊息吐出來
            records = consumer.consume(num_messages=500, timeout=1.0)  # 批次讀取
            if records is None:
                continue

            for record in records:
                # 檢查是否有錯誤
                if record is None:
                    continue
                if record.error():
                    # Error or event
                    if record.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (record.topic(), record.partition(), record.offset()))
                    else:
                        # Error
                        raise KafkaException(record.error())
                else:
                    records_pulled = True

                    # ** 在這裡進行商業邏輯與訊息處理 **
                    # 取出相關的metadata
                    topic = record.topic()
                    partition = record.partition()
                    offset = record.offset()
                    timestamp = record.timestamp()
                    # 取出msgKey與msgValue
                    msgKey2 = try_decode_utf8(record.key())
                    msgValue2 = try_decode_utf8(record.value())

                    sendmsg_trans = {
                        msgKey2: msgValue2
                    }
                    # client = pymongo.MongoClient(
                    #     "mongodb+srv://peter:0987602620@cluster0.0qqo9.mongodb.net/ceb101?retryWrites=true&w=majority")
                    #
                    # mydb = client.test
                    # mycol = mydb['wow7']
                    # mycol.insert_many([sendmsg_trans])

                    # 秀出metadata與msgKey & msgValue訊息
                    # print('%s-%d-%d : (%s , %s)' % (topic, partition, offset, msgKey, msgValue))
                    # test1_msg = ("%s" , "%s" % (msgKey, msgValue)) #('%s : %s' % (msgKey,msgValue))

                    print(sendmsg_trans)
                    return sendmsg_trans


            # 同步地執行commit (Sync commit)
            if records_pulled:
                offsets = consumer.commit(asynchronous=False)
                print_sync_commit_result(offsets)


    except KeyboardInterrupt as e:
        sys.stderr.write('Aborted by user\n')
    except Exception as e:
        sys.stderr.write(str(e))

    finally:
        # 步驟6.關掉Consumer實例的連線
        consumer.close()

from datetime import *
import threading
if __name__ == '__main__':

    # a = threading.Thread(target=memberin)
    # b = threading.Thread(target=get_items)
    # c = threading.Thread(target=get_trans)
    # x = get_items()
    # y = get_trans()
    # a.start()
    memberin()

    # time.sleep(3)
    # b.start()
    # c.start()
    try:
        c = get_trans()
        print("已收到trans訊息")
        a = get_items()
        print("即將傳送items訊息")
        b = get_items2()
        print("即將傳送items2訊息")

        final_msg = {}
        # final_msg.setdefault(c)
        # final_msg.setdefault(b)
        time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        final_msg.setdefault("Time", time)
        final_msg.update(c)
        final_msg.update(a)
        final_msg.update(b)

        print(final_msg)
    except BufferError as e:
        print(e)

    client = pymongo.MongoClient("mongodb+srv://peter:0987602620@cluster0.0qqo9.mongodb.net/ceb101?retryWrites=true&w=majority")

    mydb = client.test
    mycol = mydb['wow8']
    mycol.insert_many([final_msg])





