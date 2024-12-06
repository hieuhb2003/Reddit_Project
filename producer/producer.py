# import sys
# # append the path of the parent directory
# sys.path.append("./")

# # from confluent_kafka import Producer
# from kafka import KafkaProducer
# import os
# import json
# from dotenv import load_dotenv
# load_dotenv()
# import time
# from extract_reddit import collect_submission_details, collect_subreddit_details
# from logs.logger import setup_logger

# def main():
#     # Kafka Producer Configuration
#     kafka_bootstrap_servers = "localhost:9094"
#     kafka_config = {
#         "bootstrap.servers": kafka_bootstrap_servers,
#     }
#     producer = KafkaProducer(kafka_config)
    
#     while True:
#         try:
#             submission_details = collect_submission_details(os.getenv("SUBREDDIT_NAME"))   
#             for submission_info in submission_details:
#                 message = json.dumps(submission_info)
#                 producer.produce("reddit-submissions", message.encode("utf-8"))
#                 producer.poll(10)  # Poll to handle delivery reports
#             producer.flush()

#             submission_details = collect_subreddit_details(os.getenv("SUBREDDIT_NAME"))
#             message = json.dumps(submission_details)
#             producer.produce("reddit-subreddit", message.encode("utf-8"))
#             producer.poll(0)  # Poll to handle delivery reports
#             producer.flush()

#             logger.info("Produced subreddit details")
#         except Exception as e:
#             logger.error("An error occurred while retrieving from reddit: %s", str(e))
        
#         logger.info("Production done, sleeping for 60 seconds...")
#         time.sleep(30)
#         logger.info("Starting over again") 

# if __name__ == "__main__":
#     logger = setup_logger(__name__, 'producer.log')
#     main()


# import sys
# # append the path of the parent directory
# sys.path.append("./")

# from kafka import KafkaProducer
# import json
# import time
# from extract_reddit import collect_submission_details, collect_subreddit_details
# from logs.logger import setup_logger

# def main():
#     # Danh sách các subreddits
#     subreddits = ["soccer", "football"]

#     # Kafka Producer Configuration
#     # kafka_bootstrap_servers = "localhost:9094"
#     # kafka_config = {
#     #     "bootstrap.servers": kafka_bootstrap_servers,
#     # }
#     # producer = KafkaProducer(kafka_config)
#     kafka_bootstrap_servers = "localhost:9094"
#     producer = KafkaProducer(
#         bootstrap_servers=kafka_bootstrap_servers,  # Sử dụng đúng tham số
#         value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Tuỳ chọn, serialize dữ liệu thành JSON
#     ) 
#     while True:
#         try:
#             for subreddit_name in subreddits:
#                 # Thu thập dữ liệu bài đăng
#                 submission_details = collect_submission_details(subreddit_name)   
#                 for submission_info in submission_details:
#                     message = json.dumps(submission_info)
#                     producer.send("reddit-submissions", message.encode("utf-8"))
#                       # Poll to handle delivery reports
#                 producer.flush()

#                 # Thu thập dữ liệu subreddit
#                 subreddit_details = collect_subreddit_details(subreddit_name)
#                 message = json.dumps(subreddit_details)
#                 producer.send("reddit-subreddit", message.encode("utf-8"))
#                   # Poll to handle delivery reports
#                 producer.flush()

#                 logger.info(f"Produced details for subreddit: {subreddit_name}")
#         except Exception as e:
#             logger.error("An error occurred while retrieving from reddit: %s", str(e))
        
#         logger.info("Production done, sleeping for 60 seconds...")
#         time.sleep(30)
#         logger.info("Starting over again") 

# if __name__ == "__main__":
#     logger = setup_logger(__name__, 'producer.log')
#     main()

# import sys
# # append the path of the parent directory
# sys.path.append("./")

# from kafka import KafkaProducer
# import json
# import time
# from extract_reddit import collect_submission_details, collect_subreddit_details
# from logs.logger import setup_logger

# def main():
#     # Danh sách các subreddits
#     subreddits = ["soccer", "football"]

#     # Kafka Producer Configuration
#     kafka_bootstrap_servers = "localhost:9094"
#     producer = KafkaProducer(
#         bootstrap_servers=kafka_bootstrap_servers,  # Sử dụng đúng tham số
#         value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize dữ liệu thành JSON và mã hóa thành bytes
#     ) 
    
#     while True:
#         try:
#             for subreddit_name in subreddits:
#                 # Thu thập dữ liệu bài đăng
#                 submission_details = collect_submission_details(subreddit_name)   
#                 for submission_info in submission_details:
#                     # Chỉ serialize 1 lần
#                     message = json.dumps(submission_info)  # Serialize to JSON
#                     print(f"Sending submission details: {message}")
#                     producer.send("reddit-submissions", value=message)  # Send as JSON bytes
#                     producer.flush()  # Ensure all messages are sent
                
#                 # Thu thập dữ liệu subreddit
#                 subreddit_details = collect_subreddit_details(subreddit_name)
#                 message = json.dumps(subreddit_details)  # Serialize to JSON
#                 print(f"Sending subreddit details: {message}")
#                 producer.send("reddit-subreddit", value=message)  # Send as JSON bytes
#                 producer.flush()  # Ensure all messages are sent

#                 logger.info(f"Produced details for subreddit: {subreddit_name}")
#         except Exception as e:
#             logger.error("An error occurred while retrieving from reddit: %s", str(e))
        
#         logger.info("Production done, sleeping for 60 seconds...")
#         time.sleep(30)
#         logger.info("Starting over again") 

# if __name__ == "__main__":
#     logger = setup_logger(__name__, 'producer.log')
#     main()

import sys
sys.path.append("./")

from kafka import KafkaProducer
import json
import time
from extract_reddit import collect_submission_details, collect_subreddit_details
from logs.logger import setup_logger

def main():
    # Danh sách subreddit cần theo dõi
    subreddits = ["soccer", "football"]

    # Cấu hình Kafka Producer
    kafka_bootstrap_servers = "localhost:9094"
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    while True:
        try:
            for subreddit_name in subreddits:
                # Thu thập dữ liệu submission
                submission_details = collect_submission_details(subreddit_name)
                for submission_info in submission_details:
                    logger.info(f"Đang gửi submission: {json.dumps(submission_info)}")
                    producer.send("reddit-submissions", value=submission_info)
                producer.flush()

                # Thu thập dữ liệu subreddit
                subreddit_details = collect_subreddit_details(subreddit_name)
                logger.info(f"Đang gửi subreddit: {json.dumps(subreddit_details)}")
                producer.send("reddit-subreddit", value=subreddit_details)
                producer.flush()

                logger.info(f"Đã sản xuất dữ liệu thành công cho subreddit: {subreddit_name}")
                
        except Exception as e:
            logger.error(f"Lỗi khi sản xuất dữ liệu: {str(e)}")
        
        logger.info("Hoàn thành chu kỳ sản xuất. Chờ 30 giây...")
        time.sleep(30)
        logger.info("Bắt đầu chu kỳ sản xuất mới")

if __name__ == "__main__":
    logger = setup_logger(__name__, 'producer.log')
    main()
