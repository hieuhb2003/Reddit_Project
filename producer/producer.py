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
