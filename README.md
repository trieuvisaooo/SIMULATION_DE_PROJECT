# ODAP_CQ2021/1: LAB PROJECT

## YÊU CẦU:
1. Sử dụng kafka để đọc dữ liệu csv từng dòng và gửi thông tin này đến topic định nghĩa 
trước theo chu kì thời gian ngẫu nhiên trong phạm vi từ 1s đến 3s
- NOTE: 
    - Repo: 
        - Vì data lớn nên đã được ignore khi push lên
    - Kafka:
        - Credit Card Transaction Data được producer gửi đi dưới dạng json
    - Nếu bị lỗi import module --> chạy: pip install -r requirements.txt để cài các module liên quan
- TODO: 
    - Tạo folder 'data' 
    - Tải về các file csv và lưu vào folder 'data' 
- TEST:
    1. run zookeeper
    2. run kafka
    3. run producer.py to produce msg
    4. run consumer.py to check msg produced
2. Sử dụng spark streaming để đọc dữ liệu từ kafka theo thời gian thực, nghĩa là bất cứ
thông tin nào từ kafka được xử lý tức thì, các xử lý bao gồm lọc dữ liệu, biến đổi thông 
tin, tính toán dữ liệu.
- NOTE:
    - Version pyspark sử dụng là pyspark==3.5.3
- TODO:
    - Thực hiện đầy đủ các bước ở mục 1.kafka
    - Sau khi TEST thành công thì tiến hành cấu hình lưu trữ dữ liệu transaction vào hadoop 
    (lưu ý: cần lưu thêm dữ liệu về user và card ở 2 file sd254_cards.csv và sd254_users.csv)
- TEST: 
    1. run zookeeper
    2. run kafka
    3. run producer.py để dùng kafka đọc từng dòng từ file data
    4. run streaming.py để dùng spark đọc dữ liệu từ kafka và in ra kết quả kiểm tra
    (lưu ý: khi chạy streaming.py lần đầu sẽ mất khá nhiều thời gian để chương trình tự tải và cài đặt dependency spark-sql-kafka)

    bin\windows\zookeeper-server-start.bat config\zookeeper.properties
    bin\windows\kafka-server-start.bat config\server.properties


3. Sử dụng Hadoop để lưu trữ các thông tin được xử lý từ Spark và là nơi lưu trữ thông tin 
được xử lý để có thể trực quan hóa dữ liệu và thống kê ở giai đoạn sau.
- NOTE:
    - Version hadoop sử dụng là Hadoop 3.2.4
- TODO:
    - Thực hiện đầy đủ các bước ở mục 1.kafka và mục 2.spark streaming
- TEST: 
    1. run zookeeper
    2. run kafka
    3. run hadoop
    4. run producer.py
    5. run streaming.py
    6. Mở "http://localhost:9870/explorer.html#/user/spark/transactions_csv" để kiểm tra có file csv được lưu trữ hay chưa.
5. Sử dụng Power B I để đọc dữ liệu từ Hadoop (dạng csv), thống kê dữ liệu theo mô tả bài 
toán và hiển thị dữ liệu một cách trực quan. 
6. Sử dụng Air Flow để lên lịch quá trình đọc và hiển thị dữ liệu từ Power BI sao cho dữ liệu 
luôn được update mỗi ngày.
