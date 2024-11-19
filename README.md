# ODAP_CQ2021/1: LAB PROJECT

## YÊU CẦU:
1. Sử dụng kafka để đọc dữ liệu csv từng dòng và gửi thông tin này đến topic định nghĩa 
trước theo chu kì thời gian ngẫu nhiên trong phạm vi từ 1s đến 3s
- NOTE: 
    - Repo: 
        - Vì data lớn nên đã được ignore khi push lên
    - Kafka:
        - Credit Card Transaction Data được producer gửi đi dưới dạng json
- TODO: 
    - Tạo folder 'data' 
    - Tải về các file csv và lưu vào folder 'data' 
- TEST:
    1. run zookeeper
    2. run kafka
    3. run main.py to produce msg
    4. run consumer.py to check msg produced
2. Sử dụng spark streaming để đọc dữ liệu từ kafka theo thời gian thực, nghĩa là bất cứ
thông tin nào từ kafka được xử lý tức thì, các xử lý bao gồm lọc dữ liệu, biến đổi thông 
tin, tính toán dữ liệu.
3. Sử dụng Hadoop để lưu trữ các thông tin được xử lý từ Spark và là nơi lưu trữ thông tin 
được xử lý để có thể trực quan hóa dữ liệu và thống kê ở giai đoạn sau.
4. Sử dụng Power B I để đọc dữ liệu từ Hadoop (dạng csv), thống kê dữ liệu theo mô tả bài 
toán và hiển thị dữ liệu một cách trực quan. 
5. Sử dụng Air Flow để lên lịch quá trình đọc và hiển thị dữ liệu từ Power PI sao cho dữ liệu 
luôn được update mỗi ngày.
