# Hadoop_WordCount

Một số lệnh có đường dẫn local là demo. Các lệnh demo cho file V1, các file khác tương tự.

`hdfs namenode -format`

`start-all.sh`

Xóa dữ liệu datanode (nếu cần thiết, sau khi chạy namenode -format)

`sudo rm -rf /home/hdoop/dfsdata/datanode/*`

Khởi tạo classpath

`export HADOOP_CLASSPATH=$(hadoop classpath)`

`echo $HADOOP_CLASSPATH`

Khởi tạo thư mục WordCount và Input trên HDFS

`hadoop fs -mkdir /WordCount`

`hadoop fs -mkdir /WordCount/Input`

Các file input được chứa trong input_data, các file java class sẽ được lưu trong java_class

Đưa các file input từ LFS vào HDFS

`hadoop fs -put /home/nam/WordCount/V1/input_data/* /WordCount/Input`

Chuyển đến thư mục cần chạy

`cd /home/nam/WordCount/V1`

Biên dịch file java

`javac -classpath ${HADOOP_CLASSPATH} -d /home/nam/WordCount/V1/java_class /home/nam/WordCount/V1/WordCount.java`

Tạo file jar

`jar -cvf wordcount.jar -C /home/nam/WordCount/V1/java_class/ .`

Chạy hadoop map reduce

`hadoop jar wordcount.jar WordCount /WordCount/Input /WordCount/Output`

Kiểm tra file output

`hdfs dfs -cat /WordCount/Output/*`

Xóa folder Output trên HDFS

`hadoop fs -rm -r /WordCount/Output`
