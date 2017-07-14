## Kafka Producer
java -cp ./target/BigDataTemplate-jar-with-dependencies.jar com.template.bigdata.kafka.KafkaTopicProducer <kafkahost:portno> <topicname> <numberOfMessages>

Eg: java -cp ./target/BigDataTemplate-jar-with-dependencies.jar com.template.bigdata.kafka.KafkaTopicProducer localhost:9092 testconsume 15

## Kafka Consumer
java -cp ./target/BigDataTemplate-jar-with-dependencies.jar com.template.bigdata.kafka.KafkaTopicConsumer <zookeeperhost:portno> <groupId> <topicname>

Eg: java -cp ./target/BigDataTemplate-jar-with-dependencies.jar com.template.bigdata.kafka.KafkaTopicConsumer localhost:2181 testconsumeGrp testconsume

### TODO List
 1. White list topics using regular expression for consumer
 2. Use gzip to compress the data
 3. Kafka producer with custom partitioning
 4. Multi threaded Kafka Consumer 

 
 #### Reference
 1. Learning Apache Kafka - Second Edition, By: Nishant Garg