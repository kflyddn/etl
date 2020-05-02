

This is a publisher-subscriber pattern implementation using Java, Spring Boot and Kafka.
The application reads .csv/.xml files and transforms them into .json

**How to execute:**
1. Check out the code
2. Put .csv/.xml file in this directory:

    src/main/resources
    
3. Run zookeeper and kafka locally

4. Execute _App.java_ with following parameters:

file=[your input file name] -Dspring.profiles.active="xml,csv"

* _-Dspring.profiles.active_ can be eather "xml" or "csv" or "xml,csv"