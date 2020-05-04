FROM openjdk:8-jdk-alpine
VOLUME /tmp
COPY build/libs/*.jar app.jar
ENTRYPOINT ["java","-Dspring.profiles.active=xml\"", "-jar","/app.jar"]