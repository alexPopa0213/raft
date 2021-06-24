FROM openjdk:11-jre-slim
COPY ./build/libs/raft-server-1.0-SNAPSHOT.jar /usr/app/
WORKDIR /usr/app
ENTRYPOINT ["java", "-jar", "-ea", "raft-server-1.0-SNAPSHOT.jar"]