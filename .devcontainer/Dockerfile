FROM python:3.11-slim

COPY . .

RUN apt-get update && apt-get install -y \
    default-jdk \
    procps \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install pipenv
RUN pipenv install --deploy --ignore-pipfile

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

VOLUME [ "/app/data" ]

EXPOSE 8000


