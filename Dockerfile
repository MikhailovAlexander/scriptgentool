FROM python:3.9-bullseye

RUN apt update
RUN apt upgrade -y

RUN apt install openjdk-11-jdk -y
RUN JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64/"
RUN mkdir /opt/liquibase
RUN wget -O /opt/liquibase/liquibase-4.9.1.tar.gz "https://github.com/liquibase/liquibase/releases/download/v4.9.1/liquibase-4.9.1.tar.gz"
RUN tar -xzf /opt/liquibase/liquibase-4.9.1.tar.gz -C /opt/liquibase
RUN rm /opt/liquibase/liquibase-4.9.1.tar.gz

RUN apt install unixodbc-dev -y

WORKDIR /usr/src/app
RUN mkdir /usr/src/repo
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt install -y msodbcsql18

COPY . .

CMD [ "python", "./main.py" ]