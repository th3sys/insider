FROM ubuntu:latest

# Install dependencies
RUN apt-get update -y
RUN apt-get install -y python3-pip
RUN apt-get install -y wget
RUN pip3 install boto3
RUN pip3 install uvloop
RUN pip3 install aiohttp
RUN pip3 install beautifulsoup4
RUN pip3 install pandas

# setup
ENV START_YEAR 2014
ENV DEPLOYMENT_MODE ECS
ENV PAGE_SIZE 100
ENV TIMEOUT 900
ENV LOGGING_LEVEL INFO
ENV EDGAR_URL https://www.sec.gov

# Python
RUN mkdir insider
RUN cd insider
RUN wget https://raw.githubusercontent.com/th3sys/insider/master/save.py
RUN wget https://raw.githubusercontent.com/th3sys/insider/master/utils.py
RUN wget https://raw.githubusercontent.com/th3sys/insider/master/connectors.py
RUN wget https://raw.githubusercontent.com/th3sys/insider/master/trading.py
RUN wget https://raw.githubusercontent.com/th3sys/insider/master/analytics.py

ADD docker_files/credentials /root/.aws/credentials
ADD docker_files/config /root/.aws/config

ADD docker_files/start.sh /insider/start.sh
RUN chmod +x /insider/start.sh


EXPOSE 80
ENTRYPOINT ["/insider/start.sh", "-D", "FOREGROUND"]