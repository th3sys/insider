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
RUN pip3 install numpy

# setup
ENV START_YEAR 2014
ENV DEPLOYMENT_MODE ECS
ENV TIMEOUT 900
ENV PAGE_SIZE 100
ENV LOGGING_LEVEL INFO
ENV EDGAR_URL https://www.sec.gov

# Python
RUN mkdir insider
RUN cd insider
ADD save.py save.py
ADD analyse.py analyse.py
ADD check.py check.py
ADD find.py find.py
ADD utils.py utils.py
ADD connectors.py connectors.py
ADD trading.py trading.py
ADD analytics.py analytics.py

ADD docker_files/credentials /root/.aws/credentials
ADD docker_files/config /root/.aws/config

ADD docker_files/start_analyse.sh /insider/start.sh
RUN chmod +x /insider/start.sh


EXPOSE 80
ENTRYPOINT ["/insider/start.sh", "-D", "FOREGROUND"]