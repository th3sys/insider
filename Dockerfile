FROM ubuntu:latest

# Install dependencies
RUN apt-get update -y
RUN apt-get install -y python3-pip
RUN apt-get install -y wget

# setup
ENV START_YEAR 2014
ENV DEPLOYMENT_MODE ECS
ENV PAGE_SIZE 100
ENV TIMEOUT 900
ENV TRN_FOUND_ARN arn
ENV LOGGING_LEVEL INFO
ENV EDGAR_URL https://www.sec.gov

# Python
RUN wget https://raw.githubusercontent.com/th3sys/insider/master/save.py
RUN wget https://raw.githubusercontent.com/th3sys/insider/master/utils.py

RUN pip3 install boto3
RUN pip3 install uvloop
RUN pip3 install asyncio
ADD ~/.aws/credentials.aws /root/.aws/credentials
ADD ~/.aws/config.aws /root/.aws/config



EXPOSE 80
ENTRYPOINT ["python save.py", "-D", "FOREGROUND"]