FROM python:3-alpine

WORKDIR /usr/src/scripts

RUN apk --no-cache add curl
RUN apk --no-cache add gpg
RUN apk --no-cache add gpg-agent
RUN apk --no-cache add gcc g++ libc-dev libffi-dev libxml2 unixodbc-dev

ADD microsoft_obdc_setup.sh .
ADD credentials.json .
RUN mkdir -p '/opt/microsoft/msodbcsql18'
RUN echo > '/opt/microsoft/msodbcsql18/ACCEPT_EULA'
RUN source ./microsoft_obdc_setup.sh

ADD requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ADD ./main.py .
RUN chmod +x main.py

CMD [ "python", "./main.py" ]
