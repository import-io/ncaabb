FROM python:3.5.2

ADD /importio-ncaabb /importio-ncaabb

RUN pip3 install -r /importio-ncaabb/requirements.txt

WORKDIR /importio-ncaabb

CMD python3 app.py