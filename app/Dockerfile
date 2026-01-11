FROM python:3.12-slim

WORKDIR /app
EXPOSE 60001

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
  && pip install -r ./requirements.txt

COPY . .
CMD [ "python" , "driver.py"]
