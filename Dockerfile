FROM python:3.7

WORKDIR /app

COPY requirements.txt .

# RUN apk --update add --no-cache g++
RUN pip install --upgrade pip
# RUN pip install --no-cache-dir -r requirements.txt
RUN pip install -r requirements.txt
# COPY . .

COPY . .
# RUN apk --update add --no-cache g++
# RUN pip install --upgrade pip
# RUN pip install -e .
# 
ENTRYPOINT ["tail", "-f", "/dev/null"]