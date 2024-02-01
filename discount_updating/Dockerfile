FROM continuumio/miniconda3
RUN apt-get update -y
RUN apt-get install -y python3-pip python-dev build-essential
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
ENTRYPOINT ["python", "discount_updating.py"]
#EXPOSE 3000
#CMD ["python","discount_updating.py","-stores","(stores)"]
