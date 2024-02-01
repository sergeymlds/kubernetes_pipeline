FROM continuumio/miniconda3
RUN apt-get update -y
RUN apt-get install -y python3-pip python-dev build-essential
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
ENTRYPOINT ["python"]
#EXPOSE 3000
CMD ["temp_promo_deleting.py"]
