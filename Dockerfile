FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow fastparquet

#create directory in docker image 
#that the ingest_data file will be copied to
WORKDIR /app

#copy pipelin.py file from this build to the docker image
#first is file from computer, second is destination
COPY ingest_data.py ingest_data.py

#this is a variable used from launching docker this replaces
# docker run -it "--entrypoint=bash"
#ENTRYPOINT [ "bash" ]

ENTRYPOINT [ "python", "ingest_data.py" ]