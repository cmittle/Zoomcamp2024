FROM python:3.9


RUN pip install pandas

#create directory in docker image 
#that the pipeline file will be copied to
WORKDIR /app

#copy pipelin.py file from this build to the docker image
#first is file from computer, second is destination
COPY pipeline.py pipeline.py

#this is a variable used from launching docker this replaces
# docker run -it "--entrypoint=bash"
#ENTRYPOINT [ "bash" ]

ENTRYPOINT [ "python", "pipeline.py" ]