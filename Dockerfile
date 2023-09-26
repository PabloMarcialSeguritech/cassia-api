#It instructs Docker Engine to use official python:3.10 as the base image
FROM python:3.11

#It creates a working directory(app) for the Docker image and container
WORKDIR /code

#It copies the framework and the dependencies for the FastAPI application into the working directory
COPY Pipfile Pipfile.lock /code/

# Install dependencies
RUN pip install pipenv && pipenv install --system --dev


COPY . /code/

#It will expose the FastAPI application on port `8000` inside the container
EXPOSE 8002


#It is the command that will start and run the FastAPI application container
CMD ["python", "main.py"]