# Consumer Fallback
To run this fallback service we strongly recommend to install [pyenv](https://github.com/pyenv/pyenv). In case that you dont want to use pyenv check `.python-version` file to use that python version.

## Setup project
To setup run these commands:
1. `pyenv install`
2. `pyenv virtualenv venv`
3. `pyenv activate venv`
4. `pip install -r requirements.txt`
5. `cp .env.example .env`

## Run project
### Pre-requisites
Have running the RabbitMQ service

1. `python main.py email`