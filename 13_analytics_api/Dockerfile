FROM python:3.10.16-slim-bullseye

RUN apt-get update && apt-get install -y --no-install-recommends bash curl ca-certificates

ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh

ENV PATH="/root/.local/bin/:$PATH"

WORKDIR /code

COPY ./src /code/src/
COPY ./pyproject.toml ./uv.lock /code/

EXPOSE 8000

RUN uv sync

RUN chmod +x /code/.venv/bin/activate

RUN . /code/.venv/bin/activate

WORKDIR /code/src/

EXPOSE 8000

CMD ["/code/.venv/bin/uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
