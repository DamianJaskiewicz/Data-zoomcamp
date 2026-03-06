# podstaoowy obraz dockera 
FROM python:3.13.11-slim
RUN pip install --no-cache-dir uv
RUN pip install pandas pyarrow

# copy uv binary from official iv image (muli stage build pattern)
# dodajemy pliki lub inne rzeczy niezbedend do uruchjmiania docekrea moza mniec gotowe plikn 
#COPY --from-ghce.io/astral-sh/uv:latest /uv /bin/
WORKDIR /app

# ADD virtual envirment to path so we can use installed package 
ENV PATH="/app/.venv/bin:$PATH"

#copy dependency file first (better layer cahin ) f
# copy daje nam miżlowosc podpiąc w danyum miescu pliki czy skrypty jakie sa potrzebned  z 
# plików lokalnych 
COPY pyproject.toml uv.lock .python-version ./

RUN uv sync --locked

COPY ingest_data_01.py ./

ENTRYPOINT ["python", "ingest_data_01.py"]

