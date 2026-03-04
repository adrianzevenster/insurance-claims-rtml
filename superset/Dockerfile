FROM apache/superset:4.0.2

USER root

RUN pip install --no-cache-dir \
  "SQLAlchemy<2" \
  "alembic<1.13" \
  "clickhouse-connect[sqlalchemy]" \
  lz4 \
  psycopg2-binary

USER superset