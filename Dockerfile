FROM python:3.12-slim-bookworm@sha256:d50fb7611f86d04a3b0471b46d7557818d88983fc3136726336b2a4c657aa30b

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Shanghai

WORKDIR /app
COPY archive.py scheduler.py /app/

RUN groupadd --gid 10001 archive \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin archive \
    && mkdir -p /config /state \
    && chown -R archive:archive /state \
    && chmod 0755 /app/archive.py /app/scheduler.py

USER 10001:10001
ENTRYPOINT ["python", "/app/scheduler.py"]
