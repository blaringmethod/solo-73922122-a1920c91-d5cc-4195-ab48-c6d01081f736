FROM python:3.11-slim

# 本地 ERP 分页快照源仅使用 Python 标准库，无需第三方依赖
WORKDIR /srv/erp
COPY erp_source/server.py /srv/erp/server.py

ENV ERP_PORT=9000 \
    ERP_PAGE_SIZE=5 \
    ERP_DELAY_MS=300

EXPOSE 9000
CMD ["python", "-u", "server.py"]
