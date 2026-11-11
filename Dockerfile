ARG PRODUCT_RUNTIME=node:22.23.0-bookworm-slim
FROM $PRODUCT_RUNTIME
USER root
WORKDIR /app
COPY . /app
ENTRYPOINT []
CMD ["sh", "demo_support/verify.sh"]
