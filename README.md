# local-dynamodb-ttl

Emulates the TTL functionality of DynamoDB for local development 

## Usage

### Docker Build

```bash
docker build -t local-dynamodb-ttl:1.0 .
```

### Docker Compose

```yaml
  local-dynamodb-ttl:
    image: local-dynamodb-ttl:1.0
    environment:
      - AWS_REGION=ap-southeast-2
      - AWS_ACCESS_KEY_ID=local
      - AWS_SECRET_ACCESS_KEY=local
      - TABLE_NAME=ddb-l-ase2-ms-notifications
      - TTL_ATTRIBUTE=Expires
      - AWS_ENDPOINT=http://localstack:4566
    depends_on:
      - localstack
    networks:
      - default
```
