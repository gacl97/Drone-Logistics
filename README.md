

Docker zookeeper:

    docker run -d \
    --name zookeeper --network kafka-net \
    -p 2181:2181 \
    -e ZOOKEEPER_CLIENT_PORT=2181 \
    -e ZOOKEEPER_TICK_TIME=2000 \
    confluentinc/cp-zookeeper:7.5.0

Docker kafka:

    docker run -d --name kafka --network kafka-net -p 9092:9092 \
    -e KAFKA_BROKER_ID=1 \
    -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
    -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    confluentinc/cp-kafka:7.5.0

Docker schema-registry:

    docker run -d --name schema-registry --network kafka-net -p 8081:8081 \
    -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://localhost:9092 \
    -e SCHEMA_REGISTRY_HOST_NAME=localhost \
    -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 \
    confluentinc/cp-schema-registry:7.5.0