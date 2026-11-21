#!/bin/bash
set -e

echo "=========================================================="
echo " 1. Instalando Docker, Docker Compose, Java 17 y Maven"
echo "=========================================================="

if command -v apt-get >/dev/null 2>&1; then
    sudo apt-get update -y
    sudo apt-get install -y docker.io openjdk-17-jdk maven curl git
    sudo systemctl enable --now docker
    sudo usermod -aG docker $USER || true
elif command -v dnf >/dev/null 2>&1; then
    sudo dnf update -y
    sudo dnf install -y docker java-17-amazon-corretto-devel maven curl git
    sudo systemctl enable --now docker
    sudo usermod -aG docker $USER || true
fi

if [ ! -f /usr/local/bin/docker-compose ]; then
    sudo curl -SL "https://github.com/docker/compose/releases/download/v2.29.2/docker-compose-linux-x86_64" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
fi

echo "=========================================================="
echo " 2. Levantando RabbitMQ (2 nodos), Zookeeper (3), Kafka (3) y Kafka-UI"
echo "=========================================================="
sudo /usr/local/bin/docker-compose up -d

echo "Esperando 25 segundos a que inicien los nodos de RabbitMQ y Kafka..."
sleep 25

echo "Uniendo rabbit-node-2 al cluster con rabbit-node-1..."
sudo docker exec rabbit-node-2 rabbitmqctl stop_app || true
sudo docker exec rabbit-node-2 rabbitmqctl reset || true
sudo docker exec rabbit-node-2 rabbitmqctl join_cluster rabbit@rabbit-node-1 || true
sudo docker exec rabbit-node-2 rabbitmqctl start_app || true

echo "=========================================================="
echo " Infraestructura activa:"
echo " - RabbitMQ UI: http://<IP_EC2>:15672 (guest / guest)"
echo " - Kafka UI:    http://<IP_EC2>:8090"
echo "=========================================================="
sudo docker ps
