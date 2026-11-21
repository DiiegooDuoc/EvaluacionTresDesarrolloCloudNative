# Sistema Cloud Native: Pedidos, RabbitMQ & Streaming Apache Kafka

Evolución del Sistema de Gestión de Pedidos integrando **Apache Kafka** (3 nodos Zookeeper, 3 brokers Kafka y Kafka-UI) para procesamiento de eventos en streaming y analítica en tiempo real, complementando la mensajería asíncrona de **RabbitMQ** (clúster de 2 nodos, exchanges `direct` y `topic`, colas y DLQ) y manteniendo la autenticación OAuth 2.0 / OpenID Connect con **Microsoft Entra ID**.

---

## Estructura del Repositorio

* `docker-compose.yml`: Orquestación completa (2 nodos RabbitMQ + 3 nodos Zookeeper + 3 brokers Kafka + Kafka-UI).
* `docker-compose.rabbitmq.yml`: Archivo modular para el clúster RabbitMQ.
* `docker-compose.kafka.yml`: Archivo modular para Zookeeper, brokers Kafka y Kafka-UI.
* `spring-resource-server`: Microservicio principal (puerto `8080`) con gestión de pedidos, productores/consumidores de RabbitMQ y Kafka, analítica en tiempo real y endpoints de administración.
* `ms-rabbit-admin`: Microservicio administrador de RabbitMQ (puerto `8081`).
* `ms-kafka-admin`: Microservicio administrador de Apache Kafka (puerto `8082`) para gestión de tópicos, particiones, retención y consumer groups.
* `react-msal`: Frontend SPA en React + TypeScript con integración MSAL (Microsoft Entra ID).

---

## Ejecución del Proyecto

### 1. Infraestructura en Contenedores (RabbitMQ + Kafka + Kafka-UI)

```bash
docker-compose up -d
```

* **RabbitMQ Management UI:** `http://localhost:15672` (`guest` / `guest`)
* **Kafka UI:** `http://localhost:8090`

### 2. Microservicios Backend (Spring Boot)

```bash
cd spring-resource-server
mvn clean spring-boot:run
```

*(Opcional) Para levantar los microservicios administradores dedicados por separado:*
```bash
cd ms-rabbit-admin && mvn spring-boot:run
cd ms-kafka-admin && mvn spring-boot:run
```

### 3. Frontend (React + MSAL)

```bash
cd react-msal
npm install
npm run dev
```

Disponible en `http://localhost:5173`.
