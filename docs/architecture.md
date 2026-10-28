# Arquitectura

`activation-api` es el único punto de entrada; todo lo demás reacciona a eventos en Kafka.

```
                 POST /activations           WebSocket (activation.updated)
   Demo UI ───────────────────────► activation-api ◄──────────────► MongoDB (activation_db)
                                        │      ▲                     activations · outbox · processed_events
                    ActivationRequested │      │ resultados
                                        ▼      │
  ┌──────────────────────────── Kafka (KRaft, 1 broker) ─────────────────────────────┐
  │  activation.requested   billing.events   provisioning.events   activation.events │
  │  (3 particiones c/u, clave = customerId, retención 7 días, + <topic>.dlq)        │
  └──────────────────────────────────────────────────────────────────────────────────┘
        │         ▲            │          ▲                 │                │
        ▼         │            ▼          │                 ▼                ▼
     billing ─────┘       provisioning ───┘           notification     crm-analytics
  (crea / anula cuenta)  (aprovisiona línea)        (email a Mailhog)  (sólo escucha)
```

## Topics y consumidores

| Topic | Eventos | Productor | Consumer groups |
|---|---|---|---|
| `activation.requested` | ActivationRequested | activation-api | `billing-svc`, `provisioning-svc`, `crm-analytics` |
| `billing.events` | BillingAccountCreated, BillingFailed, BillingAccountCancelled | billing | `activation-api`, `crm-analytics` |
| `provisioning.events` | ProvisioningCompleted, ProvisioningFailed | provisioning | `activation-api`, `crm-analytics` |
| `activation.events` | ActivationCompleted, ActivationFailed | activation-api | `notification-svc`, `billing-svc`, `crm-analytics`, `loyalty-svc` |
| `<topic>.dlq` | mensajes que fallaron 3 reintentos | el consumidor que falló | inspección manual en Kafka UI |

Sobre común de todos los eventos (`libs/contracts`): `eventId`, `eventType`, `version`, `occurredAt`, `correlationId` (= activationId), `customerId` (= clave del mensaje), `source`, `payload`. Además se copian como headers de Kafka `eventId`, `eventType`, `version`, `correlationId` y `source`, para poder filtrar en Kafka UI.

## Saga: coreografía con agregador

1. `POST /activations` guarda la activación `PENDING` y el evento en la outbox (misma transacción) y responde **202**.
2. Billing y provisioning consumen el **mismo** `ActivationRequested` en paralelo y publican su resultado.
3. activation-api guarda cada resultado en `steps` y aplica la regla de agregación:
   - los dos OK → `ACTIVE` y publica `ActivationCompleted`
   - el primer fallo → `FAILED` y publica `ActivationFailed`
   - un resultado que llega con la activación ya en estado final sólo se registra en el historial
4. Notification manda el email que corresponde. Billing, si recibe `ActivationFailed` y tenía una cuenta creada, la anula (`BillingAccountCancelled`).

Estados: `PENDING → IN_PROGRESS → ACTIVE | FAILED`.

**Caso borde:** si `ActivationFailed` llega a billing antes que `ActivationRequested`, billing deja una marca `NOT_CREATED / failureSeen`. Cuando después procesa el pedido, crea la cuenta y la anula en el mismo paso.

**Concurrencia:** activation-api procesa las particiones en paralelo, así que los resultados de billing y provisioning de una misma activación pueden llegar a la vez. Las dos transacciones escriben el mismo documento: Mongo detecta el conflicto, aborta una y `withTransaction` la reintenta, y en el reintento ya ve el resultado de la otra.

## Decisiones técnicas

### 1. Idempotencia en cada consumidor (`libs/kafka-toolkit/src/event-consumer.service.ts`)
Kafka entrega "al menos una vez". Cada consumidor inserta el `eventId` en `processed_events` (el `_id` es único y hay un TTL de 7 días) **en la misma transacción** que su efecto. Si ya existía, descarta el evento y confirma el offset. Como segunda defensa, cada servicio tiene índices únicos de negocio (`billing_accounts.activationId`, `provisioning_orders.activationId`, `notifications.{activationId, eventType}`).

### 2. Reintentos y DLQ
Ante un error técnico, el consumidor reintenta 3 veces con espera creciente (1 s, 2 s, 4 s) mandando heartbeats. Si sigue fallando, publica el mensaje **original** en `<topic>.dlq` con headers `dlq-reason`, `dlq-original-topic`, `dlq-original-partition`, `dlq-original-offset`, `dlq-consumer-group` y `dlq-attempts`, y confirma el offset para seguir con el siguiente. Los fallos de negocio simulados (`simulateFailure`) **no** son errores técnicos: se publican como `BillingFailed` o `ProvisioningFailed`.

### 3. Outbox transaccional (`outbox.service.ts`)
El cambio de estado y el evento a publicar se guardan en la misma transacción de Mongo. Un relay publica los pendientes (cada 500 ms, y además enseguida después de cada transacción) y los marca como `SENT`. Cada evento se toma de forma atómica (`PENDING → PUBLISHING`), así que dos instancias no publican el mismo a la vez. Si una instancia muere a mitad de camino, el evento se retoma a los 10 s.

### 4. Cliente Kafka
Se usa el transporte Kafka de `@nestjs/microservices`: `@EventPattern` para consumir y `ClientKafka` para publicar, sobre kafkajs. Todo está aislado en `libs/kafka-toolkit` para poder cambiar el cliente sin tocar los servicios.

Configuración del consumidor: `autoCommit: false` (commit manual después de procesar), `fromBeginning: true` (un consumer group nuevo lee el historial), `partitionsConsumedConcurrently: 3` y `postfixId: ''` (el consumer group se llama igual que en el documento).

### Otras
- MongoDB en replica set de un nodo (necesario para las transacciones). El healthcheck de Compose lo inicializa solo.
- Una base por servicio (`activation_db`, `billing_db`, `provisioning_db`, `notification_db`, `crm_db`, `loyalty_db`); ninguno lee la base de otro.
- Healthchecks y `depends_on: condition` para que nada arranque antes que Kafka, los topics y Mongo.

## Trazabilidad de requerimientos

| ID | Dónde |
|---|---|
| RF-01 | `demo-ui/src/App.jsx`: formulario con cliente, plan y selector de fallo |
| RF-02 | `ActivationsController.create`: `@HttpCode(202)`, no espera a nadie |
| RF-03 | `ActivationsService.create`: outbox → `activation.requested` |
| RF-04 | billing y provisioning consumen el mismo topic con consumer groups distintos |
| RF-05, RF-06 | `ActivationsService.applyResult` + `BillingService.onActivationEvent` (compensación) |
| RF-07 | `NotificationService.notify` |
| RF-08 | `ActivationsGateway` (socket.io) + timeline de la UI |
| RF-09 | `GET /activations/:id` |
| RF-10 | `crm-analytics-service`: `event_log`; replay con otro consumer group |
| RNF-01 | `docker compose up` |
| RNF-02 | clave del mensaje = `customerId` (`EventPublisher.publish`) |
| RNF-03 | `processed_events` + índices únicos |
| RNF-04 | `EventConsumer`: 3 reintentos y DLQ |
| RNF-05 | commit manual después de procesar: al volver, sigue desde el último offset confirmado |
| RNF-06 | `correlationId` en el sobre y en los headers; `event_log.correlationId` indexado |
| RNF-07 | `libs/contracts` con `version` por evento y `CONTRACTS_VERSION` |

## Riesgos conocidos

- **Email duplicado en una ventana mínima:** el email se envía dentro de la transacción, antes del commit. Si el proceso muere justo entre el envío y el commit, al reprocesar se envía de nuevo. Para un piloto convendría un outbox de emails.
- **kafkajs** tiene un mantenimiento limitado. Antes de un piloto conviene evaluar el cliente oficial de Confluent para JavaScript.
- **Rebalanceo:** al escalar billing hay una pausa de algunos segundos (`sessionTimeout` está en 10 s).
