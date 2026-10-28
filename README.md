# POC · Activación de Servicios con Kafka

Sistemas Distribuidos 2026 · Kafka (KRaft) + NestJS + React + MongoDB

Un cliente contrata un plan desde la UI. `activation-api` publica **un único evento** (`ActivationRequested`) y, a partir de ahí, facturación, aprovisionamiento, notificación y CRM reaccionan por su cuenta. **Ningún servicio llama directamente a otro**: todo se coordina por eventos en Kafka.

## Cómo levantarlo

Requisitos: **Docker Desktop** (o Docker Engine + Compose v2), con unos 4 GB de RAM libres. No hace falta tener Node instalado.

```bash
docker compose up --build
```

La primera vez tarda unos minutos porque descarga las imágenes y compila los servicios. Cuando en los logs aparece `activation-api | Escuchando en http://localhost:3000`, está todo listo.

| Qué | URL |
|---|---|
| **Demo UI** (contratar planes y ver la timeline en vivo) | http://localhost:5173 |
| Kafka UI (topics, mensajes, particiones, consumer groups, lag) | http://localhost:8080 |
| Mailhog (bandeja de los emails enviados) | http://localhost:8025 |
| API REST | http://localhost:3000/activations |

Para apagar todo y borrar los datos: `docker compose down`.

## Prueba rápida

1. Abrir http://localhost:5173 y tocar **Contratar plan** (sin fallo). La timeline muestra 4 eventos en verde y la activación queda **Activa**. En Mailhog aparece el email de bienvenida.
2. Elegir **Forzar fallo → Provisioning** y contratar. Aparecen `ProvisioningFailed`, `ActivationFailed` y `BillingAccountCancelled` (la compensación), y en Mailhog el email de error.

También hay una prueba automática de los criterios de aceptación (necesita bash: Linux, macOS, Git Bash o WSL):

```bash
bash docker/smoke-test.sh
```

El guion completo de los 5 escenarios está en [`docs/demo-script.md`](docs/demo-script.md).

## API

| Método | Ruta | Descripción |
|---|---|---|
| `POST` | `/activations` | Body `{ customerId, planId, simulateFailure: "none" \| "billing" \| "provisioning" }`. Responde **202** con `activationId` y estado `PENDING`. |
| `GET` | `/activations/:id` | Estado actual, resultado de cada paso e historial de eventos. |
| `GET` | `/activations` | Últimas 50 activaciones. |
| `POST` | `/demo/replay/:id` | Reenvía el `ActivationRequested` original (mismo `eventId`) para probar la idempotencia. |
| `POST` | `/demo/poison` | Publica un mensaje inválido para probar los reintentos y la DLQ. |

WebSocket (socket.io) en el mismo puerto: evento `activation.updated` con la activación completa en cada cambio.

## Estructura

```
activation-poc/
├── apps/
│   ├── activation-api/          REST + WebSocket + agregador de la saga
│   ├── billing-service/         crea y anula cuentas (compensación)
│   ├── provisioning-service/    aprovisiona la línea (demora de 1 a 3 s)
│   ├── notification-service/    emails a Mailhog
│   ├── crm-analytics-service/   escucha todos los topics, no publica nada
│   ├── loyalty-service/         escenario 5: consumidor nuevo que lee el historial
│   └── demo-ui/                 React + Vite + socket.io-client
├── libs/
│   ├── contracts/               sobre común, tipos de eventos, nombres de topics (versionados)
│   └── kafka-toolkit/           idempotencia, reintentos, DLQ, outbox, conexión a Mongo y Kafka
├── docker/
│   ├── create-topics.sh         crea los topics con 3 particiones
│   ├── Dockerfile.service       imagen única para los servicios NestJS
│   └── smoke-test.sh            prueba automática de los criterios de aceptación
├── docker-compose.yml
└── docs/
    ├── architecture.md
    └── demo-script.md
```

## Qué cubre

- **Fases 1 a 3 completas** del plan: camino feliz, UI con timeline en vivo, fallos forzados, compensación, idempotencia, reintentos con DLQ, crm-analytics y loyalty.
- **Fase 4, parcial:** el **outbox transaccional** está implementado en activation-api, billing y provisioning. Schema Registry y OpenTelemetry quedan fuera.
- Todos los requerimientos funcionales (RF-01 a RF-10) y no funcionales (RNF-01 a RNF-07). El detalle de cada uno está en [`docs/architecture.md`](docs/architecture.md).

## Problemas comunes

- **Un puerto ya está en uso** (3000, 5173, 8080, 8025, 9092 o 27017): cerrar el programa que lo usa o cambiar el puerto del lado izquierdo en `docker-compose.yml`.
- **Mac con Apple Silicon:** Mailhog sólo publica imagen amd64 y corre emulada (`platform: linux/amd64`). Funciona igual con Docker Desktop.
- **Un servicio arrancó antes que Kafka:** los servicios tienen `restart: on-failure` y se reintentan solos.
- **Empezar de cero:** `docker compose down && docker compose up --build`.

## Desarrollo local (opcional)

Con la infraestructura en Docker (`docker compose up kafka kafka-init mongo mailhog kafka-ui`), cada servicio se puede correr fuera de Docker con `npm install` y `npm run start:<servicio>`. Por defecto usan `localhost:9092` y `mongodb://localhost:27017/?replicaSet=rs0&directConnection=true`. Para la UI: `cd apps/demo-ui && npm install && npm run dev`. Los tipos se verifican con `npm run typecheck`.
