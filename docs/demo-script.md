# Guion de la demo (unos 15 minutos)

Pantalla dividida: **Demo UI** (http://localhost:5173) a la izquierda y **Kafka UI** (http://localhost:8080) a la derecha. Tener Mailhog (http://localhost:8025) en otra pestaña y una terminal con logs:

```bash
docker compose logs -f activation-api billing-service provisioning-service notification-service
```

## 1 · Camino feliz (fan-out)
**Qué se hace:** contratar un plan con el fallo en "Ninguno".
**Qué se ve:** la timeline muestra 4 eventos en verde (`ActivationRequested`, `BillingAccountCreated`, `ProvisioningCompleted`, `ActivationCompleted`) y la activación pasa a **Activa**. En Mailhog aparece el email de bienvenida. En Kafka UI → Topics → `activation.requested` → Messages se ve el evento, con clave = customerId.
**Concepto:** un evento, varias reacciones. Billing y provisioning leen el mismo mensaje con consumer groups distintos.

## 2 · Fallo y compensación (saga sin llamadas directas)
**Qué se hace:** "Forzar fallo → Provisioning" y contratar.
**Qué se ve:** `ProvisioningFailed` → `ActivationFailed` → `BillingAccountCancelled` (en amarillo) y un email de error en Mailhog.
**Concepto:** nadie llama a billing para que anule la cuenta: billing escucha `ActivationFailed` y se compensa solo.

## 3 · Servicio caído (offsets: nadie pierde mensajes)
```bash
docker compose stop notification-service
```
Crear 3 activaciones desde la UI. En Kafka UI → Consumers → `notification-svc` se ve un **lag de 3**. Después:
```bash
docker compose start notification-service
```
Llegan los 3 emails y el lag vuelve a 0.
**Concepto:** el offset confirmado queda guardado en Kafka. Al volver, el consumidor sigue desde ahí.

## 4 · Escalar (consumer group y particiones)
```bash
docker compose up -d --scale billing-service=2
```
En Kafka UI → Consumers → `billing-svc` se ve cómo se reparten las 3 particiones entre los 2 consumidores (por ejemplo 2 y 1). Al crear activaciones de distintos clientes, los logs muestran qué instancia procesó cada una (el hostname entre corchetes).
**Concepto:** dentro de un grupo, cada partición la consume una sola instancia. Para escalar, se suman instancias, hasta tantas como particiones. Al rebalancear hay una pausa de unos segundos.

Para volver a una instancia: `docker compose up -d --scale billing-service=1`.

## 5 · Nuevo consumidor (replay del log)
```bash
docker compose --profile loyalty up -d loyalty-service
docker compose logs -f loyalty-service
```
loyalty-service tiene un consumer group nuevo (`loyalty-svc`) con `auto.offset.reset=earliest`: lee `activation.events` **desde el inicio** y otorga puntos por todas las activaciones completadas antes de que existiera, sin tocar a ningún otro servicio.
**Concepto:** el log de Kafka se puede releer. Con APIs directas esto no se puede hacer.

---

## Extras para verificar los criterios de aceptación

**Idempotencia.** Seleccionar una activación activa y tocar **Reenviar evento duplicado**. En los logs de billing y provisioning aparece `ya fue procesado: se descarta (idempotencia)`. No se crea otra cuenta ni otra orden, y no llega otro email.

**DLQ.** Tocar **Enviar mensaje inválido → DLQ**. Los logs muestran `Intento 1/4 ... Reintento en 1 s`, después 2 s y 4 s, y finalmente `enviado a activation.requested.dlq`. En Kafka UI, el topic `activation.requested.dlq` tiene el mensaje original con los headers `dlq-reason`, `dlq-original-offset`, etc. Mientras tanto se pueden seguir creando activaciones sin problemas.

**Trazabilidad por correlationId.**
```bash
docker compose exec mongo mongosh --quiet --eval "db.getSiblingDB('crm_db').event_log.find({correlationId: 'act-XXXXXXXX'}, {eventType: 1, source: 1, occurredAt: 1}).toArray()"
```
En Kafka UI también se puede filtrar por el header `correlationId`.

**RF-10: reprocesar el historial de crm-analytics desde cero** (con otro consumer group y otra base):
```bash
docker compose run --rm -e KAFKA_GROUP_ID=crm-analytics-replay -e MONGO_DB=crm_replay_db crm-analytics-service
```

**Todo junto, automático:**
```bash
bash docker/smoke-test.sh
```

## Comandos de apoyo
```bash
docker compose ps                                  # estado de los contenedores
docker compose logs -f <servicio>                  # logs de un servicio
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:29092 --describe --all-groups
docker compose down                                # apagar y borrar todo
```
