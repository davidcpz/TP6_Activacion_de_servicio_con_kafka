#!/usr/bin/env bash
# Prueba automática de los criterios de aceptación.
# Uso (con todo levantado):   bash docker/smoke-test.sh
# Requiere: bash, curl y docker compose (en Windows: Git Bash o WSL).
set -uo pipefail

API="${API:-http://localhost:3000}"
PASS=0
FAIL=0

ok()   { echo "  ✔ $1"; PASS=$((PASS + 1)); }
bad()  { echo "  ✘ $1"; FAIL=$((FAIL + 1)); }
step() { echo; echo "▶ $1"; }

mongo() { docker compose exec -T mongo mongosh --quiet --eval "$1" 2>/dev/null | tr -d '\r' | tail -n 1; }

create() { # $1 customerId, $2 simulateFailure
  curl -s -X POST "$API/activations" -H 'Content-Type: application/json' \
    -d "{\"customerId\":\"$1\",\"planId\":\"Plan Digital\",\"simulateFailure\":\"$2\"}" |
    sed -E 's/.*"activationId":"([^"]+)".*/\1/'
}

status_of() { mongo "const a = db.getSiblingDB('activation_db').activations.findOne({_id: '$1'}); print(a ? a.status : 'NONE')"; }

wait_status() { # $1 id, $2 esperado
  local s=""
  for _ in $(seq 1 40); do
    s=$(status_of "$1")
    [ "$s" = "$2" ] && return 0
    sleep 1
  done
  echo "    (quedó en $s)"
  return 1
}

count() { # $1 db, $2 colección, $3 filtro JSON
  mongo "print(db.getSiblingDB('$1').$2.countDocuments($3))"
}

wait_count() { # $1 db, $2 colección, $3 filtro, $4 esperado
  local c=""
  for _ in $(seq 1 20); do
    c=$(count "$1" "$2" "$3")
    [ "$c" = "$4" ] && return 0
    sleep 1
  done
  echo "    (hay $c, se esperaba $4)"
  return 1
}

echo "Probando contra $API"
if ! curl -sf "$API/health" > /dev/null; then
  echo "activation-api no responde en $API/health. ¿Corriste 'docker compose up'?"
  exit 1
fi

step "Escenario 1 · Camino feliz"
HAPPY=$(create "C-1001" none)
echo "  activación $HAPPY"
wait_status "$HAPPY" ACTIVE && ok "la activación pasa a ACTIVE" || bad "la activación no llegó a ACTIVE"
wait_count billing_db billing_accounts "{activationId: '$HAPPY', status: 'CREATED'}" 1 && ok "billing creó 1 cuenta" || bad "billing no creó la cuenta"
wait_count provisioning_db provisioning_orders "{activationId: '$HAPPY', status: 'COMPLETED'}" 1 && ok "provisioning creó 1 orden" || bad "provisioning no creó la orden"
wait_count notification_db notifications "{activationId: '$HAPPY', eventType: 'ActivationCompleted'}" 1 && ok "se envió el email de bienvenida" || bad "no se envió el email de bienvenida"
wait_count crm_db event_log "{correlationId: '$HAPPY'}" 4 && ok "crm-analytics registró los 4 eventos por correlationId" || bad "crm-analytics no tiene los 4 eventos"

step "Escenario 2 · Fallo en provisioning y compensación"
PROV=$(create "C-1002" provisioning)
echo "  activación $PROV"
wait_status "$PROV" FAILED && ok "la activación pasa a FAILED" || bad "la activación no llegó a FAILED"
wait_count billing_db billing_accounts "{activationId: '$PROV', status: 'CANCELLED'}" 1 && ok "billing anuló la cuenta (BillingAccountCancelled)" || bad "billing no anuló la cuenta"
wait_count notification_db notifications "{activationId: '$PROV', eventType: 'ActivationFailed'}" 1 && ok "se envió el email de error" || bad "no se envió el email de error"

step "Fallo en billing"
BILL=$(create "C-1003" billing)
echo "  activación $BILL"
wait_status "$BILL" FAILED && ok "la activación pasa a FAILED" || bad "la activación no llegó a FAILED"
wait_count billing_db billing_accounts "{activationId: '$BILL', status: 'REJECTED'}" 1 && ok "billing rechazó sin crear cuenta" || bad "estado de billing inesperado"

step "Idempotencia · reenviar un evento ya procesado"
curl -s -X POST "$API/demo/replay/$HAPPY" > /dev/null
echo "  reenviado el ActivationRequested de $HAPPY; esperando 6 s..."
sleep 6
[ "$(count billing_db billing_accounts "{activationId: '$HAPPY'}")" = "1" ] && ok "sigue habiendo 1 sola cuenta" || bad "se duplicó la cuenta"
[ "$(count provisioning_db provisioning_orders "{activationId: '$HAPPY'}")" = "1" ] && ok "sigue habiendo 1 sola orden" || bad "se duplicó la orden"
[ "$(count notification_db notifications "{activationId: '$HAPPY'}")" = "1" ] && ok "no se repitió el email" || bad "se repitió el email"

step "DLQ · mensaje inválido"
curl -s -X POST "$API/demo/poison" -H 'Content-Type: application/json' -d '{}' > /dev/null
echo "  publicado un mensaje inválido; esperando los reintentos (1 s + 2 s + 4 s)..."
AFTER=$(create "C-1004" none)
wait_status "$AFTER" ACTIVE && ok "el resto del flujo sigue funcionando ($AFTER ACTIVE)" || bad "el flujo se trabó después del mensaje inválido"
DLQ=0
for _ in $(seq 1 20); do
  DLQ=$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:29092 \
    --topic activation.requested.dlq 2>/dev/null | tr -d '\r' | awk -F: '{s += $3} END {print s + 0}')
  [ "${DLQ:-0}" -gt 0 ] && break
  sleep 1
done
[ "${DLQ:-0}" -gt 0 ] && ok "activation.requested.dlq tiene $DLQ mensaje(s)" || bad "no llegó nada a activation.requested.dlq"

echo
echo "Resultado: $PASS OK, $FAIL con error"
[ "$FAIL" -eq 0 ]
