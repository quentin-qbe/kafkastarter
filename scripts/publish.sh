#!/bin/bash

BASE_URL="http://localhost:8080/kafkastarter"

check_response() {
  local name="$1"
  local code="$2"

  if [ "$code" = "201" ]; then
    echo "✅ $name -> HTTP $code"
  else
    echo "❌ $name -> HTTP $code"
  fi
}

echo "Publishing PRODUCT..."

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${BASE_URL}/products" \
  -H "Content-Type: application/json" \
  -d '{
    "productId": "P001",
    "sku": "KB001",
    "name": "Keyboard",
    "brand": "Microsoft",
    "category": "Accessories",
    "active": true,
    "alertThreshold": 50,
    "createdAt": "2026-08-26T09:00:00Z"
  }')

check_response "PRODUCT" "$HTTP_CODE"

echo
echo "Publishing PRICE..."

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${BASE_URL}/prices" \
  -H "Content-Type: application/json" \
  -d '{
    "productId": "P001",
    "amount": 49.99,
    "currency": "EUR",
    "lastUpdatedAt": "2026-08-26T09:01:00Z"
  }')

check_response "PRICE" "$HTTP_CODE"

echo
echo "Publishing STOCK..."

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${BASE_URL}/stocks" \
  -H "Content-Type: application/json" \
  -d '{
    "productId": "P001",
    "availableQuantity": 120,
    "reservedQuantity": 10,
    "warehouseCode": "WH001",
    "lastUpdatedAt": "2026-08-26T09:02:00Z"
  }')

check_response "STOCK" "$HTTP_CODE"

echo
echo "Publishing SUPPLIER..."

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${BASE_URL}/suppliers" \
  -H "Content-Type: application/json" \
  -d '{
    "productId": "P001",
    "supplierId": "SUP001",
    "supplierName": "TechSupplier",
    "supplierCountry": "France",
    "lastUpdatedAt": "2026-08-26T09:03:00Z"
  }')

check_response "SUPPLIER" "$HTTP_CODE"

echo
echo "Publishing MARKETING..."

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${BASE_URL}/marketing" \
  -H "Content-Type: application/json" \
  -d '{
    "productId": "P001",
    "shortDescription": "Wireless Keyboard",
    "longDescription": "Ergonomic wireless keyboard",
    "tags": [
      "office",
      "wireless"
    ],
    "lastUpdatedAt": "2026-08-26T09:04:00Z"
  }')

check_response "MARKETING" "$HTTP_CODE"

echo
echo "Done."