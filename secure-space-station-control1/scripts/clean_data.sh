#!/bin/bash

set -e

cat > data/state.json << 'EOF'
{
  "gateways": {
    "GATEWAY-1": {
      "gateway_id": "GATEWAY-1",
      "inner_door": "closed",
      "outer_door": "closed",
      "state": "closed",
      "blocked": false
    }
  },
  "compartments": {
    "COMPARTMENT-1": {
      "pressure": 100,
      "pressure_normal": true,
      "sealed": true,
      "temperature_normal": true
    },
    "REACTOR-SECTOR": {
      "pressure": 100,
      "pressure_normal": true,
      "sealed": true,
      "temperature_normal": true
    }
  },
  "docking": {
    "DOCKING-PORT-1": {
      "status": "free",
      "node_status": "normal"
    }
  },
  "communication": {
    "earth": {
      "connection_status": "connected",
      "last_command_id": null
    },
    "ships": {}
  },
  "emergency": {
    "safe_mode": false,
    "emergency_mode": false,
    "reason": null
  }
}
EOF

: > data/journal.log

echo "Data cleaned"