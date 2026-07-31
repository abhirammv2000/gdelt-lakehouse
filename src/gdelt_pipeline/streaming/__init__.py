"""Kafka/Redpanda streaming for the live GDELT event stream (Phase 6).

A producer fans a landed bronze batch out onto ``gdelt.events.raw`` as one message
per event; a consumer applies a per-event data-quality gate, dead-letters bad
records, and raises alerts on high-impact conflict events. JSON on the wire keeps
it dependency-light; a Schema Registry + Avro contract is the production upgrade.
"""
