#!/usr/bin/env python3
"""
Simulate PIM product change events and publish them to a Kafka topic.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
from aiokafka import AIOKafkaProducer

ADJECTIVES = ["Premium", "Ultra", "Pro", "Eco", "Smart", "Classic", "Deluxe", "Sleek"]
PRODUCTS = ["Laptop", "Headphones", "Monitor", "Speaker", "Phone Case", "Desk Lamp", "Yoga Mat", "Water Bottle"]
CATEGORIES = ["Electronics", "Computers", "Audio", "Home & Kitchen", "Sports & Outdoors"]
BRANDS = ["TechVault", "NovaPeak", "EcoSphere", "ApexWave", "VoltCraft"]


def generate_event(index: int) -> dict:
    """Generate a realistic PIM product.updated event."""
    adj = random.choice(ADJECTIVES)
    prod = random.choice(PRODUCTS)
    brand = random.choice(BRANDS)
    
    sku = f"SKU-KAFKA-{index:04d}"
    name = f"{adj} {prod}"
    price = round(random.uniform(9.99, 1499.99), 2)
    desc = f"Experience the incredible {adj} {prod} from {brand}. This is a highly detailed description exceeding 50 characters to score perfectly."
    
    return {
        "event": "product.updated",
        "sku": sku,
        "data": {
            "name": name,
            "price": price,
            "description": desc,
            "image_url": f"https://cdn.example.com/products/{sku}.jpg",
            "category": random.choice(CATEGORIES),
            "brand": brand,
        }
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate PIM events and publish to Kafka")
    parser.add_argument("--count", "-n", type=int, default=1000, help="Number of events to emit (default: 1000)")
    parser.add_argument("--topic", "-t", type=str, default="product-updates", help="Kafka topic name")
    parser.add_argument("--bootstrap-servers", "-b", type=str, default="localhost:9092", help="Kafka bootstrap servers")
    args = parser.parse_args()

    print(f"Emitting {args.count:,} PIM events to topic '{args.topic}' at {args.bootstrap_servers}...")

    producer = AIOKafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    
    try:
        await producer.start()
    except Exception as e:
        print(f"Error starting Kafka producer: {e}", file=sys.stderr)
        print("Is Kafka running? Try running: docker-compose up kafka", file=sys.stderr)
        sys.exit(1)

    try:
        for i in range(1, args.count + 1):
            event = generate_event(i)
            await producer.send_and_wait(args.topic, event)
            
            if i % 100 == 0:
                print(f"  Sent {i:,} / {args.count:,} events")
                # Add a tiny sleep to simulate rate flow
                await asyncio.sleep(0.05)
                
        print(f"Successfully published all {args.count:,} events!")
    except Exception as e:
        print(f"Error sending events: {e}", file=sys.stderr)
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
