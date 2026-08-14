#!/usr/bin/env python3
"""
Generate a test CSV with N rows for acceptance testing.

Usage:
    python scripts/generate_test_data.py --rows 100000
    python scripts/generate_test_data.py --rows 100000 --output data/test/big_catalog.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import string
import sys
import time

# ── Vocabularies for realistic-looking data ──────────────────────────

ADJECTIVES = [
    "Premium", "Ultra", "Pro", "Eco", "Smart", "Classic", "Deluxe", "Essential",
    "Advanced", "Basic", "Elite", "Compact", "Heavy-Duty", "Portable", "Wireless",
    "Organic", "Natural", "Vintage", "Modern", "Sleek", "Ergonomic", "Lightweight",
    "Durable", "Waterproof", "High-Performance", "Budget", "Luxury", "Industrial",
]

PRODUCTS = [
    "Laptop", "Headphones", "Mouse", "Keyboard", "Monitor", "Speaker", "Camera",
    "Tablet", "Phone Case", "Charger", "Cable", "Backpack", "Desk Lamp", "Chair",
    "Microphone", "Webcam", "Router", "Hard Drive", "Flash Drive", "Adapter",
    "Sneakers", "T-Shirt", "Jacket", "Watch", "Sunglasses", "Wallet", "Belt",
    "Coffee Maker", "Blender", "Toaster", "Knife Set", "Pan", "Cutting Board",
    "Yoga Mat", "Dumbbell", "Resistance Band", "Water Bottle", "Protein Powder",
    "Shampoo", "Face Cream", "Toothbrush", "Vitamins", "Essential Oil", "Candle",
    "Book", "Notebook", "Pen Set", "Planner", "Sticker Pack", "Poster",
]

BRANDS = [
    "TechVault", "NovaPeak", "EcoSphere", "ZenithGear", "PulseDrive", "CoreFlex",
    "ApexWave", "SummitEdge", "VoltCraft", "QuantumLeap", "BlueRidge", "IronPeak",
    "CloudNine", "PixelForge", "SonicBloom", "AquaPure", "TerraFirm", "LunarTech",
    "StarLine", "OmniCore", "BrightPath", "SwiftEdge", "NexGen", "CrystalClear",
]

CATEGORIES = [
    "Electronics", "Computers", "Audio", "Wearables", "Mobile Accessories",
    "Home & Kitchen", "Sports & Outdoors", "Health & Wellness", "Beauty",
    "Books & Stationery", "Fashion", "Toys & Games", "Automotive", "Office",
]

DESCRIPTION_TEMPLATES = [
    "Experience the {adj} {product} from {brand}. Designed for {use_case}, this product delivers {benefit}. "
    "Built with {material} for lasting durability. Customer rating: {rating}/5.",
    "The {brand} {adj} {product} is the perfect choice for {use_case}. Featuring {feature}, "
    "it provides {benefit}. Available in {color}. Free shipping on orders over $50.",
    "{adj} {product} by {brand}. {feature} makes this ideal for {use_case}. "
    "Compact design, {material} construction. {benefit}. 30-day money-back guarantee.",
    "Introducing the all-new {brand} {product}. {adj} design meets {feature}. "
    "Perfect for {use_case}. Made with premium {material}. {benefit}.",
]

USE_CASES = [
    "everyday use", "professional work", "outdoor adventures", "home office",
    "travel", "fitness training", "creative projects", "gaming sessions",
    "kitchen tasks", "personal care", "studying", "commuting",
]
BENEFITS = [
    "exceptional performance", "unmatched comfort", "superior quality",
    "incredible value", "long-lasting reliability", "cutting-edge technology",
    "sleek aesthetics", "effortless usability",
]
MATERIALS = [
    "aluminum alloy", "organic cotton", "recycled plastic", "stainless steel",
    "bamboo fiber", "tempered glass", "carbon fiber", "silicone",
]
FEATURES = [
    "noise cancellation", "fast charging", "4K resolution", "Bluetooth 5.3",
    "AI-powered optimization", "ergonomic grip", "anti-slip base", "UV protection",
]
COLORS = [
    "Midnight Black", "Arctic White", "Ocean Blue", "Forest Green",
    "Rose Gold", "Space Gray", "Coral Red", "Sunset Orange",
]


def generate_sku(index: int) -> str:
    """Generate a realistic-looking SKU."""
    prefix = random.choice(["B0", "X0", "A0", "P0", "SK"])
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=7))
    return f"{prefix}{suffix}-{index}"


def generate_row(index: int) -> dict:
    """Generate a single realistic product row."""
    adj = random.choice(ADJECTIVES)
    product = random.choice(PRODUCTS)
    brand = random.choice(BRANDS)
    name = f"{adj} {product}"

    template = random.choice(DESCRIPTION_TEMPLATES)
    description = template.format(
        adj=adj,
        product=product,
        brand=brand,
        use_case=random.choice(USE_CASES),
        benefit=random.choice(BENEFITS),
        material=random.choice(MATERIALS),
        feature=random.choice(FEATURES),
        rating=round(random.uniform(3.5, 5.0), 1),
        color=random.choice(COLORS),
    )

    price = round(random.uniform(4.99, 999.99), 2)
    has_image = random.random() > 0.05  # 5% missing images

    return {
        "sku": generate_sku(index),
        "name": name,
        "price": price if random.random() > 0.02 else "",  # 2% missing prices
        "description": description if random.random() > 0.03 else "",  # 3% empty
        "image_url": f"https://cdn.example.com/products/{index}.jpg" if has_image else "",
        "category": random.choice(CATEGORIES),
        "brand": brand,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate test CSV data for ingestion engine acceptance testing."
    )
    parser.add_argument(
        "--rows", "-n",
        type=int,
        default=100_000,
        help="Number of rows to generate (default: 100,000)",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default="data/test/test_100k.csv",
        help="Output file path (default: data/test/test_100k.csv)",
    )
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    print(f"Generating {args.rows:,} rows → {args.output}")
    start = time.monotonic()

    fieldnames = ["sku", "name", "price", "description", "image_url", "category", "brand"]
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(args.rows):
            writer.writerow(generate_row(i))
            if (i + 1) % 25_000 == 0:
                print(f"  ... {i + 1:,} rows written")

    elapsed = time.monotonic() - start
    file_size_mb = os.path.getsize(args.output) / (1024 * 1024)
    print(f"Done in {elapsed:.1f}s — {file_size_mb:.1f} MB written to {args.output}")


if __name__ == "__main__":
    main()
