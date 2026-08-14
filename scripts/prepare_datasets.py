#!/usr/bin/env python3
import argparse
import os
import polars as pl

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

def process_dataset(dataset_name: str, sample_size: int = None):
    if dataset_name == "local-demo":
        raw_path = os.path.join(RAW_DIR, "demo_products.csv")
        processed_path = os.path.join(PROCESSED_DIR, "demo_products.csv")
    else:
        dataset_dir = os.path.join(RAW_DIR, dataset_name)
        if not os.path.exists(dataset_dir) or not os.listdir(dataset_dir):
            raise FileNotFoundError(f"Raw files for dataset '{dataset_name}' not found. Run download_datasets.py first.")
            
        csv_files = []
        for root, _, files in os.walk(dataset_dir):
            for f in files:
                if f.endswith(".csv"):
                    csv_files.append(os.path.join(root, f))
                    
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found recursively in raw dataset directory: {dataset_dir}")
            
        raw_path = csv_files[0]
        processed_path = os.path.join(PROCESSED_DIR, f"{dataset_name.replace('-', '_')}.csv")
        
    print(f"Preprocessing dataset '{dataset_name}' from {raw_path}...")
    
    # Read the dataset using Polars
    # We do NOT use ignore_errors=True so that any syntax/quoting bugs raise exceptions
    df = pl.read_csv(raw_path, truncate_ragged_lines=True)
    
    # Priority selectors for mapping columns dynamically
    cols = df.columns
    
    def find_best_match(keywords, col_list):
        for col in col_list:
            if col.lower() in keywords:
                return col
        for col in col_list:
            for kw in keywords:
                if kw in col.lower():
                    return col
        return None

    sku_col = find_best_match(["uniq id", "uniq_id", "asin", "sku", "id"], cols)
    name_col = find_best_match(["product name", "title", "name"], cols)
    desc_col = find_best_match(["about product", "description", "about", "about_product"], cols)
    price_col = find_best_match(["selling price", "price", "selling_price"], cols)
    image_col = find_best_match(["image", "image_url", "imageurl", "main_image"], cols)
    cat_col = find_best_match(["category", "categoryname", "categories"], cols)

    rename_map = {}
    if sku_col: rename_map[sku_col] = "sku"
    if name_col: rename_map[name_col] = "name"
    if desc_col: rename_map[desc_col] = "description"
    if price_col: rename_map[price_col] = "price"
    if image_col: rename_map[image_col] = "image_url"
    if cat_col: rename_map[cat_col] = "category"
            
    df = df.rename(rename_map)
    
    # Ensure all required schema fields are present
    required = ["sku", "name", "price", "description", "image_url", "category"]
    for col in required:
        if col not in df.columns:
            df = df.with_columns(pl.lit("").alias(col))
            
    # Clean price: cast to string, keep only digits and dots, cast to float
    df = df.with_columns(
        pl.col("price")
        .cast(pl.String)
        .str.replace_all(r"[^\d.]", "")
        .cast(pl.Float64, strict=False)
    )
    
    # Select only the required columns to conform strictly to Constructor schema
    df = df.select(required)
    
    # Apply sampling if requested
    if sample_size:
        df = df.head(sample_size)
        
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    df.write_csv(processed_path)
    print(f"Processed file written to {processed_path}. Shape: {df.shape}")

def main():
    parser = argparse.ArgumentParser(description="Clean and map e-commerce datasets to Constructor API format.")
    parser.add_argument(
        "--dataset",
        type=str,
        choices=["amazon-2020", "amazon-uk-2023", "dirty-ecommerce", "local-demo"],
        default="local-demo",
        help="Specify which dataset to preprocess."
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=None,
        help="Limit output to N rows for quick testing."
    )
    args = parser.parse_args()

    try:
        process_dataset(args.dataset, args.sample)
    except Exception as e:
        print(f"Error during preprocessing: {str(e)}")

if __name__ == "__main__":
    main()
