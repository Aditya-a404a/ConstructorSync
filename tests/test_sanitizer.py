"""
Tests for the SanitizerStage and HTML/CSS sanitization logic.
"""

from __future__ import annotations

import pytest

from constructsync.engine.sanitizer import SanitizerStage, ContentStripper, CustomCSSSanitizer, normalize_entities


def test_content_stripper():
    """Verify that ContentStripper strips targeted tags and their content completely."""
    stripper = ContentStripper({"script", "iframe"})
    
    # 1. Simple script tag
    stripper.feed("Hello <script>alert('xss')</script> World")
    assert stripper.get_content() == "Hello  World"
    assert stripper.tags_stripped_count == 1
    
    # 2. Nested iframe inside script
    stripper2 = ContentStripper({"script", "iframe"})
    stripper2.feed("Start <script>alert(1); <iframe>nested frame</iframe></script> End")
    assert stripper2.get_content() == "Start  End"
    assert stripper2.tags_stripped_count == 1  # Top-level script counts as 1 strip action
    
    # 3. Multiple tags
    stripper3 = ContentStripper({"script", "iframe"})
    stripper3.feed("A <script>s1</script> B <iframe>f1</iframe> C")
    assert stripper3.get_content() == "A  B  C"
    assert stripper3.tags_stripped_count == 2


def test_custom_css_sanitizer():
    """Verify that CustomCSSSanitizer strips dangerous properties while preserving safe ones."""
    sanitizer = CustomCSSSanitizer()
    
    # Safe styles preserved
    assert sanitizer.sanitize_css("color: red; font-weight: bold;") == "color: red; font-weight: bold;"
    
    # Dangerous expression stripped
    assert sanitizer.sanitize_css("color: blue; background: expression(alert(1));") == "color: blue;"
    
    # Dangerous javascript link in background-image stripped
    assert sanitizer.sanitize_css("background-image: url(javascript:alert(1)); margin: 10px;") == "margin: 10px;"
    
    # Empty style if all properties are dangerous
    assert sanitizer.sanitize_css("behavior: url(test.htc); @import url('xss.css');") == ""


def test_normalize_entities():
    """Verify double-encoded HTML entity normalization."""
    # Single level double encoding
    text1, count1 = normalize_entities("&amp;lt; &amp;gt; &amp;amp;")
    assert text1 == "&lt; &gt; &amp;"
    assert count1 == 3
    
    # Multi-level nesting (nested loop should reduce it fully)
    text2, count2 = normalize_entities("&amp;amp;lt;")
    assert text2 == "&lt;"
    assert count2 == 2


def test_sanitizer_html_cleaning():
    """Verify the end-to-end HTML sanitization logic of SanitizerStage."""
    sanitizer = SanitizerStage()
    
    # 1. Allowed formatting kept
    html_ok = "Hello <b>World</b>, this is <strong>bold</strong> and <span>colored</span>."
    clean, tags, encoded, double = sanitizer.sanitize_html(html_ok)
    assert clean == html_ok
    assert tags == 0
    assert encoded == 0
    assert double == 0
    
    # 2. Script stripped with contents, div stripped but content kept
    html_bad = "<div>Text in div</div> <script>alert(1)</script> <span>Tag</span>"
    clean, tags, encoded, double = sanitizer.sanitize_html(html_bad)
    # Bleach strips <div> and </div> but keeps content. ContentStripper removes <script>...
    assert clean == "Text in div  <span>Tag</span>"
    assert tags == 3  # script (1) + div start (1) + div end (1)
    
    # 3. Event handlers removed
    html_events = "<span onclick=\"alert(1)\" style=\"color: red;\">Click me</span>"
    clean, tags, encoded, double = sanitizer.sanitize_html(html_events)
    # onclick event handler is stripped. Style is kept because color is safe.
    assert "onclick" not in clean
    assert "style=\"color: red;\"" in clean
    
    # 4. Dangerous style expressions removed
    html_expr = "<span style=\"color: blue; background: expression(alert(1));\">Text</span>"
    clean, tags, encoded, double = sanitizer.sanitize_html(html_expr)
    assert "background: expression" not in clean
    assert "color: blue;" in clean


def test_bare_characters_encoding():
    """Verify that bare < and > are entity-encoded instead of being stripped."""
    sanitizer = SanitizerStage()
    
    # Bare less-than and greater-than
    clean, tags, encoded, double = sanitizer.sanitize_html("Response time < 2ms for ages > 18.")
    assert clean == "Response time &lt; 2ms for ages &gt; 18."
    assert encoded == 2
    assert tags == 0


def test_identifier_preservation():
    """Verify that identifier fields (IDs/SKUs) are NOT HTML sanitized."""
    sanitizer = SanitizerStage()
    
    batch = [{
        "id": "SKU-<ABC>-123",
        "name": "Widget",
        "data": {
            "description": "Safe text <script>alert(1)</script>",
            "sku": "SKU-<XYZ>-999",
        }
    }]
    
    import asyncio
    cleaned = asyncio.run(sanitizer.process(batch))
    
    assert len(cleaned) == 1
    # Check that identifiers are preserved exactly (brackets not stripped/encoded)
    assert cleaned[0]["id"] == "SKU-<ABC>-123"
    assert cleaned[0]["data"]["sku"] == "SKU-<XYZ>-999"
    
    # Check that text description is cleaned
    assert "script" not in cleaned[0]["data"]["description"]


def test_field_validation():
    """Verify that invalid IDs, numerics, and URLs filter out items."""
    sanitizer = SanitizerStage()
    
    batch = [
        # 1. Valid item
        {
            "id": "ID-001",
            "name": "Item 1",
            "data": {
                "price": "99.99",
                "image_url": "https://example.com/img.jpg",
                "description": "Safe",
            }
        },
        # 2. Invalid ID (empty)
        {
            "id": "",
            "name": "Item 2",
            "data": {
                "price": "10.00",
                "image_url": "https://example.com/img.jpg",
                "description": "Safe",
            }
        },
        # 3. Invalid numeric price
        {
            "id": "ID-003",
            "name": "Item 3",
            "data": {
                "price": "not-a-number",
                "image_url": "https://example.com/img.jpg",
                "description": "Safe",
            }
        },
        # 4. Invalid URL (javascript exploit)
        {
            "id": "ID-004",
            "name": "Item 4",
            "data": {
                "price": "5.00",
                "image_url": "javascript:alert('xss')",
                "description": "Safe",
            }
        },
        # 5. Optional missing values (should pass)
        {
            "id": "ID-005",
            "name": "Item 5",
            "data": {
                "price": "",  # Empty optional numeric
                "image_url": None,  # Missing optional URL
                "description": "Safe",
            }
        }
    ]
    
    import asyncio
    cleaned = asyncio.run(sanitizer.process(batch))
    
    # Items 1 and 5 should pass. Items 2, 3, and 4 should be dropped.
    assert len(cleaned) == 2
    assert cleaned[0]["id"] == "ID-001"
    assert cleaned[1]["id"] == "ID-005"
    
    # Cast check: Item 1's price should be cast to float/int
    assert isinstance(cleaned[0]["data"]["price"], float)
    assert cleaned[0]["data"]["price"] == 99.99
    
    # Stats checks
    assert sanitizer.stats["items_processed"] == 5
    assert sanitizer.stats["items_failed_validation"] == 3
