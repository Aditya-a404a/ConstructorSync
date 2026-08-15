"""
Sanitization module for product description and metadata cleaning.

Ensures that HTML is cleaned securely (whitelisting safe formatting tags),
event handlers and expressions are stripped, bare tag characters are entity-encoded,
double-encoded entities are normalized, and identifier/numeric/URL fields are validated.
"""

from __future__ import annotations

import logging
import re
from html.parser import HTMLParser
from urllib.parse import urlparse
from typing import Any

import bleach

from constructsync.engine.models import PipelineStage

logger = logging.getLogger(__name__)

# whitelisted tags for text fields
ALLOWED_TAGS = ["b", "i", "strong", "em", "ul", "ol", "li", "br", "p", "span"]

# whitelisted attributes for text fields
ALLOWED_ATTRIBUTES = {
    "*": ["class", "style"]
}

# tags to be stripped WITH their contents
STRIP_WITH_CONTENT_TAGS = {"script", "iframe", "object", "embed", "applet"}


class ContentStripper(HTMLParser):
    """
    Parser that strips targeted tags along with all of their contents.
    
    Example:
        <script>alert(1)</script> -> ''
    """

    def __init__(self, tags_to_strip: set[str]) -> None:
        super().__init__()
        self.tags_to_strip = {t.lower() for t in tags_to_strip}
        self.stack: list[str] = []
        self.result: list[str] = []
        self.tags_stripped_count = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag_lower = tag.lower()
        if tag_lower in self.tags_to_strip:
            self.stack.append(tag_lower)
            self.tags_stripped_count += 1
        elif not self.stack:
            # Reconstruct start tag
            attr_str = "".join(f' {k}="{v}"' if v is not None else f' {k}' for k, v in attrs)
            self.result.append(f"<{tag}{attr_str}>")

    def handle_endtag(self, tag: str) -> None:
        tag_lower = tag.lower()
        if tag_lower in self.tags_to_strip:
            if self.stack and self.stack[-1] == tag_lower:
                self.stack.pop()
        elif not self.stack:
            self.result.append(f"</{tag}>")

    def handle_data(self, data: str) -> None:
        if not self.stack:
            self.result.append(data)

    def handle_comment(self, data: str) -> None:
        # Comments are skipped entirely (safe default)
        pass

    def get_content(self) -> str:
        return "".join(self.result)


class CustomCSSSanitizer:
    """
    CSS sanitizer for style attribute values.
    
    Allows standard styling properties but strips expressions, behavior,
    and javascript: schemes.
    """

    def sanitize_css(self, style: str) -> str:
        cleaned = []
        # Split by semicolon to inspect each style declaration
        for decl in style.split(";"):
            decl = decl.strip()
            if not decl:
                continue
            if ":" not in decl:
                continue
            
            prop, val = decl.split(":", 1)
            prop_clean = prop.strip().lower()
            val_clean = val.strip().lower()
            
            # Dangerous keywords to look out for
            dangerous = (
                "javascript:",
                "expression",
                "vbscript:",
                "data:",
                "behavior",
                "@import",
                "url(",
            )
            
            if any(d in prop_clean or d in val_clean for d in dangerous):
                continue
                
            cleaned.append(f"{prop.strip()}: {val.strip()}")
            
        return "; ".join(cleaned) + (";" if cleaned else "")


def normalize_entities(text: str) -> tuple[str, int]:
    """
    Normalize double-encoded HTML entities.
    
    Example:
        &amp;lt; -> &lt;
    """
    pattern = re.compile(r"&amp;(lt|gt|amp|quot|apos|#[0-9]+|#[xX][0-9a-fA-F]+);")
    count = 0
    prev = ""
    while prev != text:
        prev = text
        text, n = pattern.subn(r"&\1;", text)
        count += n
    return text, count


class SanitizerStage(PipelineStage):
    """
    Pipeline stage that validates types and sanitizes HTML text fields.
    
    Filters out items that fail validation for IDs, numerics, or URLs.
    Tracks detailed metrics/statistics for the run.
    """

    def __init__(
        self,
        text_fields: list[str] | None = None,
        id_fields: list[str] | None = None,
        numeric_fields: list[str] | None = None,
        url_fields: list[str] | None = None,
    ) -> None:
        # Default fields configured from project requirements
        self.text_fields = text_fields if text_fields is not None else ["description", "features", "about_product"]
        self.id_fields = id_fields if id_fields is not None else ["sku", "item_id", "group_id", "id"]
        self.numeric_fields = numeric_fields if numeric_fields is not None else ["price", "rating", "review_count"]
        self.url_fields = url_fields if url_fields is not None else ["image_url", "product_url", "url"]

        self.css_sanitizer = CustomCSSSanitizer()
        
        # Statistics
        self.stats = {
            "items_processed": 0,
            "items_sanitized": 0,
            "items_failed_validation": 0,
            "tags_stripped": 0,
            "entities_encoded": 0,
            "double_encoded_normalized": 0,
        }

    def _get_field_ref(self, item: dict, field_name: str) -> tuple[dict, str] | None:
        """
        Locate where a field is stored (top-level or in 'data' dictionary).
        
        Returns the parent dictionary and the key, or None if not found.
        """
        if field_name in item:
            return item, field_name
        if "data" in item and isinstance(item["data"], dict) and field_name in item["data"]:
            return item["data"], field_name
        return None

    def _validate_id(self, val: Any) -> bool:
        """Verify that identifier is non-empty string or integer."""
        if val is None:
            return False
        val_str = str(val).strip()
        return len(val_str) > 0

    def _validate_numeric(self, val: Any) -> tuple[bool, Any]:
        """Verify that numeric is float/int or convertible (or empty if optional)."""
        if val is None or val == "":
            return True, None
        try:
            # Try float convert
            f_val = float(val)
            # If it's mathematically an integer, preserve as int
            if f_val.is_integer():
                return True, int(f_val)
            return True, f_val
        except (ValueError, TypeError):
            return False, val

    def _validate_url(self, val: Any) -> bool:
        """Verify that URL contains a valid structure and no dangerous scheme."""
        if val is None or val == "":
            return True
        val_str = str(val).strip()
        try:
            parsed = urlparse(val_str)
            if parsed.scheme:
                return parsed.scheme.lower() in ("http", "https")
            # If no scheme, it must not look like an exploit (e.g. javascript:alert)
            return "javascript:" not in val_str.lower()
        except Exception:
            return False

    def sanitize_html(self, text: str) -> tuple[str, int, int, int]:
        """
        Perform complete HTML sanitization pipeline on string.
        
        Returns (sanitized_text, tags_stripped, entities_encoded, double_normalized).
        """
        # 1. Content Stripping (script, iframe, etc. with contents)
        stripper = ContentStripper(STRIP_WITH_CONTENT_TAGS)
        stripper.feed(text)
        stripped = stripper.get_content()
        stripper_tags = stripper.tags_stripped_count
        
        # 2. Bleach Clean
        # Counts before bleach
        pre_lt = stripped.count("<")
        pre_lt_ent = stripped.count("&lt;")
        pre_gt_ent = stripped.count("&gt;")
        
        cleaned = bleach.clean(
            stripped,
            tags=ALLOWED_TAGS,
            attributes=ALLOWED_ATTRIBUTES,
            css_sanitizer=self.css_sanitizer,
            strip=True,
            strip_comments=True,
        )
        
        # Counts after bleach to compute tags stripped & bare characters encoded
        post_lt = cleaned.count("<")
        post_lt_ent = cleaned.count("&lt;")
        post_gt_ent = cleaned.count("&gt;")
        
        # Number of bare < and > turned into &lt; and &gt;
        new_lt = max(0, post_lt_ent - pre_lt_ent)
        new_gt = max(0, post_gt_ent - pre_gt_ent)
        encoded_count = new_lt + new_gt
        
        # Bleach decreases < count for stripped tags AND encoded bare characters.
        # So tags stripped by bleach = (decrease in < count) - (bare < encoded).
        dec_lt = max(0, pre_lt - post_lt)
        bleach_tags = max(0, dec_lt - new_lt)
        
        total_tags = stripper_tags + bleach_tags
        
        # 3. Double entity normalization
        normalized, double_normalized_count = normalize_entities(cleaned)
        
        return normalized, total_tags, encoded_count, double_normalized_count

    async def process(self, batch: list[dict]) -> list[dict]:
        """
        Process a batch of items, sanitizing text fields and validating types.
        
        Filters out any items failing validation.
        """
        clean_batch = []
        
        for item in batch:
            self.stats["items_processed"] += 1
            is_valid = True
            item_modified = False
            
            # --- 1. Type Validation (IDs) ---
            for id_field in self.id_fields:
                ref = self._get_field_ref(item, id_field)
                if ref is not None:
                    parent, key = ref
                    if not self._validate_id(parent[key]):
                        logger.warning(
                            "Item failed validation: invalid/empty ID field '%s'=%s",
                            id_field, parent[key]
                        )
                        is_valid = False
                        break
            
            if not is_valid:
                self.stats["items_failed_validation"] += 1
                continue
                
            # --- 2. Type Validation (Numerics) ---
            for num_field in self.numeric_fields:
                ref = self._get_field_ref(item, num_field)
                if ref is not None:
                    parent, key = ref
                    ok, cast_val = self._validate_numeric(parent[key])
                    if not ok:
                        logger.warning(
                            "Item '%s' failed validation: invalid numeric field '%s'=%s",
                            item.get("id"), num_field, parent[key]
                        )
                        is_valid = False
                        break
                    else:
                        if parent[key] != cast_val:
                            parent[key] = cast_val
                            item_modified = True
                            
            if not is_valid:
                self.stats["items_failed_validation"] += 1
                continue
                
            # --- 3. Type Validation (URLs) ---
            for url_field in self.url_fields:
                ref = self._get_field_ref(item, url_field)
                if ref is not None:
                    parent, key = ref
                    if not self._validate_url(parent[key]):
                        logger.warning(
                            "Item '%s' failed validation: invalid URL field '%s'=%s",
                            item.get("id"), url_field, parent[key]
                        )
                        is_valid = False
                        break
                        
            if not is_valid:
                self.stats["items_failed_validation"] += 1
                continue
                
            # --- 4. HTML Sanitization (Text Fields) ---
            for text_field in self.text_fields:
                ref = self._get_field_ref(item, text_field)
                if ref is not None:
                    parent, key = ref
                    val = parent[key]
                    if val is not None and isinstance(val, str) and val.strip() != "":
                        sanitized, tags, encoded, double_norm = self.sanitize_html(val)
                        
                        # Accumulate statistics
                        self.stats["tags_stripped"] += tags
                        self.stats["entities_encoded"] += encoded
                        self.stats["double_encoded_normalized"] += double_norm
                        
                        if sanitized != val:
                            parent[key] = sanitized
                            item_modified = True

            if item_modified:
                self.stats["items_sanitized"] += 1
                
            clean_batch.append(item)
            
        return clean_batch
