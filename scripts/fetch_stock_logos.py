#!/usr/bin/env python3
"""
One-time script to fetch company logos for all stocks in the database.

Uses a multi-source approach:
1. Logo.dev API (primary) - high quality logos
2. Google Favicon API (fallback) - always available
3. Text placeholder (final fallback) - for any missing logos

Usage:
    python3 scripts/fetch_stock_logos.py
"""

import json
import os
import time
import urllib.request
import urllib.error
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
import hashlib

# Configuration
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
MAPPING_FILE = SCRIPT_DIR / "symbol_domain_mapping.json"
OUTPUT_DIR = PROJECT_ROOT / "frontend" / "public" / "logos"
MANIFEST_FILE = OUTPUT_DIR / "manifest.json"

# Logo size
LOGO_SIZE = 64

# Rate limiting
REQUEST_DELAY = 0.3  # seconds between requests

# Colors for placeholder icons (based on first letter hash)
PLACEHOLDER_COLORS = [
    "#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4", "#FFEAA7",
    "#DDA0DD", "#98D8C8", "#F7DC6F", "#BB8FCE", "#85C1E9",
    "#F8B500", "#00CED1", "#FF69B4", "#7B68EE", "#3CB371"
]


def load_domain_mapping() -> dict:
    """Load the symbol to domain mapping."""
    with open(MAPPING_FILE, "r") as f:
        return json.load(f)


def get_placeholder_color(symbol: str) -> str:
    """Get a consistent color for a symbol based on hash."""
    hash_val = int(hashlib.md5(symbol.encode()).hexdigest(), 16)
    return PLACEHOLDER_COLORS[hash_val % len(PLACEHOLDER_COLORS)]


def create_placeholder_logo(symbol: str, output_path: Path) -> bool:
    """Create a text-based placeholder logo."""
    try:
        # Create image with colored background
        color = get_placeholder_color(symbol)
        img = Image.new("RGB", (LOGO_SIZE, LOGO_SIZE), color)
        draw = ImageDraw.Draw(img)
        
        # Get initials (first 1-2 characters)
        initials = symbol[:2].upper()
        
        # Try to use a nice font, fall back to default
        try:
            font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 24)
        except:
            try:
                font = ImageFont.truetype("/System/Library/Fonts/SFNSMono.ttf", 24)
            except:
                font = ImageFont.load_default()
        
        # Center the text
        bbox = draw.textbbox((0, 0), initials, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        x = (LOGO_SIZE - text_width) / 2
        y = (LOGO_SIZE - text_height) / 2 - 2
        
        # Draw white text
        draw.text((x, y), initials, fill="white", font=font)
        
        # Save as PNG
        img.save(output_path, "PNG")
        return True
    except Exception as e:
        print(f"  ⚠️  Error creating placeholder for {symbol}: {e}")
        return False


def fetch_logo_from_logodev(domain: str, output_path: Path) -> bool:
    """Fetch logo from Logo.dev API."""
    # Logo.dev public endpoint (no API key needed for basic usage)
    url = f"https://img.logo.dev/{domain}?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ&size=128&format=png"
    
    try:
        request = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        })
        
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status == 200:
                content = response.read()
                # Verify it's a valid image (not an error page)
                if len(content) > 500:  # Real logos are usually larger
                    # Save and resize
                    temp_path = output_path.with_suffix(".tmp")
                    with open(temp_path, "wb") as f:
                        f.write(content)
                    
                    # Resize to standard size
                    img = Image.open(temp_path)
                    img = img.convert("RGBA")
                    img = img.resize((LOGO_SIZE, LOGO_SIZE), Image.Resampling.LANCZOS)
                    
                    # Save as PNG with white background
                    background = Image.new("RGB", (LOGO_SIZE, LOGO_SIZE), "white")
                    if img.mode == "RGBA":
                        background.paste(img, mask=img.split()[3])
                    else:
                        background.paste(img)
                    background.save(output_path, "PNG")
                    
                    temp_path.unlink()
                    return True
    except Exception as e:
        pass  # Will try fallback
    
    return False


def fetch_logo_from_google(domain: str, output_path: Path) -> bool:
    """Fetch favicon from Google's favicon service."""
    url = f"https://www.google.com/s2/favicons?domain={domain}&sz=128"
    
    try:
        request = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        })
        
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status == 200:
                content = response.read()
                if len(content) > 100:  # Check it's not empty
                    # Save and resize
                    temp_path = output_path.with_suffix(".tmp")
                    with open(temp_path, "wb") as f:
                        f.write(content)
                    
                    # Check if it's the default globe icon (we want to skip these)
                    img = Image.open(temp_path)
                    
                    # Resize to standard size
                    img = img.convert("RGBA")
                    img = img.resize((LOGO_SIZE, LOGO_SIZE), Image.Resampling.LANCZOS)
                    
                    # Save as PNG with white background
                    background = Image.new("RGB", (LOGO_SIZE, LOGO_SIZE), "white")
                    if img.mode == "RGBA":
                        background.paste(img, mask=img.split()[3])
                    else:
                        background.paste(img)
                    background.save(output_path, "PNG")
                    
                    temp_path.unlink()
                    return True
    except Exception as e:
        pass  # Will try fallback
    
    return False


def fetch_logos():
    """Main function to fetch all logos."""
    print("🚀 Starting logo fetch...")
    print(f"   Output directory: {OUTPUT_DIR}")
    print()
    
    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load mapping
    mapping = load_domain_mapping()
    total = len(mapping)
    print(f"📋 Found {total} stocks to process")
    print()
    
    # Track results
    manifest = {}
    stats = {"logodev": 0, "google": 0, "placeholder": 0, "errors": 0}
    
    for i, (symbol, domain) in enumerate(mapping.items(), 1):
        output_path = OUTPUT_DIR / f"{symbol}.png"
        
        print(f"[{i:3}/{total}] {symbol:15} ({domain})...", end=" ", flush=True)
        
        # Try Logo.dev first
        if fetch_logo_from_logodev(domain, output_path):
            print("✅ Logo.dev")
            manifest[symbol] = {"available": True, "source": "logo.dev"}
            stats["logodev"] += 1
        # Try Google Favicon as fallback
        elif fetch_logo_from_google(domain, output_path):
            print("✅ Google")
            manifest[symbol] = {"available": True, "source": "google"}
            stats["google"] += 1
        # Create placeholder as final fallback
        elif create_placeholder_logo(symbol, output_path):
            print("📝 Placeholder")
            manifest[symbol] = {"available": False, "placeholder": True}
            stats["placeholder"] += 1
        else:
            print("❌ Failed")
            manifest[symbol] = {"available": False, "error": True}
            stats["errors"] += 1
        
        # Rate limiting
        time.sleep(REQUEST_DELAY)
    
    # Save manifest
    with open(MANIFEST_FILE, "w") as f:
        json.dump(manifest, f, indent=2)
    
    # Print summary
    print()
    print("=" * 60)
    print("📊 SUMMARY")
    print("=" * 60)
    print(f"   Total stocks:     {total}")
    print(f"   ✅ Logo.dev:      {stats['logodev']}")
    print(f"   ✅ Google:        {stats['google']}")
    print(f"   📝 Placeholder:   {stats['placeholder']}")
    print(f"   ❌ Errors:        {stats['errors']}")
    print()
    print(f"   Real logos:       {stats['logodev'] + stats['google']} ({(stats['logodev'] + stats['google']) / total * 100:.1f}%)")
    print()
    print(f"✨ Logos saved to: {OUTPUT_DIR}")
    print(f"📄 Manifest saved to: {MANIFEST_FILE}")


if __name__ == "__main__":
    fetch_logos()
