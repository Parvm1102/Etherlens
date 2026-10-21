#!/usr/bin/env python3
"""Replace __PLACEHOLDERS__ in a template file using .env values."""
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

if len(sys.argv) < 2:
    print("Usage: render_template.py <template_file>", file=sys.stderr)
    sys.exit(1)

text = Path(sys.argv[1]).read_text(encoding="utf-8")

replacements = {
    "__START_DATE__": os.environ.get("START_DATE", ""),
    "__END_DATE__": os.environ.get("END_DATE", ""),
    "__BUCKET__": os.environ.get("GCS_BUCKET", ""),
    "__SILVER_PATH__": f"gs://{os.environ.get('GCS_BUCKET', '')}/silver/tx",
    "__GOLD_PATH__": f"gs://{os.environ.get('GCS_BUCKET', '')}/gold",
}

for placeholder, value in replacements.items():
    text = text.replace(placeholder, value)

print(text, end="")
