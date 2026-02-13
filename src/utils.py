"""
Shared utility functions for Relay
"""

import re


def sanitize_table_name(name: str) -> str:
    """Convert a pipeline name into a safe SQL table name.

    Rules:
    - Lowercase
    - Replace spaces and hyphens with underscores
    - Strip non-alphanumeric characters (except underscores)
    - Prefix with 't_' if name starts with a digit
    """
    table = name.lower().replace(" ", "_").replace("-", "_")
    table = re.sub(r"[^a-z0-9_]", "", table)
    if table and table[0].isdigit():
        table = "t_" + table
    return table
