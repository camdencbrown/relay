"""Generate PDF from Relay presentation markdown"""

import markdown
from weasyprint import HTML
from pathlib import Path

# Read markdown
with open('RELAY_PRESENTATION.md', 'r', encoding='utf-8') as f:
    md_content = f.read()

# Convert to HTML
html_content = markdown.markdown(md_content, extensions=['extra', 'codehilite', 'fenced_code'])

# Wrap in styled HTML
full_html = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        @page {
            size: letter;
            margin: 0.75in;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 100%;
        }
        h1 {
            color: #2c3e50;
            font-size: 36px;
            margin-top: 0;
            margin-bottom: 8px;
            page-break-after: avoid;
        }
        h2 {
            color: #34495e;
            font-size: 28px;
            margin-top: 24px;
            margin-bottom: 12px;
            border-bottom: 3px solid #3498db;
            padding-bottom: 8px;
            page-break-after: avoid;
        }
        h3 {
            color: #555;
            font-size: 20px;
            margin-top: 16px;
            margin-bottom: 8px;
            page-break-after: avoid;
        }
        p {
            margin: 8px 0;
        }
        ul, ol {
            margin: 8px 0;
            padding-left: 24px;
        }
        li {
            margin: 4px 0;
        }
        code {
            background: #f4f4f4;
            padding: 2px 6px;
            border-radius: 3px;
            font-family: 'Courier New', monospace;
            font-size: 14px;
        }
        pre {
            background: #2c3e50;
            color: #ecf0f1;
            padding: 16px;
            border-radius: 6px;
            overflow-x: auto;
            page-break-inside: avoid;
        }
        pre code {
            background: none;
            color: inherit;
            padding: 0;
        }
        blockquote {
            border-left: 4px solid #3498db;
            padding-left: 16px;
            margin: 16px 0;
            color: #555;
            font-style: italic;
        }
        hr {
            border: none;
            border-top: 2px solid #eee;
            margin: 32px 0;
            page-break-after: always;
        }
        strong {
            color: #2c3e50;
        }
    </style>
</head>
<body>
""" + html_content + """
</body>
</html>
"""

# Generate PDF
HTML(string=full_html).write_pdf('RELAY_PRESENTATION.pdf')
print('PDF generated successfully: RELAY_PRESENTATION.pdf')
