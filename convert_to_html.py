import markdown

with open('RELAY_PRESENTATION.md', 'r', encoding='utf-8') as f:
    md_content = f.read()

html_body = markdown.markdown(md_content, extensions=['extra', 'fenced_code'])

full_html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Relay - Agent-Native Data Movement Platform</title>
<style>
@page {{ size: letter; margin: 0.75in; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; line-height: 1.6; max-width: 100%; color: #333; padding: 20px; }}
h1 {{ color: #2c3e50; font-size: 36px; margin: 24px 0 8px; page-break-after: avoid; }}
h2 {{ color: #34495e; font-size: 28px; margin: 20px 0 12px; border-bottom: 3px solid #3498db; padding-bottom: 8px; page-break-after: avoid; }}
h3 {{ color: #555; font-size: 20px; margin: 16px 0 8px; page-break-after: avoid; }}
p {{ margin: 8px 0; }}
ul, ol {{ margin: 8px 0; padding-left: 24px; }}
li {{ margin: 4px 0; }}
code {{ background: #f4f4f4; padding: 2px 6px; border-radius: 3px; font-family: 'Courier New', Courier, monospace; font-size: 14px; }}
pre {{ background: #2c3e50; color: #ecf0f1; padding: 16px; border-radius: 6px; page-break-inside: avoid; overflow-x: auto; }}
pre code {{ background: none; color: inherit; padding: 0; }}
blockquote {{ border-left: 4px solid #3498db; padding-left: 16px; margin: 16px 0; color: #555; font-style: italic; }}
hr {{ border: none; border-top: 2px solid #eee; margin: 32px 0; page-break-after: always; }}
strong {{ color: #2c3e50; }}
@media print {{
    body {{ padding: 0; }}
}}
</style>
</head>
<body>
{html_body}
</body>
</html>"""

with open('RELAY_PRESENTATION.html', 'w', encoding='utf-8') as f:
    f.write(full_html)

print('HTML generated: RELAY_PRESENTATION.html')
print('Open in browser and use Ctrl+P (or Cmd+P) to Print to PDF')
