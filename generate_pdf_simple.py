"""Generate PDF from Relay presentation markdown using reportlab"""

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Preformatted
from reportlab.lib.colors import HexColor
from pathlib import Path

# Read markdown
with open('RELAY_PRESENTATION.md', 'r', encoding='utf-8') as f:
    md_content = f.read()

# Create PDF
pdf = SimpleDocTemplate(
    "RELAY_PRESENTATION.pdf",
    pagesize=letter,
    rightMargin=0.75*inch,
    leftMargin=0.75*inch,
    topMargin=0.75*inch,
    bottomMargin=0.75*inch
)

# Styles
styles = getSampleStyleSheet()

# Custom styles
title_style = ParagraphStyle(
    'CustomTitle',
    parent=styles['Heading1'],
    fontSize=36,
    textColor=HexColor('#2c3e50'),
    spaceAfter=12,
    alignment=TA_CENTER
)

h1_style = ParagraphStyle(
    'CustomH1',
    parent=styles['Heading1'],
    fontSize=28,
    textColor=HexColor('#34495e'),
    spaceBefore=24,
    spaceAfter=12,
    borderWidth=0,
    borderColor=HexColor('#3498db'),
    borderPadding=8
)

h2_style = ParagraphStyle(
    'CustomH2',
    parent=styles['Heading2'],
    fontSize=20,
    textColor=HexColor('#555'),
    spaceBefore=16,
    spaceAfter=8
)

h3_style = ParagraphStyle(
    'CustomH3',
    parent=styles['Heading3'],
    fontSize=16,
    textColor=HexColor('#666'),
    spaceBefore=12,
    spaceAfter=6
)

normal_style = ParagraphStyle(
    'CustomNormal',
    parent=styles['Normal'],
    fontSize=11,
    leading=16,
    spaceAfter=8
)

code_style = ParagraphStyle(
    'CustomCode',
    parent=styles['Code'],
    fontSize=9,
    fontName='Courier',
    leftIndent=20,
    rightIndent=20,
    spaceBefore=8,
    spaceAfter=8,
    backColor=HexColor('#f4f4f4')
)

bullet_style = ParagraphStyle(
    'CustomBullet',
    parent=styles['Normal'],
    fontSize=11,
    leftIndent=20,
    bulletIndent=10,
    spaceAfter=4
)

# Parse markdown into story
story = []
lines = md_content.split('\n')

i = 0
in_code_block = False
code_lines = []

while i < len(lines):
    line = lines[i].rstrip()
    
    # Handle code blocks
    if line.startswith('```'):
        if in_code_block:
            # End code block
            code_text = '\n'.join(code_lines)
            story.append(Preformatted(code_text, code_style))
            code_lines = []
            in_code_block = False
        else:
            # Start code block
            in_code_block = True
        i += 1
        continue
    
    if in_code_block:
        code_lines.append(line)
        i += 1
        continue
    
    # Page breaks
    if line == '---':
        story.append(PageBreak())
        i += 1
        continue
    
    # Headings
    if line.startswith('# '):
        text = line[2:].strip()
        if i < 5:  # Title slide
            story.append(Paragraph(text, title_style))
        else:
            story.append(Paragraph(text, h1_style))
        story.append(Spacer(1, 0.2*inch))
    
    elif line.startswith('## '):
        text = line[3:].strip()
        story.append(Paragraph(text, h1_style))
        story.append(Spacer(1, 0.1*inch))
    
    elif line.startswith('### '):
        text = line[4:].strip()
        story.append(Paragraph(text, h2_style))
    
    elif line.startswith('#### '):
        text = line[5:].strip()
        story.append(Paragraph(text, h3_style))
    
    # Bullets
    elif line.startswith('- ') or line.startswith('* '):
        text = line[2:].strip()
        # Convert markdown formatting
        text = text.replace('**', '<b>').replace('**', '</b>')
        text = text.replace('`', '<font face="Courier">')
        text = text.replace('`', '</font>')
        story.append(Paragraph('• ' + text, bullet_style))
    
    elif line.startswith('  - ') or line.startswith('  * '):
        text = line[4:].strip()
        text = text.replace('**', '<b>').replace('**', '</b>')
        story.append(Paragraph('  ◦ ' + text, bullet_style))
    
    # Numbered lists
    elif len(line) > 2 and line[0].isdigit() and line[1:3] == '. ':
        text = line[line.index('.')+2:].strip()
        text = text.replace('**', '<b>').replace('**', '</b>')
        story.append(Paragraph(line[:line.index('.')+1] + ' ' + text, bullet_style))
    
    # Blockquotes
    elif line.startswith('> '):
        text = line[2:].strip()
        quote_style = ParagraphStyle(
            'Quote',
            parent=normal_style,
            leftIndent=20,
            italic=True,
            textColor=HexColor('#555')
        )
        story.append(Paragraph(text, quote_style))
    
    # Normal paragraphs
    elif line.strip():
        text = line.strip()
        # Convert markdown formatting
        text = text.replace('**', '<b>').replace('**', '</b>')
        text = text.replace('*', '<i>').replace('*', '</i>')
        text = text.replace('`', '<font face="Courier">')
        text = text.replace('`', '</font>')
        story.append(Paragraph(text, normal_style))
    
    # Empty lines create space
    else:
        if story and not isinstance(story[-1], Spacer):
            story.append(Spacer(1, 0.1*inch))
    
    i += 1

# Build PDF
pdf.build(story)
print('✓ PDF generated successfully: RELAY_PRESENTATION.pdf')
print(f'  File size: {Path("RELAY_PRESENTATION.pdf").stat().st_size / 1024:.1f} KB')
print(f'  Pages: ~{len([s for s in story if isinstance(s, PageBreak)]) + 1}')
