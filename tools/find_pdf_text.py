from pypdf import PdfReader
import sys
if len(sys.argv) < 3:
    print('Usage: find_pdf_text.py <pdf_path> <search_phrase>')
    sys.exit(2)

pdf_path = sys.argv[1]
phrase = sys.argv[2]
reader = PdfReader(pdf_path)
for i, page in enumerate(reader.pages, start=1):
    text = page.extract_text() or ''
    if phrase.lower() in text.lower():
        print(f'Found on page {i}')
        print(text)
        break
else:
    print('Phrase not found')
