from pypdf import PdfReader
import sys

if len(sys.argv) < 3:
    print('Usage: extract_pdf_page.py <pdf_path> <page_number_1_based>')
    sys.exit(2)

pdf_path = sys.argv[1]
page_num = int(sys.argv[2]) - 1

reader = PdfReader(pdf_path)
if page_num < 0 or page_num >= len(reader.pages):
    print('Page out of range')
    sys.exit(3)

page = reader.pages[page_num]
text = page.extract_text()
print(text or '')
