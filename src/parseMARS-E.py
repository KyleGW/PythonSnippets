################################################
# MARS-E PDF Control Extractor
# Extracts controls and enhancements from CMS MARS-E PDF documents
# Outputs to CSV for further analysis
# Author: KyleGW
# Date: 06-21-2025
# 
# Installation and Use note: Ensure pdfplumber is installed in your Python environment, you can install it via pip: pip install pdfplumber
# Usage: Adjust the PDF path and output CSV filename as needed in the example usage section at the bottom of the script.
# Example: extract_controls_from_pdf("path/to/MARS-E-v2-2-Vol-2.pdf", "output/mars-e-controls-vol2.csv")
# 
# Description:
# This script processes the CMS MARS-E v2.2 Volume 2 PDF document to extract control identifiers, titles, descriptions, and applicability baselines.
# It captures both base controls and their enhancements, organizing them into a structured CSV format.
# The script generates a timestamped CSV file in the output directory to avoid overwriting previous outputs.
#
#  There are a couple of controls that do not follow the expected format and may not be captured correctly.
#  Two of these are SC-20 on page 64 and SC-202 on page 245 of the MARS-E v2.2 Vol 2 PDF.

#  Known Parsing / source file issues:
#  SC-301 on pdf page 343 is parsed correctly as the control is 'Table SC-301. SC-ACA-1: Electronic Mail'
#  SC-302 on pdf page 344 is parsed correctly as the control is 'Table SC-302. SC-ACA-2: FAX Usage '
#  PC-356 on pdf page 403 is parsed correctly now - correcting in script for the control being mis-spelled in the source as 'Table PC-356. D-1 (1): Validate PII ' when it should be DI-1 (1):'
#  PC-363 on pdf page 410 is not fully parsed since the control title is split across lines in the PDF, causing incomplete extraction. 
#  The table on pdf page 436 is wrong as it skips controls SC-4 and SC-5 in it's count, thus adding PC-378 and PC-379 which do not exist
################################################


import pdfplumber
import csv
import re
from datetime import datetime

def extract_detailed_controls(pdf_path, output_csv):
    # Patterns to capture control ID and enhancements
    pattern_base = re.compile(r'^([A-Z]{2}-\d{1,2})\s+–\s+(.*)')
    pattern_enhancement = re.compile(r'^([A-Z]{2}-\d{1,2})\((\d+)\)\s+–\s+(.*)')
    pattern_baseline = re.compile(r'Applicability\s*:\s*(LO)?\s*(MD)?', re.IGNORECASE)

    rows = []
    current_id, current_title, current_type = None, None, 'base'
    description_lines = []
    baseline = []

    def flush_current():
        if current_id and current_title:
            rows.append([
                current_id,
                '' if current_type == 'base' else current_id.split('(')[-1].rstrip(')'),
                current_title,
                ' '.join(description_lines).strip(),
                ', '.join(baseline)
            ])

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            print(f"Processing page {page.page_number}...")
            lines = page.extract_text().split('\n')
            for line in lines:
                line = line.strip()

                # If a new base control starts, flush the old one
                if pattern_base.match(line):
                    flush_current()
                    match = pattern_base.match(line)
                    current_id = match.group(1)
                    current_title = match.group(2)
                    current_type = 'base'
                    description_lines = []
                    baseline = []
                    continue

                # If a new enhancement starts, flush the previous block
                elif pattern_enhancement.match(line):
                    flush_current()
                    match = pattern_enhancement.match(line)
                    current_id = f"{match.group(1)}({match.group(2)})"
                    current_title = match.group(3)
                    current_type = 'enhancement'
                    description_lines = []
                    baseline = []
                    continue

                # Check for applicability baseline
                elif pattern_baseline.search(line):
                    match = pattern_baseline.search(line)
                    baseline = [level for level in match.groups() if level]
                    continue

                # Otherwise, treat as part of description
                elif current_id:
                    description_lines.append(line)

        flush_current()  # Final entry

    # Export to CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Base Control ID', 'Enhancement Number', 'Title', 'Description', 'Baseline'])
        writer.writerows(rows)

    print(f"Extracted {len(rows)} controls and enhancements to {output_csv}")

def normalize_control(code: str) -> str:
    # Handle special controls like MP-CMS-1, SC-ACA-2 (format: XX-YYY-Z)
    match = re.match(r'([A-Z]{2})-([A-Z]+)-(\d+)(?:\s+\((\d+)\))?', code)
    if match:
        base = f"{match.group(1).lower()}-{match.group(2).lower()}-{match.group(3)}"
        enhancement = match.group(4)
        return f"{base}.{enhancement}" if enhancement else base
    
    # Handle standard controls like AC-2 or AC-2 (1)
    match = re.match(r'([A-Z]{2}-\d+)(?:\s+\((\d+)\))?', code)
    if not match:
        return code.lower()  # fallback if pattern doesn't match

    base = match.group(1).lower()
    enhancement = match.group(2)

    return f"{base}.{enhancement}" if enhancement else base



def extract_controls_from_pdf(pdf_path, output_csv):
    rows = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            if page.page_number > 0:
                text = page.extract_text()
                if not text:
                    continue

                lines = text.split('\n')
                for line in lines:
                    # Example: Detect control like "AC-2(1) – Account Management"
                    #if line[:5].upper().startswith(('AC-', 'IA-', 'SC-', 'AU-', 'CM-', 'RA-', 'SA-', 'SI-', 'MP-', 'MA-')):
                    if line[:8].startswith(('Table SC', 'Table PC', 'SC', 'PC')):    
                        #print(f"Processing page {page.page_number} line: {line.strip()}")
                        
                        # Handle malformed patterns first
                        # Pattern 1: "SC-Table 20. AC-20: ..." -> SC-20. AC-20: ...
                        line = re.sub(r'(SC|PC)-Table\s+(\d+)', r'\1-\2', line)
                        # Pattern 2: "Table SC-202. AC-1: ..." or "Table PC-348. AR-1: ..." -> SC-202. AC-1: ... or PC-348. AR-1: ...
                        line = re.sub(r'Table\s+((SC|PC)-\d+)\.', r'\1.', line)
                        # Pattern 3: Remove trailing period after SC/PC code "SC-301. AC-" -> "SC-301 AC-"
                        line = re.sub(r'((SC|PC)-\d+)\.\s+([A-Z]{2}-)', r'\1. \3', line)
                        # Pattern 4: Fix misspelled control "PC-356. D-1 (1):" -> "PC-356. DI-1 (1):"
                        line = re.sub(r'(PC-356\.\s+)D-1\s+\(', r'\1DI-1 (', line)
                        
                        # Updated regex to handle:
                        # - Standard controls: AC-2, AC-2 (1)
                        # - CMS-specific controls: MP-CMS-1, SC-ACA-2
                        # - Both SC (Security Control) and PC (Privacy Control) codes
                        match = re.search(r'\b((SC|PC)-\d+)\.?\s+([A-Z]{2}-(?:[A-Z]+-)?[A-Z]*\d+(?: ?\(\d+\))?):\s+(.*?)(?:\s+\.{3,}\s+\d+)?$',line)
                        if match:
                            code_prefix = match.group(1)  # SC-123 or PC-348
                            control_label = match.group(3)
                            control_title = match.group(4)
                            print(f"Processing page {page.page_number} - Matched Code: {code_prefix}, Control ID: {control_label}, Title: {control_title}")
                            rows.append([code_prefix, normalize_control(control_label) ,control_label, control_title])

    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Table Code",'Control ID','Control Label','Title'])
        writer.writerows(rows)

    print(f'Exported {len(rows)} controls to {output_csv}')

# Example usage
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_filename = f"output\\mars-e-controls-vol2.1_{timestamp}.csv"
extract_controls_from_pdf("docs\MARS-E-v2-2-Vol-2-AE-ACA-SSP_Final_08032021.pdf", output_filename) # type: ignore
#extract_controls_from_pdf("docs\MARS-E v2-2-Vol-1_Final-Signed_08032021-1.pdf", "mars-e-controls-vol1.csv") # type: ignore

# Examples
print(normalize_control("AC-2 (1)"))  # Output: ac-2.1
print(normalize_control("CM-3 (2)"))  # Output: cm-3.2
print(normalize_control("IA-5"))      # Output: ia-5
# Example usage:
#extract_detailed_controls("MARS-E-v2-2-Vol-2-AE-ACA-SSP_Final_08032021.pdf", "mars-e-controls-and-enhancements.csv")
