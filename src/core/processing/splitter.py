import os
import tempfile
from io import BytesIO

import uuid
from PyPDF2 import PdfReader, PdfWriter
from fastapi import HTTPException


def split_pdf(pdf_path):
    try:
        pdf_list = []
        pdf_name_without_ext = os.path.splitext(os.path.basename(pdf_path))[0]

        with open(pdf_path, "rb") as f:
            reader = PdfReader(f)

            for page_num in range(0, len(reader.pages)):
                selected_page = reader.pages[page_num]
                writer = PdfWriter()
                writer.add_page(selected_page)
                
                with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                    writer.write(tmp_file)
                    pdf_list.append(tmp_file.name)
                    
        return pdf_list

    except Exception as e:
        raise Exception(e)


def split_pdf_by_orders(pdf_path: str, orders: list):
    """Split PDF by orders using OS temp files. Caller must clean up returned files."""
    reader = PdfReader(pdf_path)
    output_files = []

    for idx, pages in enumerate(orders, start=1):
        writer = PdfWriter()
        for p in pages:
            if 1 <= p <= len(reader.pages):
                writer.add_page(reader.pages[p - 1])

        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
            writer.write(tmp_file)
            output_files.append(tmp_file.name)

    return output_files
