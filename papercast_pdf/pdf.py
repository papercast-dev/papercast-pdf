import logging
from pathlib import Path
from urllib.error import HTTPError
from typing import Dict, Any

import requests
import wget

from papercast.types import PathLike, PDFFile
from papercast.production import Production
from papercast.base import BaseCollector


class PDFCollector(BaseCollector):
    def __init__(self, pdf_dir: PathLike):
        self.pdf_dir = pdf_dir

    input_types = {"pdf_link": str}
    output_types = {
            "pdf": PDFFile,
            "title": str,
            "arxiv_id": str,
            "authors": list,
            "doi": str,
            "description": str,
        }

    def process(self, pdf_link) -> Production:
        pdf_path, jsonpath, doc = self._download_pdf_link(pdf_link)
        pdf = PDFFile(path=pdf_path)
        production = Production()
        setattr(production, "pdf", pdf)
        for k, v in doc.items():
            setattr(production, k, v)
        return production

    def _download_pdf_link(self, pdf_link):
        logging.info(f"Downloading {pdf_link}... run papercast extract to create json.")
        try:
            wget.download(pdf_link, out=str(self.pdf_dir))
        except HTTPError as e:
            logging.info(
                f"Could not download {pdf_link}: {e} with wget, retrying with requests"
            )
            r = requests.get(
                pdf_link,
                allow_redirects=True,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT x.y; rv:10.0) Gecko/20100101 Firefox/10.0"
                },
            )
            r.raise_for_status()
            with open(Path(self.pdf_dir) / Path(pdf_link).name, "wb") as f:
                f.write(r.content)

        return Path(self.pdf_dir) / Path(pdf_link).name, None, {}
