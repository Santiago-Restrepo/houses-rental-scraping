import logging
from typing import List, Dict
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from .base_loader import BaseLoader


class SheetsLoader(BaseLoader):
    """Loader for saving data to a Google Sheet."""

    def __init__(self, creds_path: str):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)

        # Auth with Google Sheets
        scope = ["https://spreadsheets.google.com/feeds",
                 "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
        self.client = gspread.authorize(creds)


    def load(self, data: List[Dict], sheet_key: str) -> bool:
        """Overwrite Google Sheet with new data."""
        if not self._validate_data(data):
            return False

        try:
            df = pd.DataFrame(data)

            # Open sheet
            sheet = self.client.open_by_key(sheet_key).get_worksheet(0)

            # Clear sheet
            sheet.clear()

            # Write headers + rows
            sheet.update([df.columns.values.tolist()] + df.values.tolist())

            self.logger.info(f"Loaded {len(data)} records into Google Sheet: {sheet_key}")
            return True

        except Exception as e:
            self.logger.error(f"Error saving data to Google Sheets: {e}")
            return False
