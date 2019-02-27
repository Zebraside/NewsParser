import os
import logging
from csv import DictWriter


class CsvSaver:
    storage_folder = "Data"

    def __init__(self, name: str, field_names):
        self.name = name
        self.file = open(os.path.join(self.storage_folder, self.name), 'w')

        self.writer = DictWriter(self.file, fieldnames=field_names)
        self.writer.writeheader()

    def save(self, row: dict):
        try:
            self.writer.writerow(row)
        except UnicodeEncodeError:
            logging.warning("Can't encode message")  # TODO: Even better would be if we could preprocess string

    def __del__(self):
        self.file.close()