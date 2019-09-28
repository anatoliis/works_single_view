from typing import Type

from django.core.management import BaseCommand

from works_single_view.importers.abstract_importer import AbstractImporter
from works_single_view.importers.csv_importer.csv_importer import WorksCSVImporter


class Command(BaseCommand):
    help = "This command initiates importing data from CSV file."

    importer: Type[AbstractImporter] = WorksCSVImporter

    def add_arguments(self, parser):
        parser.add_argument(
            "file", type=str, help="Path to input data file in CSV format."
        )

    def handle(self, *args, **options):
        file_path = options["file"]
        importer = self.importer()
        importer.import_from_file(file_path)
