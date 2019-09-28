from django.core.management import BaseCommand

from works_single_view.utils import generate_random_input_csv


class Command(BaseCommand):
    help = "This command generates sample CSV file, allowing to test import logic."

    def add_arguments(self, parser):
        parser.add_argument(
            "number_of_entries", type=int, help="The number of entries to generate."
        )

    def handle(self, *args, **options):
        number_of_entries = options.get("number_of_entries", 1_000_000)
        file_path = "input_data/test.csv"
        generate_random_input_csv(file_path, number_of_entries)
        print(
            f"Sample CSV file successfully generated: {file_path}\n"
            f"Number of entries: {number_of_entries}\n"
            f"You can now try importing them using the following command\n"
            f"python manage.py import_works {file_path}"
        )
