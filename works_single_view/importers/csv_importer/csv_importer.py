from io import StringIO

import pandas
from django.db import connection
from psycopg2 import sql
from tqdm import tqdm

from works_single_view.importers.abstract_importer import AbstractImporter
from works_single_view.importers.csv_importer import sql_queries
from works_single_view.utils import count_number_of_lines_in_file


class WorksCSVImporter(AbstractImporter):
    """
    Note: CSV file format is more a convention than a standard, so it's
    hard to guarantee compatibility with every CSV file out there.
    """

    max_chunk_size = 50_000  # for minimal memory usage and improved performance
    min_number_of_chunks = 10  # for better import progress visualisation
    contributors_separator = "|"
    target_table_name = "musical_works"
    tmp_table_name = f"tmp_{target_table_name}"

    def import_from_file(self, file_path: str) -> None:
        # Note: some quoted values in a CSV file may contain embedded
        # carriage returns and line feeds. Thus CSV files are not
        # strictly one line per table row like text-format files.
        number_of_lines = (
            count_number_of_lines_in_file(file_path) - 1
        )  # -1 because we don't want include header line
        number_of_chunks = max(
            self.min_number_of_chunks, number_of_lines // self.max_chunk_size
        )
        chunk_size = min(self.max_chunk_size, number_of_lines // number_of_chunks)

        with connection.cursor() as cursor, open(file_path, "r") as csv_file:
            cursor.execute(
                sql=sql_queries.CREATE_TEMP_TABLE_QUERY.format(
                    tmp_table=sql.Identifier(self.tmp_table_name)
                )
            )

            csv_reader = pandas.read_csv(csv_file, chunksize=chunk_size)
            for chunk in tqdm(csv_reader, total=number_of_chunks):
                # Normalizing entries in DataFrame to make them
                # compatible with PostgreSQL 'COPY FROM' statement.
                self._normalize_chunk(chunk)

                # Clearing temporary DB table
                cursor.execute(
                    sql=sql_queries.CLEAR_TEMP_TABLE_QUERY.format(
                        tmp_table=sql.Identifier(self.tmp_table_name)
                    )
                )

                # Copying data from CSV directly to temporary table
                cursor.copy_expert(
                    sql=sql_queries.COPY_FROM_QUERY.format(
                        tmp_table=sql.Identifier(self.tmp_table_name)
                    ),
                    file=StringIO(chunk.to_csv(index=False)),
                )

                # Reconciling entries and storing them to target table
                cursor.execute(
                    sql=sql_queries.MERGE_ROWS_BY_ISWC.format(
                        target_table=sql.Identifier(self.target_table_name),
                        tmp_table=sql.Identifier(self.tmp_table_name),
                    )
                )

    def _normalize_chunk(self, chunk: pandas.DataFrame) -> None:
        chunk["contributors"] = chunk["contributors"].apply(
            self._convert_contributors_line_to_postgres_array_literal
        )

    def _convert_contributors_line_to_postgres_array_literal(
        self, contributors: str
    ) -> str:
        """
        Accepts contributors as single string, with each individual contributor
        separated by e.g. "|" symbol. As output, produces a string in format of
        Postgres ARRAY type literal. This allows PostgreSQL to directly convert
        this line to array during import.
        """
        contributors_list = contributors.split(self.contributors_separator)
        return "{" + ",".join(contributors_list) + "}"
