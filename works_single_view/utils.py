import csv
import random

ISWC_REGEX = "T-?[0-9]{3}\.?[0-9]{3}\.?[0-9]{3}-?[0-9]"


def count_number_of_lines_in_file(file_path: str) -> int:
    """
    This method counts the number of lines in a given
    file in a very rapid way. Processing of 3.7 GB CSV
    file with 100 million lines takes about 4 seconds
    from SSD with 3180 MBps sequential reading rating.
    """

    def buf_generator(reader):
        buf = reader(1024 * 1024)
        while buf:
            yield buf
            buf = reader(1024 * 1024)

    with open(file_path, "rb") as fd:
        return sum(buf.count(b"\n") for buf in buf_generator(fd.raw.read))


def generate_random_input_csv(file_path: str, number_of_entries: int) -> None:
    with open(file_path, 'w+') as csv_file:
        writer = csv.writer(csv_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['title', 'contributors', 'iswc'])
        for i in range(number_of_entries):
            title = f"Music title #{i}"

            show_iswc = random.random() <= 0.5
            iswc = f'T{i:010d}' if show_iswc else ''

            number_of_contributors = random.randrange(1, 5)
            contributors = '|'.join(
                f'Contributor #{i}/{contributor_index}'
                for contributor_index in range(number_of_contributors)
            )

            writer.writerow([title, contributors, iswc])
