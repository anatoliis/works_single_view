from abc import ABC, abstractmethod


class AbstractImporter(ABC):
    @abstractmethod
    def import_from_file(self, file_path: str) -> None:
        pass
