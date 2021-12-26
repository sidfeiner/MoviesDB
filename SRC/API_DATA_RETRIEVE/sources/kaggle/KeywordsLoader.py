import ast
import csv
import logging
from typing import Generator, Sequence

import fire
from SRC.API_DATA_RETRIEVE.sources import KeywordsLoader
from SRC.API_DATA_RETRIEVE.common.mysql import MySQL, DEFAULT_DB
from SRC.API_DATA_RETRIEVE.sources.kaggle.base import *

DEFAULT_INSERT_BATCH_SIZE = 1000


class KeywordsLoaderImpl(KeywordsLoader.KeywordsLoader):
    def __init__(self, keywords_file: str, mysql_usr: str = '', mysql_pwd: str = '', mysql_host: Optional[str] = None,
                 mysql_port: Optional[int] = None, mysql_db: str = DEFAULT_DB, mysql_helper: Optional[MySQL] = None,
                 log_level: str = logging.INFO):
        super().__init__(mysql_usr, mysql_pwd, mysql_host, mysql_port, mysql_db, mysql_helper, log_level)
        self.keywords_file = keywords_file

    def get_parsed_keywords_generator(self) -> Generator[Keywords, None, None]:
        with open(self.keywords_file, newline='') as fp:
            reader = csv.DictReader(fp, delimiter=',', quotechar='"')
            for row in reader:
                yield Keywords(
                    int(row['id']),
                    [Keyword.from_dict(d) for d in ast.literal_eval(row['keywords'])]
                )

    def load(self, insert_batch_size: int = DEFAULT_INSERT_BATCH_SIZE) -> Sequence[Keywords]:
        gen = super().load(insert_batch_size)
        return [keywords for keywords in gen]


if __name__ == '__main__':
    fire.Fire(KeywordsLoaderImpl)
