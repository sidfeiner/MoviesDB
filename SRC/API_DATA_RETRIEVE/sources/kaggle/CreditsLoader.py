import ast
import csv
import logging
from typing import Generator, Sequence, Any

import fire

from SRC.API_DATA_RETRIEVE.sources import CreditsLoader
from SRC.API_DATA_RETRIEVE.common.mysql import MySQL, DEFAULT_DB
from SRC.API_DATA_RETRIEVE.sources.kaggle.base import *

DEFAULT_INSERT_BATCH_SIZE = 2000


class CreditsLoaderImpl(CreditsLoader.CreditsLoader):
    def __init__(self, credits_file: str, mysql_usr: str = '', mysql_pwd: str = '', mysql_host: Optional[str] = None,
                 mysql_port: Optional[int] = None, mysql_db: str = DEFAULT_DB, mysql_helper: Optional[MySQL] = None,
                 log_level: str = logging.INFO):
        super().__init__(mysql_usr, mysql_pwd, mysql_host, mysql_port, mysql_db, mysql_helper, log_level)
        self.credits_file = credits_file

    def get_parsed_credits_generator(self) -> Generator[Credits, None, None]:
        with open(self.credits_file, newline='') as fp:
            reader = csv.DictReader(fp, delimiter=',', quotechar='"')
            for _, row in enumerate(reader):
                crew = []
                for member in ast.literal_eval(row['crew']):
                    member['movie_id'] = int(row['id'])
                    crew.append(CrewMember.from_dict(member))
                cast = []
                for member in ast.literal_eval(row['cast']):
                    member['movie_id'] = int(row['id'])
                    cast.append(CastMember.from_dict(member))
                yield Credits(
                    int(row['id']),
                    crew,
                    cast
                )

    def load(self, insert_batch_size: int = DEFAULT_INSERT_BATCH_SIZE) -> Sequence[Credits]:
        gen = super().load(insert_batch_size)
        return [credits for credits in gen]


if __name__ == '__main__':
    fire.Fire(CreditsLoader)
