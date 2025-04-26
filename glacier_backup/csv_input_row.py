import dataclasses
from typing import Union, Optional

from file_data import UPLOAD_TIME_ONCE, UPLOAD_TIME_EVERY_BACKUP


@dataclasses.dataclass
class CsvInputRow:
    file_path: str
    upload_time: Union[UPLOAD_TIME_ONCE, UPLOAD_TIME_EVERY_BACKUP]
    output_file_path: Optional[str] = None
