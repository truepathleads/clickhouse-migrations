import hashlib
import os
from collections import namedtuple
from pathlib import Path
from typing import List, Optional, Union

Migration = namedtuple("Migration", ["version", "md5", "script"])


class MigrationStorage:
    def __init__(self, storage_dir: Union[Path, str], md5_source_dir: Optional[Union[Path, str]] = None):
        self.storage_dir: Path = Path(storage_dir)
        self.md5_source_dir: Optional[Path] = Path(md5_source_dir) if md5_source_dir else None

    def filenames(self) -> List[Path]:
        l: List[Path] = []
        for f in os.scandir(self.storage_dir):
            if f.name.endswith(".sql"):
                l.append(self.storage_dir / f.name)

        return l

    def _find_md5_source(self, sql_filename: str) -> Optional[Path]:
        if not self.md5_source_dir:
            return None
        version_prefix = sql_filename.split("_")[0]
        for f in os.scandir(self.md5_source_dir):
            if f.name.startswith(version_prefix + "_"):
                return self.md5_source_dir / f.name
        return None

    def migrations(
        self, explicit_migrations: Optional[List[str]] = None
    ) -> List[Migration]:
        migrations: List[Migration] = []

        for full_path in self.filenames():
            version_string = full_path.name.split("_")[0]
            version_number = int(version_string)

            md5_source = self._find_md5_source(full_path.name)
            md5_bytes = md5_source.read_bytes() if md5_source else full_path.read_bytes()

            migration = Migration(
                version=version_number,
                script=str(full_path.read_text(encoding="utf8")),
                md5=hashlib.md5(md5_bytes).hexdigest(),
            )

            if (
                not explicit_migrations
                or full_path.name in explicit_migrations
                or full_path.stem in explicit_migrations
                or version_string in explicit_migrations
                or str(version_number) in explicit_migrations
            ):
                migrations.append(migration)

        migrations.sort(key=lambda m: m.version)

        return migrations
