from cyvcf2 import VCF
from pydantic import BaseModel, Field, PrivateAttr
from typing import Dict, Optional, List

CHROM_COL_NAME = "#CHROM"

class FileIndex(BaseModel):
    by_name: Dict[str, int] = Field(alias="by_name")
    by_index: Dict[int, str] = Field(alias="by_index")
    _readable: VCF = PrivateAttr(default=None)

    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def from_header(cls, header_line: str) -> "FileIndex":
        columns = header_line.split('\t')

        by_name = {}
        by_index = {}

        for idx, name in enumerate(columns):
            by_name[name] = idx
            by_index[idx] = name

        return cls(
            by_name=by_name,
            by_index=by_index
        )

    def get_by_name(self, name: str) -> Optional[int]:
        """Get column index by name"""
        return self.by_name.get(name)

    def get_by_index(self, idx: int) -> Optional[str]:
        """Get column name by index"""
        return self.by_index.get(idx)

    def __len__(self) -> int:
        return len(self.by_name)


def load_file(path: str, threads: int = 10) -> FileIndex:
    vcf = VCF(path, mode="r", lazy=True, threads=threads)

    header = next(line for line in reversed(vcf.raw_header.split('\n')) if line.startswith(CHROM_COL_NAME))

    file_index = FileIndex.from_header(header)
    file_index._readable = vcf

    return file_index
