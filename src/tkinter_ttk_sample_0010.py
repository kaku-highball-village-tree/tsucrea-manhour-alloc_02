from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import List


def read_tsv_rows(objInputPath: Path) -> List[List[str]]:
    objRows: List[List[str]] = []
    with open(objInputPath, mode="r", encoding="utf-8-sig", newline="") as objFile:
        objReader = csv.reader(objFile, delimiter="\t")
        for objRow in objReader:
            objRows.append(list(objRow))
    return objRows


def write_tsv_rows(objOutputPath: Path, objRows: List[List[str]]) -> None:
    with open(objOutputPath, mode="w", encoding="utf-8", newline="") as objFile:
        objWriter = csv.writer(objFile, delimiter="\t", lineterminator="\n")
        for objRow in objRows:
            objWriter.writerow(objRow)


def build_step0007_output_path_from_step0006(objStep0006Path: Path) -> Path:
    pszFileName: str = objStep0006Path.name
    if "_step0006_" not in pszFileName:
        raise ValueError(f"Input is not step0006 file: {objStep0006Path}")
    pszOutputFileName: str = pszFileName.replace("_step0006_", "_step0007_", 1)
    return objStep0006Path.resolve().parent / pszOutputFileName


def parse_h_mm_ss_to_seconds(pszTimeText: str) -> int:
    objParts: List[str] = pszTimeText.strip().split(":")
    if len(objParts) != 3:
        raise ValueError(f"Invalid time format: {pszTimeText}")
    iHours: int = int(objParts[0])
    iMinutes: int = int(objParts[1])
    iSeconds: int = int(objParts[2])
    return iHours * 3600 + iMinutes * 60 + iSeconds


def format_seconds_as_h_mm_ss(iTotalSeconds: int) -> str:
    iHours: int = iTotalSeconds // 3600
    iMinutes: int = (iTotalSeconds % 3600) // 60
    iSeconds: int = iTotalSeconds % 60
    return f"{iHours}:{iMinutes:02d}:{iSeconds:02d}"


def is_staff_start_row(objRow: List[str]) -> bool:
    if len(objRow) < 5:
        return False
    pszName: str = (objRow[3] or "").strip()
    pszProject: str = (objRow[4] or "").strip()
    return pszName != "" and pszProject == "合計"


def process_new_rawdata_step0007_from_step0006(objNewRawdataStep0006Path: Path) -> int:
    objInputRows: List[List[str]] = read_tsv_rows(objNewRawdataStep0006Path)
    if not objInputRows:
        raise ValueError(f"Input TSV has no rows: {objNewRawdataStep0006Path}")

    objOutputRows: List[List[str]] = [list(objRow) for objRow in objInputRows]

    iRowIndex: int = 0
    while iRowIndex < len(objOutputRows):
        objRow: List[str] = objOutputRows[iRowIndex]
        if not is_staff_start_row(objRow):
            iRowIndex += 1
            continue

        while len(objRow) < 6:
            objRow.append("")

        iTotalSeconds: int = 0
        iDetailIndex: int = iRowIndex + 1
        while iDetailIndex < len(objOutputRows):
            objDetailRow: List[str] = objOutputRows[iDetailIndex]
            if len(objDetailRow) >= 4 and (objDetailRow[3] or "").strip() != "":
                break

            if len(objDetailRow) >= 6:
                pszManhour: str = (objDetailRow[5] or "").strip()
                if pszManhour != "":
                    iTotalSeconds += parse_h_mm_ss_to_seconds(pszManhour)

            iDetailIndex += 1

        objRow[5] = format_seconds_as_h_mm_ss(iTotalSeconds)
        iRowIndex = iDetailIndex

    objOutputPath: Path = build_step0007_output_path_from_step0006(objNewRawdataStep0006Path)
    write_tsv_rows(objOutputPath, objOutputRows)
    return 0


def main() -> int:
    objParser = argparse.ArgumentParser()
    objParser.add_argument("input", help="Path to 新_ローデータ_シート_step0006_YYYY年MM月.tsv")
    objArgs = objParser.parse_args()
    return process_new_rawdata_step0007_from_step0006(Path(objArgs.input))


if __name__ == "__main__":
    raise SystemExit(main())
