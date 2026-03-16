from __future__ import annotations

import argparse
import csv
import re
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import List


INVALID_FILE_CHARS_PATTERN: re.Pattern[str] = re.compile(r'[\\/:*?"<>|]')
YEAR_MONTH_PATTERN: re.Pattern[str] = re.compile(r"(\d{2})\.(\d{1,2})月")
DURATION_TEXT_PATTERN: re.Pattern[str] = re.compile(r"^\s*(\d+)\s+day(?:s)?,\s*(\d+):(\d{2}):(\d{2})\s*$")
TIME_TEXT_PATTERN: re.Pattern[str] = re.compile(r"^\d+:\d{2}:\d{2}$")
HM_PATTERN: re.Pattern[str] = re.compile(r"^(\d+):(\d{2})$")


def build_candidate_paths(pszInputPath: str) -> List[Path]:
    objInputPath: Path = Path(pszInputPath)
    objScriptDirectoryPath: Path = Path(__file__).resolve().parent
    objInputDirectoryPath: Path = Path.cwd() / "input"
    return [
        objInputPath,
        objScriptDirectoryPath / pszInputPath,
        objInputDirectoryPath / pszInputPath,
    ]


def resolve_existing_input_path(pszInputPath: str) -> Path:
    for objCandidatePath in build_candidate_paths(pszInputPath):
        if objCandidatePath.exists():
            return objCandidatePath
    raise FileNotFoundError(f"Input file not found: {pszInputPath}")


def sanitize_sheet_name_for_file_name(pszSheetName: str) -> str:
    pszSanitized: str = INVALID_FILE_CHARS_PATTERN.sub("_", pszSheetName).strip()
    if pszSanitized == "":
        return "Sheet"
    return pszSanitized


def build_unique_output_path(
    objBaseDirectoryPath: Path,
    pszExcelStem: str,
    pszSanitizedSheetName: str,
    objUsedPaths: set[Path],
) -> Path:
    objOutputPath: Path = objBaseDirectoryPath / f"{pszExcelStem}_{pszSanitizedSheetName}.tsv"
    if objOutputPath not in objUsedPaths:
        objUsedPaths.add(objOutputPath)
        return objOutputPath

    iSuffix: int = 2
    while True:
        objCandidatePath: Path = (
            objBaseDirectoryPath / f"{pszExcelStem}_{pszSanitizedSheetName}_{iSuffix}.tsv"
        )
        if objCandidatePath not in objUsedPaths:
            objUsedPaths.add(objCandidatePath)
            return objCandidatePath
        iSuffix += 1


def format_timedelta_as_h_mm_ss(objDuration: timedelta) -> str:
    iTotalSeconds: int = int(objDuration.total_seconds())
    iSign: int = -1 if iTotalSeconds < 0 else 1
    iAbsTotalSeconds: int = abs(iTotalSeconds)
    iHours: int = iAbsTotalSeconds // 3600
    iMinutes: int = (iAbsTotalSeconds % 3600) // 60
    iSeconds: int = iAbsTotalSeconds % 60
    pszPrefix: str = "-" if iSign < 0 else ""
    return f"{pszPrefix}{iHours}:{iMinutes:02d}:{iSeconds:02d}"


def normalize_cell_value(objValue: object) -> str:
    if objValue is None:
        return ""
    if isinstance(objValue, timedelta):
        return format_timedelta_as_h_mm_ss(objValue)
    return str(objValue).replace("\t", "_")


def write_sheet_to_tsv(objOutputPath: Path, objRows: List[List[object]]) -> None:
    with open(objOutputPath, mode="w", encoding="utf-8", newline="") as objFile:
        objWriter: csv.writer = csv.writer(objFile, delimiter="\t", lineterminator="\n")
        for objRow in objRows:
            objWriter.writerow([normalize_cell_value(objValue) for objValue in objRow])


def format_xlsx_cell_value_for_tsv(objValue: object) -> object:
    if isinstance(objValue, datetime):
        if (
            objValue.hour == 0
            and objValue.minute == 0
            and objValue.second == 0
            and objValue.microsecond == 0
        ):
            return objValue.strftime("%Y/%m/%d")
        return objValue.strftime("%Y/%m/%d %H:%M:%S")

    if isinstance(objValue, date):
        return objValue.strftime("%Y/%m/%d")

    if isinstance(objValue, time):
        if objValue.second == 0 and objValue.microsecond == 0:
            return f"{objValue.hour}:{objValue.minute:02d}"
        return f"{objValue.hour}:{objValue.minute:02d}:{objValue.second:02d}"

    if isinstance(objValue, timedelta):
        pszText: str = format_timedelta_as_h_mm_ss(objValue)
        return re.sub(r"^(\d+):(\d{2}):00$", r"\1:\2", pszText)

    return objValue


def convert_xlsx_rows_to_tsv_file(objOutputPath: Path, objRows: List[List[object]]) -> None:
    objNormalizedRows: List[List[object]] = []
    for objRow in objRows:
        objNormalizedRows.append([
            format_xlsx_cell_value_for_tsv(objValue) for objValue in objRow
        ])
    write_sheet_to_tsv(objOutputPath, objNormalizedRows)


def read_tsv_rows(objInputPath: Path) -> List[List[str]]:
    objRows: List[List[str]] = []
    with open(objInputPath, mode="r", encoding="utf-8-sig", newline="") as objFile:
        objReader = csv.reader(objFile, delimiter="\t")
        for objRow in objReader:
            objRows.append(list(objRow))
    return objRows


def is_blank_text(pszText: str) -> bool:
    return (pszText or "").strip().replace("\u3000", "") == ""


def get_effective_column_count(objRow: List[str]) -> int:
    for iIndex in range(len(objRow) - 1, -1, -1):
        if not is_blank_text(objRow[iIndex]):
            return iIndex + 1
    return 0


def is_jobcan_long_format_tsv(objRows: List[List[str]]) -> bool:
    objNonEmptyRows: List[List[str]] = [
        objRow for objRow in objRows if any(not is_blank_text(pszCell) for pszCell in objRow)
    ]
    if not objNonEmptyRows:
        return False

    iTotal: int = len(objNonEmptyRows)
    iFourColumnsLike: int = 0
    iTimeTextRows: int = 0
    iProjectCodeRows: int = 0
    for objRow in objNonEmptyRows:
        iEffectiveColumns: int = get_effective_column_count(objRow)
        if 3 <= iEffectiveColumns <= 5:
            iFourColumnsLike += 1

        if len(objRow) >= 4:
            pszTimeText: str = (objRow[3] or "").strip()
            if TIME_TEXT_PATTERN.match(pszTimeText) is not None or DURATION_TEXT_PATTERN.match(pszTimeText) is not None:
                iTimeTextRows += 1

        if len(objRow) >= 2:
            pszProjectText: str = (objRow[1] or "").strip()
            if re.match(r"^(P\d{5}|[A-OQ-Z]\d{3})", pszProjectText) is not None:
                iProjectCodeRows += 1

    return (
        iFourColumnsLike / iTotal >= 0.7
        and iTimeTextRows / iTotal >= 0.5
        and iProjectCodeRows / iTotal >= 0.5
    )


def build_step0001_output_path_from_manhour_tsv(objInputPath: Path) -> Path:
    objMatch = YEAR_MONTH_PATTERN.search(objInputPath.stem)
    if objMatch is None:
        raise ValueError(f"Could not extract YY.MM月 from input path: {objInputPath}")
    iYear: int = 2000 + int(objMatch.group(1))
    iMonth: int = int(objMatch.group(2))
    pszOutputFileName: str = f"プロジェクト_工数_step0001_{iYear}年{iMonth:02d}月.tsv"
    return objInputPath.resolve().with_name(pszOutputFileName)




def build_step0002_output_path_from_step0001(objStep0001Path: Path) -> Path:
    pszFileName: str = objStep0001Path.name
    if "_step0001_" not in pszFileName:
        raise ValueError(f"Input is not step0001 file: {objStep0001Path}")
    pszOutputFileName: str = pszFileName.replace("_step0001_", "_step0002_", 1)
    return objStep0001Path.resolve().parent / pszOutputFileName


def build_step0003_output_path_from_step0002(objStep0002Path: Path) -> Path:
    pszFileName: str = objStep0002Path.name
    if "_step0002_" not in pszFileName:
        raise ValueError(f"Input is not step0002 file: {objStep0002Path}")
    pszOutputFileName: str = pszFileName.replace("_step0002_", "_step0003_", 1)
    return objStep0002Path.resolve().parent / pszOutputFileName




def build_step0004_output_path_from_step0003(objStep0003Path: Path) -> Path:
    pszFileName: str = objStep0003Path.name
    if "_step0003_" not in pszFileName:
        raise ValueError(f"Input is not step0003 file: {objStep0003Path}")
    pszOutputFileName: str = pszFileName.replace("_step0003_", "_step0004_", 1)
    return objStep0003Path.resolve().parent / pszOutputFileName



def build_step0005_output_path_from_step0004(objStep0004Path: Path) -> Path:
    pszFileName: str = objStep0004Path.name
    if "_step0004_" not in pszFileName:
        raise ValueError(f"Input is not step0004 file: {objStep0004Path}")
    pszOutputFileName: str = pszFileName.replace("_step0004_", "_step0005_", 1)
    return objStep0004Path.resolve().parent / pszOutputFileName


def parse_h_mm_ss_text_to_seconds(pszText: str) -> int | None:
    pszValue: str = (pszText or "").strip()
    if pszValue == "":
        return None

    objMatch = re.match(r"^(\d+):(\d{2}):(\d{2})$", pszValue)
    if objMatch is not None:
        iHours: int = int(objMatch.group(1))
        iMinutes: int = int(objMatch.group(2))
        iSeconds: int = int(objMatch.group(3))
        return iHours * 3600 + iMinutes * 60 + iSeconds

    objMatch = re.match(r"^(\d+):(\d{2})$", pszValue)
    if objMatch is not None:
        iHours = int(objMatch.group(1))
        iMinutes = int(objMatch.group(2))
        return iHours * 3600 + iMinutes * 60

    return None


def format_seconds_as_h_mm_ss(iTotalSeconds: int) -> str:
    iHours: int = iTotalSeconds // 3600
    iMinutes: int = (iTotalSeconds % 3600) // 60
    iSeconds: int = iTotalSeconds % 60
    return f"{iHours}:{iMinutes:02d}:{iSeconds:02d}"

def normalize_project_name_for_step0003(pszProjectName: str) -> str:
    pszNormalized: str = (pszProjectName or "").replace("\t", "_")
    pszNormalized = re.sub(r"(P\d{5})(?![ _\t　【])", r"\1_", pszNormalized)
    pszNormalized = re.sub(r"([A-OQ-Z]\d{3})(?![ _\t　【])", r"\1_", pszNormalized)
    pszNormalized = re.sub(r"((?:P\d{5}|[A-OQ-Z]\d{3}))[\u0020\u3000]+", r"\1_", pszNormalized)
    pszNormalized = re.sub(r"^([A-OQ-Z]\d{3}) +", r"\1_", pszNormalized)

    objMatch = re.match(r"^【([^】]+)】\s*((?:P\d{5}|[A-OQ-Z]\d{3}))(.*)$", pszNormalized)
    if objMatch is not None:
        pszTag: str = objMatch.group(1)
        pszCode: str = objMatch.group(2)
        pszRest: str = objMatch.group(3)
        pszRest = re.sub(r"^[\u0020\u3000_]+", "", pszRest)
        if pszRest != "":
            pszNormalized = f"{pszCode}_【{pszTag}】{pszRest}"
        else:
            pszNormalized = f"{pszCode}_【{pszTag}】"

    pszNormalized = re.sub(r"((?:P\d{5}|[A-OQ-Z]\d{3}))(?=【)", r"\1_", pszNormalized)
    return pszNormalized


def remove_first_and_third_columns(objRows: List[List[str]]) -> List[List[str]]:
    objOutputRows: List[List[str]] = []
    for objRow in objRows:
        pszSecondColumn: str = objRow[1] if len(objRow) >= 2 else ""
        pszFourthColumn: str = objRow[3] if len(objRow) >= 4 else ""
        objOutputRows.append([pszSecondColumn, pszFourthColumn])
    return objOutputRows


def process_step0002_from_step0001(objStep0001Path: Path) -> int:
    objRows: List[List[str]] = read_tsv_rows(objStep0001Path)
    objOutputRows: List[List[str]] = remove_first_and_third_columns(objRows)
    objOutputPath: Path = build_step0002_output_path_from_step0001(objStep0001Path)
    write_sheet_to_tsv(objOutputPath, objOutputRows)
    process_step0003_from_step0002(objOutputPath)
    return 0


def process_step0003_from_step0002(objStep0002Path: Path) -> int:
    objRows: List[List[str]] = read_tsv_rows(objStep0002Path)
    objOutputRows: List[List[str]] = []
    for objRow in objRows:
        objNewRow: List[str] = list(objRow)
        if len(objNewRow) >= 1:
            objNewRow[0] = normalize_project_name_for_step0003((objNewRow[0] or "").strip())
        objOutputRows.append(objNewRow)

    objOutputPath: Path = build_step0003_output_path_from_step0002(objStep0002Path)
    write_sheet_to_tsv(objOutputPath, objOutputRows)
    process_step0004_from_step0003(objOutputPath)
    return 0


def process_step0004_from_step0003(objStep0003Path: Path) -> int:
    objRows: List[List[str]] = read_tsv_rows(objStep0003Path)
    objOutputRows: List[List[str]] = [list(objRow) for objRow in objRows]
    objOutputRows.sort(key=lambda objRow: (objRow[0] or "").strip() if len(objRow) >= 1 else "")

    objOutputPath: Path = build_step0004_output_path_from_step0003(objStep0003Path)
    write_sheet_to_tsv(objOutputPath, objOutputRows)
    process_step0005_from_step0004(objOutputPath)
    return 0



def process_step0005_from_step0004(objStep0004Path: Path) -> int:
    objRows: List[List[str]] = read_tsv_rows(objStep0004Path)

    objTotalSecondsByProject: dict[str, int] = {}
    for objRow in objRows:
        pszProjectName: str = (objRow[0] or "").strip() if len(objRow) >= 1 else ""
        if pszProjectName == "":
            continue

        pszManhour: str = (objRow[1] or "").strip() if len(objRow) >= 2 else ""
        iSeconds = parse_h_mm_ss_text_to_seconds(pszManhour)
        if iSeconds is None:
            continue

        if pszProjectName not in objTotalSecondsByProject:
            objTotalSecondsByProject[pszProjectName] = 0
        objTotalSecondsByProject[pszProjectName] += iSeconds

    objOutputRows: List[List[str]] = [
        [pszProjectName, format_seconds_as_h_mm_ss(iTotalSeconds)]
        for pszProjectName, iTotalSeconds in objTotalSecondsByProject.items()
    ]

    objOutputPath: Path = build_step0005_output_path_from_step0004(objStep0004Path)
    write_sheet_to_tsv(objOutputPath, objOutputRows)
    return 0

def is_fourth_column_manhour_h_mm_tsv(objRows: List[List[str]]) -> bool:
    objNonEmptyRows: List[List[str]] = [
        objRow for objRow in objRows if any(not is_blank_text(pszCell) for pszCell in objRow)
    ]
    if not objNonEmptyRows:
        return False

    iTotal: int = len(objNonEmptyRows)
    iHmRows: int = 0
    for objRow in objNonEmptyRows:
        if len(objRow) < 4:
            continue
        pszTimeText: str = (objRow[3] or "").strip()
        if HM_PATTERN.match(pszTimeText) is not None:
            iHmRows += 1
    return iHmRows / iTotal >= 0.5


def build_h_mm_ss_output_path_from_input_tsv(objInputPath: Path) -> Path:
    return objInputPath.resolve().with_name(f"{objInputPath.stem}_h_mm_ss.tsv")


def convert_manhour_h_mm_to_h_mm_ss_rows(objRows: List[List[str]]) -> List[List[str]]:
    objConvertedRows: List[List[str]] = []
    for objRow in objRows:
        objNewRow: List[str] = list(objRow)
        if len(objNewRow) >= 4:
            pszManhour: str = (objNewRow[3] or "").strip()
            objMatch = HM_PATTERN.match(pszManhour)
            if objMatch is not None:
                iHours: int = int(objMatch.group(1))
                iMinutes: int = int(objMatch.group(2))
                objNewRow[3] = f"{iHours}:{iMinutes:02d}:00"
        objConvertedRows.append(objNewRow)
    return objConvertedRows


def process_tsv_input(objResolvedInputPath: Path) -> int:
    objRows: List[List[str]] = read_tsv_rows(objResolvedInputPath)
    if len(objRows) < 2:
        raise ValueError(f"Input TSV has too few rows: {objResolvedInputPath}")

    if is_jobcan_long_format_tsv(objRows):
        objOutputPath: Path = build_step0001_output_path_from_manhour_tsv(objResolvedInputPath)
        write_sheet_to_tsv(objOutputPath, objRows)
        process_step0002_from_step0001(objOutputPath)
        return 0

    if not is_fourth_column_manhour_h_mm_tsv(objRows):
        raise ValueError(f"Unsupported TSV format: {objResolvedInputPath}")

    objOutputPath = build_h_mm_ss_output_path_from_input_tsv(objResolvedInputPath)
    objConvertedRows: List[List[str]] = convert_manhour_h_mm_to_h_mm_ss_rows(objRows)
    write_sheet_to_tsv(objOutputPath, objConvertedRows)

    objConvertedOutputRows: List[List[str]] = read_tsv_rows(objOutputPath)
    if is_jobcan_long_format_tsv(objConvertedOutputRows):
        objStep0001OutputPath: Path = build_step0001_output_path_from_manhour_tsv(objResolvedInputPath)
        write_sheet_to_tsv(objStep0001OutputPath, objConvertedOutputRows)
        process_step0002_from_step0001(objStep0001OutputPath)

    return 0


def process_single_input(pszInputXlsxPath: str) -> int:
    objResolvedInputPath: Path = resolve_existing_input_path(pszInputXlsxPath)
    pszSuffix: str = objResolvedInputPath.suffix.lower()

    if pszSuffix == ".tsv":
        return process_tsv_input(objResolvedInputPath)

    if pszSuffix != ".xlsx":
        raise ValueError(f"Unsupported extension (only .xlsx/.tsv): {objResolvedInputPath}")

    objBaseDirectoryPath: Path = objResolvedInputPath.resolve().parent
    pszExcelStem: str = objResolvedInputPath.stem

    try:
        import openpyxl
    except Exception as objException:
        raise RuntimeError(f"Failed to import openpyxl: {objException}") from objException

    try:
        objWorkbook = openpyxl.load_workbook(
            filename=objResolvedInputPath,
            read_only=True,
            data_only=True,
        )
    except Exception as objException:
        raise RuntimeError(f"Failed to read workbook: {objResolvedInputPath}. Detail = {objException}") from objException

    objUsedPaths: set[Path] = set()
    try:
        for objWorksheet in objWorkbook.worksheets:
            pszSanitizedSheetName: str = sanitize_sheet_name_for_file_name(objWorksheet.title)
            objOutputPath: Path = build_unique_output_path(
                objBaseDirectoryPath,
                pszExcelStem,
                pszSanitizedSheetName,
                objUsedPaths,
            )
            objRows: List[List[object]] = [list(objRow) for objRow in objWorksheet.iter_rows(values_only=True)]
            convert_xlsx_rows_to_tsv_file(objOutputPath, objRows)
    finally:
        objWorkbook.close()

    return 0


def main() -> int:
    objParser: argparse.ArgumentParser = argparse.ArgumentParser()
    objParser.add_argument(
        "pszInputXlsxPaths",
        nargs="+",
        help="Input file paths (.xlsx/.tsv)",
    )
    objArgs: argparse.Namespace = objParser.parse_args()

    iExitCode: int = 0
    for pszInputXlsxPath in objArgs.pszInputXlsxPaths:
        try:
            process_single_input(pszInputXlsxPath)
        except Exception as objException:
            print(
                "Error: failed to process input file: {0}. Detail = {1}".format(
                    pszInputXlsxPath,
                    objException,
                )
            )
            iExitCode = 1
            continue

    return iExitCode


if __name__ == "__main__":
    raise SystemExit(main())
