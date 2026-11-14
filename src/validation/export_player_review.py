
import os
from pathlib import Path
from collections import Counter

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.utils import get_column_letter


# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

VIEW_NAME = "sleeper_player_match_review_export"
OUTPUT_FILE = BASE_DIR / "player_match_review.xlsx"

PAGE_SIZE = 1000

# Load environment variables from the .env beside this script.
load_dotenv(dotenv_path=ENV_PATH, override=True)

SUPABASE_URL = os.getenv("SUPABASE_URL")

# Support either key variable name.
SUPABASE_KEY = (
    os.getenv("SUPABASE_KEY")
    or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or os.getenv("SUPABASE_SECRET_KEY")
)

if not ENV_PATH.exists():
    raise FileNotFoundError(
        f"Could not find .env file at: {ENV_PATH}"
    )

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError(
        "Missing Supabase credentials.\n"
        f"SUPABASE_URL loaded: {bool(SUPABASE_URL)}\n"
        f"SUPABASE_KEY loaded: {bool(SUPABASE_KEY)}\n"
        f".env location: {ENV_PATH}"
    )


# ============================================================
# CONNECT TO SUPABASE
# ============================================================

print("Connecting to Supabase...")

supabase = create_client(
    SUPABASE_URL,
    SUPABASE_KEY
)


# ============================================================
# DOWNLOAD ALL VIEW RECORDS
# ============================================================

def fetch_all_records():
    """Download every row from the review view using pagination."""

    all_records = []
    start = 0

    print(f"Downloading records from {VIEW_NAME}...")

    while True:
        end = start + PAGE_SIZE - 1

        response = (
            supabase
            .table(VIEW_NAME)
            .select("*")
            .range(start, end)
            .execute()
        )

        batch = response.data or []

        if not batch:
            break

        all_records.extend(batch)

        print(f"Downloaded {len(all_records):,} records...")

        if len(batch) < PAGE_SIZE:
            break

        start += PAGE_SIZE

    print(f"Finished downloading {len(all_records):,} records.")

    return all_records


records = fetch_all_records()

if not records:
    raise ValueError(
        "The review view returned no records. "
        "Check that the view exists and contains unmapped players."
    )

df = pd.DataFrame(records)


# ============================================================
# VALIDATE EXPECTED VIEW COLUMNS
# ============================================================

required_columns = ["recommendation"]

missing_columns = [
    column for column in required_columns
    if column not in df.columns
]

if missing_columns:
    raise ValueError(
        f"The review view is missing expected columns: {missing_columns}\n"
        f"Available columns: {list(df.columns)}"
    )

# Add review columns for decisions made in Excel.
if "review_decision" not in df.columns:
    df["review_decision"] = ""

if "review_notes" not in df.columns:
    df["review_notes"] = ""


# ============================================================
# SORT RECORDS
# ============================================================

sort_columns = [
    column for column in ["recommendation", "sleeper_name"]
    if column in df.columns
]

if sort_columns:
    df = df.sort_values(
        by=sort_columns,
        na_position="last"
    )


# ============================================================
# WORKSHEET CATEGORIES
# ============================================================

sheet_categories = {
    "Auto Approve": "AUTO APPROVE",
    "College Review": "MANUAL REVIEW - COLLEGE",
    "DOB Review": "MANUAL REVIEW - DOB",
    "Identifier Conflicts": "MANUAL REVIEW - IDENTIFIER",
    "Missing College": "MANUAL REVIEW - MISSING COLLEGE",
    "Multiple Candidates": "MANUAL REVIEW - MULTIPLE CANDIDATES",
    "Position Review": "MANUAL REVIEW - POSITION",
    "No Match": "NO MATCH",
}


# ============================================================
# SUMMARY INFORMATION
# ============================================================

def find_column(possible_names):
    """Find the first matching column name."""

    for name in possible_names:
        if name in df.columns:
            return name

    return None


sleeper_id_column = find_column([
    "sleeper_player_id",
    "source_player_id"
])

nflverse_id_column = find_column([
    "player_id",
    "nflverse_player_id",
    "candidate_player_id"
])

recommendation_counts = df["recommendation"].value_counts().to_dict()

summary_rows = [
    ["Metric", "Count"],
    ["Total candidate rows", len(df)],
]

if sleeper_id_column:
    summary_rows.append([
        "Unique Sleeper players",
        df[sleeper_id_column].nunique()
    ])

if nflverse_id_column:
    summary_rows.append([
        "Unique NFLverse player IDs",
        df[nflverse_id_column].nunique()
    ])

summary_rows.append(["", ""])
summary_rows.append(["Recommendation", "Candidate rows"])

for recommendation, count in sorted(recommendation_counts.items()):
    summary_rows.append([recommendation, count])


# ============================================================
# CREATE EXCEL WORKBOOK
# ============================================================

print("Creating Excel workbook...")

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:

    # --------------------------------------------------------
    # READ ME SHEET
    # --------------------------------------------------------

    readme = pd.DataFrame({
        "Instructions": [
            "DYNASTY DECISION ENGINE - PLAYER MATCH REVIEW",
            "",
            "Review the candidate player matches in this workbook.",
            "",
            "Use the review_decision column to record your decision.",
            "",
            "APPROVE: The Sleeper player and NFLverse player are the same person.",
            "HOLD: There is not enough evidence to approve or reject the match.",
            "REJECT: The proposed match is clearly the wrong player.",
            "",
            "Use review_notes to explain uncertain decisions or conflicts.",
            "",
            "Only APPROVE records should be inserted into bridge_player_ids.",
            "HOLD records should remain unmapped for later review.",
            "REJECT records should remain unmapped.",
            "",
            "Do not approve a match based on name alone.",
            "Compare DOB, college, position, GSIS ID, and ESPN ID.",
            "",
            "The All Candidates sheet contains the complete review dataset.",
            "",
            "After reviewing the workbook, upload it to ChatGPT for help",
            "generating the SQL inserts for approved matches."
        ]
    })

    readme.to_excel(
        writer,
        sheet_name="Read Me",
        index=False
    )

    # --------------------------------------------------------
    # SUMMARY SHEET
    # --------------------------------------------------------

    summary = pd.DataFrame(
        summary_rows[1:],
        columns=summary_rows[0]
    )

    summary.to_excel(
        writer,
        sheet_name="Summary",
        index=False
    )

    # --------------------------------------------------------
    # CATEGORY SHEETS
    # --------------------------------------------------------

    for sheet_name, recommendation in sheet_categories.items():

        category_df = df[
            df["recommendation"] == recommendation
        ].copy()

        category_df.to_excel(
            writer,
            sheet_name=sheet_name,
            index=False
        )

        print(
            f"{sheet_name}: {len(category_df):,} candidate rows"
        )

    # --------------------------------------------------------
    # ALL CANDIDATES SHEET
    # --------------------------------------------------------

    df.to_excel(
        writer,
        sheet_name="All Candidates",
        index=False
    )

    # --------------------------------------------------------
    # FORMAT WORKBOOK
    # --------------------------------------------------------

    workbook = writer.book

    header_fill = PatternFill(
        fill_type="solid",
        fgColor="1F4E78"
    )

    header_font = Font(
        color="FFFFFF",
        bold=True
    )

    decision_fill = PatternFill(
        fill_type="solid",
        fgColor="FFF2CC"
    )

    for worksheet in workbook.worksheets:

        worksheet.freeze_panes = "A2"
        worksheet.sheet_view.showGridLines = False

        # Format header row.
        for cell in worksheet[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(
                vertical="center",
                wrap_text=True
            )

        worksheet.row_dimensions[1].height = 30

        # Add filters when there is tabular data.
        if worksheet.max_row >= 2 and worksheet.max_column >= 1:
            worksheet.auto_filter.ref = worksheet.dimensions

        # Adjust column widths.
        for column_cells in worksheet.columns:

            column_letter = get_column_letter(
                column_cells[0].column
            )

            max_length = 0

            for cell in column_cells:
                if cell.value is not None:
                    max_length = max(
                        max_length,
                        len(str(cell.value))
                    )

            worksheet.column_dimensions[column_letter].width = min(
                max(max_length + 2, 12),
                45
            )

        # Wrap cell contents.
        for row in worksheet.iter_rows(min_row=2):
            for cell in row:
                cell.alignment = Alignment(
                    vertical="top",
                    wrap_text=True
                )

        # Add review decision dropdowns.
        if "review_decision" in [
            cell.value for cell in worksheet[1]
        ]:

            decision_column = [
                cell.column
                for cell in worksheet[1]
                if cell.value == "review_decision"
            ][0]

            decision_letter = get_column_letter(
                decision_column
            )

            validation = DataValidation(
                type="list",
                formula1='"APPROVE,HOLD,REJECT"',
                allow_blank=True
            )

            validation.error = (
                "Choose APPROVE, HOLD, or REJECT."
            )

            validation.errorTitle = "Invalid Decision"

            validation.prompt = (
                "Select a review decision."
            )

            validation.promptTitle = "Match Review"

            worksheet.add_data_validation(validation)

            validation.add(
                f"{decision_letter}2:"
                f"{decision_letter}{worksheet.max_row}"
            )

            # Highlight decision column.
            for row_number in range(2, worksheet.max_row + 1):
                worksheet[
                    f"{decision_letter}{row_number}"
                ].fill = decision_fill

    # Keep Read Me as the first worksheet.
    workbook._sheets = [
        workbook["Read Me"],
        workbook["Summary"],
        workbook["Auto Approve"],
        workbook["College Review"],
        workbook["DOB Review"],
        workbook["Identifier Conflicts"],
        workbook["Missing College"],
        workbook["Multiple Candidates"],
        workbook["Position Review"],
        workbook["No Match"],
        workbook["All Candidates"],
    ]


# ============================================================
# FINISHED
# ============================================================

print()
print("========================================")
print("PLAYER REVIEW EXPORT COMPLETE")
print("========================================")
print(f"Total candidate rows: {len(df):,}")
print(f"Output file: {OUTPUT_FILE}")
print()
print("Open player_match_review.xlsx and review the candidates.")
print("Use APPROVE, HOLD, or REJECT in the review_decision column.")
