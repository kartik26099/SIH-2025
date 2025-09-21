import csv
import io
import re
from collections import Counter
from typing import List, Optional, Tuple

import pandas as pd


def sniff_delimiter_from_file(path: str, read_bytes: int = 200_000) -> Tuple[Optional[str], Optional[bool]]:
	"""Try to sniff delimiter and header presence using csv.Sniffer on raw bytes."""
	for encoding in ("utf-8", "utf-8-sig", "latin-1"):
		try:
			with open(path, "r", encoding=encoding, errors="ignore") as f:
				sample = f.read(read_bytes)
			if not sample:
				return None, None
			try:
				dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "|", "\t"])  # type: ignore
				has_header = None
				try:
					has_header = csv.Sniffer().has_header(sample)  # type: ignore
				except Exception:
					pass
				return dialect.delimiter, has_header
			except Exception:
				continue
		except Exception:
			continue
	return None, None


def read_csv_robust(path: str) -> pd.DataFrame:
	"""Read a messy CSV robustly with dtype=str and inferred separator.

	Tries Sniffer first, then pandas inference, then fallback candidates.
	"""
	read_kwargs = dict(dtype=str, engine="python", keep_default_na=False, na_filter=False)
	# Attempt sniffer
	delimiter, _ = sniff_delimiter_from_file(path)
	if delimiter:
		for encoding in ("utf-8", "utf-8-sig", "latin-1"):
			try:
				df = pd.read_csv(path, sep=delimiter, encoding=encoding, **read_kwargs)
				if df.shape[1] > 1:
					return df
				# If single column, keep trying
			except Exception:
				continue
	# Pandas inference
	for encoding in ("utf-8", "utf-8-sig", "latin-1"):
		try:
			df = pd.read_csv(path, sep=None, encoding=encoding, **read_kwargs)
			if df.shape[1] > 1:
				return df
		except Exception:
			continue
	# Try candidate delimiters and pick max columns
	best_df: Optional[pd.DataFrame] = None
	for delim in [",", ";", "|", "\t"]:
		for encoding in ("utf-8", "utf-8-sig", "latin-1"):
			try:
				df = pd.read_csv(path, sep=delim, encoding=encoding, **read_kwargs)
				if best_df is None or df.shape[1] > best_df.shape[1]:
					best_df = df
			except Exception:
				continue
	if best_df is not None:
		return best_df
	# Final fallback: read as single column
	return pd.read_csv(path, sep="\n", header=None, names=["col_1"], engine="python", **read_kwargs)


def is_sentence_like(text: str) -> bool:
	"""Heuristic to detect narrative sentence rows from PDF conversions.

	Flags long strings with many spaces and punctuation and few digits.
	"""
	if text is None:
		return False
	text = str(text).strip()
	if not text:
		return False
	# Very long cell
	if len(text) >= 120:
		return True
	# Contains sentence punctuation and multiple spaces
	punct = sum(1 for c in text if c in ".,;:!?()-")
	spaces = text.count(" ")
	digits = sum(ch.isdigit() for ch in text)
	letters = sum(ch.isalpha() for ch in text)
	if letters >= 20 and spaces >= 6 and punct >= 2 and digits <= 2:
		return True
	# Looks like page header/footer
	if re.search(r"\bPage\s+\d+\b", text, re.IGNORECASE):
		return True
	return False


def drop_sentence_rows(df: pd.DataFrame) -> pd.DataFrame:
	"""Drop rows that look like narrative/sentences or pure headers/footers."""
	mask_keep = []
	for _, row in df.iterrows():
		cells = [str(v) if v is not None else "" for v in row.tolist()]
		non_empty = [c for c in cells if c.strip() != ""]
		joined = " ".join(non_empty)
		row_is_sentence = False
		if len(non_empty) <= 2:
			row_is_sentence = any(is_sentence_like(c) for c in non_empty) or is_sentence_like(joined)
		if not row_is_sentence and len(non_empty) == 1 and len(non_empty[0]) >= 80:
			row_is_sentence = True
		mask_keep.append(not row_is_sentence)
	return df.loc[mask_keep].reset_index(drop=True)


def infer_target_col_count(df: pd.DataFrame, sample_size: int = 1000) -> int:
	"""Infer the intended number of columns by taking the mode of non-empty counts."""
	n = min(len(df), sample_size)
	counts: List[int] = []
	for i in range(n):
		row = df.iloc[i]
		counts.append(sum(1 for v in row.tolist() if str(v).strip() != ""))
	if not counts:
		return df.shape[1]
	ctr = Counter(counts)
	# Prefer larger counts in a tie to avoid underfitting
	most_common = sorted(ctr.items(), key=lambda kv: (kv[1], kv[0]))
	return most_common[-1][0]


def split_cell_smart(cell: str) -> List[str]:
	"""Attempt to split a merged cell by common delimiters or multiple spaces."""
	if cell is None:
		return [""]
	cell = str(cell)
	for delim in ["|", ";", "\t", ","]:
		if delim in cell:
			parts = [p.strip() for p in cell.split(delim)]
			return parts
	# Split on 2+ spaces
	parts = re.split(r"\s{2,}", cell.strip())
	return [p.strip() for p in parts if p.strip() != ""] or [cell.strip()]


def normalize_columns(df: pd.DataFrame, target_cols: int) -> pd.DataFrame:
	"""Normalize rows so that each has exactly target_cols columns.

	- If a row has fewer columns, try splitting the largest text cell to fill.
	- If a row has more columns, merge trailing extras into the last column.
	"""
	records: List[List[str]] = []
	for _, row in df.iterrows():
		cells = [str(v) if v is not None else "" for v in row.tolist()]
		# Trim to actual content length by removing trailing empty cells
		while cells and cells[-1].strip() == "":
			cells.pop()
		if len(cells) == target_cols:
			records.append(cells)
			continue
		if len(cells) < target_cols:
			# Try to split the largest cell
			if cells:
				idx_largest = max(range(len(cells)), key=lambda i: len(cells[i]))
				splits = split_cell_smart(cells[idx_largest])
				new_cells = cells[:idx_largest] + splits + cells[idx_largest + 1 :]
			else:
				new_cells = cells
			# Pad if still short
			if len(new_cells) < target_cols:
				new_cells = new_cells + [""] * (target_cols - len(new_cells))
			# Truncate if overshot
			records.append(new_cells[:target_cols])
			continue
		# len(cells) > target_cols: merge extras into last
		merged = cells[: target_cols - 1]
		last = " ".join(cells[target_cols - 1 :]).strip()
		merged.append(last)
		records.append(merged)
	return pd.DataFrame(records)


def infer_header(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
	"""Infer header row by selecting the first row that looks header-like.

	Heuristic: choose the earliest row whose cells are mostly short alphabetic tokens
	and unique-ish; otherwise, fallback to generic names.
	"""
	def score_header_row(values: List[str]) -> float:
		if not values:
			return -1.0
		scores = 0.0
		seen = set()
		for v in values:
			v = str(v).strip()
			if not v:
				continue
			# Prefer alphabetic short tokens
			alpha_ratio = sum(ch.isalpha() for ch in v) / max(1, len(v))
			if alpha_ratio >= 0.6 and len(v) <= 30:
				scores += 1.0
			# Penalize duplicates
			if v in seen:
				scores -= 0.5
			seen.add(v)
		return scores

	best_idx = None
	best_score = -1.0
	for i in range(min(20, len(df))):
		row_vals = [str(v) for v in df.iloc[i].tolist()]
		s = score_header_row(row_vals)
		if s > best_score:
			best_score = s
			best_idx = i

	if best_idx is None:
		columns = [f"col_{i+1}" for i in range(df.shape[1])]
		return df.reset_index(drop=True), columns

	headers = [str(v).strip() or f"col_{i+1}" for i, v in enumerate(df.iloc[best_idx].tolist())]
	# Deduplicate header names
	ctr = Counter()
	final_headers: List[str] = []
	for name in headers:
		ctr[name] += 1
		final_name = name if ctr[name] == 1 else f"{name}_{ctr[name]}"
		final_headers.append(final_name)

	body = df.iloc[best_idx + 1 :].reset_index(drop=True)
	return body, final_headers


def clean_premonsoon_csv(input_csv: str,
						 output_csv: str,
						 output_xlsx: Optional[str] = None) -> Tuple[str, Optional[str]]:
	"""Clean the messy pre-monsoon CSV and write cleaned CSV and optional Excel.

	Returns the output paths written.
	"""
	# 1) Read
	df_raw = read_csv_robust(input_csv)

	# If columns collapsed to 1, attempt to split that column into multiple using smart splits
	if df_raw.shape[1] == 1:
		series = df_raw.iloc[:, 0].astype(str)
		split_rows: List[List[str]] = []
		for val in series.tolist():
			parts = split_cell_smart(val)
			split_rows.append(parts)
		max_len = max((len(r) for r in split_rows), default=1)
		split_rows = [r + [""] * (max_len - len(r)) for r in split_rows]
		df_raw = pd.DataFrame(split_rows)

	# 2) Drop sentence-like rows
	df = drop_sentence_rows(df_raw)

	# 3) Infer target column count and normalize
	target_cols = max(infer_target_col_count(df), df.shape[1], 1)
	df_norm = normalize_columns(df, target_cols)

	# 4) Infer headers
	body, headers = infer_header(df_norm)
	body.columns = headers

	# 5) Trim entirely empty rows
	body = body[~(body.apply(lambda r: all(str(v).strip() == "" for v in r), axis=1))]
	body = body.reset_index(drop=True)

	# 6) Write outputs
	body.to_csv(output_csv, index=False, quoting=csv.QUOTE_MINIMAL)
	xlsx_written = None
	if output_xlsx:
		body.to_excel(output_xlsx, index=False)
		xlsx_written = output_xlsx

	return output_csv, xlsx_written


if __name__ == "__main__":
	input_path = "pre-monsoon_2014-2024.csv"
	csv_out = "pre-monsoon_2014-2024.cleaned.csv"
	xlsx_out = "pre-monsoon_2014-2024.cleaned.xlsx"
	csv_path, xlsx_path = clean_premonsoon_csv(input_path, csv_out, xlsx_out)
	print(f"Cleaned CSV written: {csv_path}")
	if xlsx_path:
		print(f"Cleaned Excel written: {xlsx_path}")