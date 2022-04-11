import calendar
from datetime import datetime as dt
import numpy as np
import os
import pandas as pd


def generate_current_calendar(
	csv_file: str = "DOCS/races.csv",
	year: int = int(dt.utcnow().year),
):
	column_names = [
		"raceId", "year", "round", "circuitId", "name", "race_date", "race_time", "url", "fp1_date", "fp1_time",
		"fp2_date", "fp2_time", "fp3_date", "fp3_time", "quali_date", "quali_time", "spr_date", "spr_time"
	]

	# Read csv
	with open(csv_file, "r") as f:
		df = pd.read_csv(f, names=column_names, na_values="\\N", header=0)

	# Fetch races in current year
	df = df[df["year"] == year]
	if not len(df):
		print(f"No information available for the {year} calendar year.")
		return
	sessions = ["fp1", "fp2", "fp3", "quali", "spr", "race"]

	for session in sessions:
		# Concatenate dates and times of sessions
		df[f"{session}_time"] = df[f"{session}_date"] + df[f"{session}_time"]

		# Convert to timestamp
		df[f"{session}"] = df[f"{session}_time"].apply(
			lambda x: x if pd.isnull(x) else calendar.timegm(dt.strptime(str(x), '%Y-%m-%d%H:%M:%S').utctimetuple()))

		# Delete session date column
		df.drop(f"{session}_date", axis=1, inplace=True)
		df.drop(f"{session}_time", axis=1, inplace=True)

	# Generate row entry for every session
	df = pd.melt(
		frame=df,
		id_vars=[x for x in df.columns.tolist() if x not in sessions],
		value_vars=sessions,
		var_name="session",
		value_name="start",
	)

	# Remove entries with invalid sessions (Sprint in regular weekends / FP3 in sprint weekends)
	df = df[pd.notnull(df["start"])]

	# Sort in chronological order
	df.sort_values(["start"], inplace=True)
	df.reset_index(drop=True, inplace=True)

	# Generate sessionId
	df.insert(0, "sessionId", df["raceId"] * 10 + df.groupby("raceId").cumcount() * 2)

	# Scheduled session start and end times (2hrs for race, 1hr otherwise)
	df["start"] = df["start"].map(int)
	df["end"] = np.where(df["session"] != "race", df["start"] + 3600, df["start"] + 7200)

	# Write to csv
	if not os.path.exists(f"DOCS/calendars/{year}"):
		os.makedirs(f"DOCS/calendars/{year}")
	df.to_csv(f"DOCS/calendars/{year}/{dt.utcnow().strftime(f'{year}_calendar_v%y%m%d.csv')}", index=False)

	return
