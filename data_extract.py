import os
import pandas as pd
from datetime import datetime

def try_parse_datetime(date_str, time_str):
    candidates = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%d %H:%M",  # in case seconds missing
    ]
    ts = f"{date_str} {time_str}"
    for fmt in candidates:
        try:
            return datetime.strptime(ts, fmt)
        except Exception:
            continue
    raise ValueError(f"Unknown datetime format: '{ts}'")

def parse_plt_file(filepath, user_id, verbose=False):
    """Parses a single .plt file and returns a list of dicts (robust to minor format variations)."""
    data = []
    bad_lines = []
    total_lines = 0
    skipped = 0

    try:
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            # GeoLife has 6 header lines normally; if file shorter this will raise StopIteration which we catch.
            for _ in range(6):
                try:
                    next(f)
                except StopIteration:
                    break

            for line in f:
                total_lines += 1
                raw = line.strip()
                if not raw:
                    skipped += 1
                    continue

                # split and drop empty strings (handles trailing commas)
                parts = [p.strip() for p in raw.split(',') if p.strip() != ""]

                # require at least 6 meaningful fields (lat, lon, ?, alt, date, time)
                if len(parts) < 6:
                    skipped += 1
                    if len(bad_lines) < 5:
                        bad_lines.append(raw)
                    continue

                try:
                    # latitude & longitude should be first two entries
                    latitude = float(parts[0])
                    longitude = float(parts[1])

                    # altitude commonly at index 3, but if not use -3 (third from end)
                    if len(parts) > 3:
                        try:
                            altitude = float(parts[3])
                        except Exception:
                            altitude = float(parts[-3])
                    else:
                        altitude = float(parts[-3])

                    # date/time are almost always the last two fields
                    date_str = parts[-2]
                    time_str = parts[-1]

                    timestamp = try_parse_datetime(date_str, time_str)

                    data.append({
                        'user_id': user_id,
                        'latitude': latitude,
                        'longitude': longitude,
                        'altitude': altitude,
                        'timestamp': timestamp
                    })
                except ValueError as ve:
                    skipped += 1
                    if len(bad_lines) < 5:
                        bad_lines.append(f"{raw}  <-- {ve}")
                    continue

    except Exception as e:
        print(f"Error processing file {filepath}: {e}")
        return []

    if verbose and (skipped > 0 or total_lines > 0):
        print(f"File: {os.path.basename(filepath)} — total lines checked: {total_lines}, skipped: {skipped}")
        if bad_lines:
            print("Sample bad lines (up to 5):")
            for bl in bad_lines:
                print("  ", bl)

    return data

def process_geolife_dataset(base_path, output_csv_filename="geolife_trajectories_cleaned.csv", verbose=True, limit_files_per_user=None):
    all_data = []
    users_processed = 0
    for user_dir_name in sorted(os.listdir(base_path)):
        user_path = os.path.join(base_path, user_dir_name)
        if not os.path.isdir(user_path):
            continue

        trajectory_path = os.path.join(user_path, 'Trajectory')
        if not os.path.isdir(trajectory_path):
            continue

        users_processed += 1
        user_id = user_dir_name
        if verbose:
            print(f"\nProcessing user: {user_id}")

        file_count = 0
        for filename in sorted(os.listdir(trajectory_path)):
            if not filename.lower().endswith('.plt'):
                continue
            filepath = os.path.join(trajectory_path, filename)
            file_data = parse_plt_file(filepath, user_id, verbose=verbose)
            all_data.extend(file_data)
            file_count += 1

            if limit_files_per_user and file_count >= limit_files_per_user:
                break

        if verbose:
            print(f"  files read for user {user_id}: {file_count}")

    if all_data:
        print(f"\n✅ Total data points collected: {len(all_data)} (from {users_processed} users)")
        df = pd.DataFrame(all_data)
        df = df.sort_values(by=['user_id', 'timestamp']).reset_index(drop=True)
        df.to_csv(output_csv_filename, index=False)
        print(f"✅ Saved cleaned GeoLife trajectories to: {output_csv_filename}")
    else:
        print("\n⚠️ No data was processed. Please check the base_path and files for content/format issues.")

# Example usage:
if __name__ == "__main__":
    geolife_data_path = r"C:\Users\mitta\Downloads\archive (1)\Geolife Trajectories 1.3\Data"
    if not os.path.exists(geolife_data_path):
        print(f"Error: GeoLife data path not found: {geolife_data_path}")
    else:
        process_geolife_dataset(geolife_data_path, verbose=True, limit_files_per_user=5)  # limit used to debug quickly
