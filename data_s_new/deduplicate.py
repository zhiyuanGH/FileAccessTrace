import os
import pandas as pd

def deduplicate_csvs(data_s_dir):
    """
    Deduplicates merged CSV files in the data_s directory based on the 'Filename' column.
    The deduplicated CSVs overwrite the original merged CSV files.

    Parameters:
    - data_s_dir (str): Path to the data_s directory containing merged CSV files.
    """
    if not os.path.isdir(data_s_dir):
        print(f"Error: The directory '{data_s_dir}' does not exist.")
        return

    # List all CSV files in the data_s directory
    csv_files = [f for f in os.listdir(data_s_dir) if f.endswith('.csv')]

    if not csv_files:
        print(f"No CSV files found in '{data_s_dir}'. Nothing to deduplicate.")
        return

    print(f"Found {len(csv_files)} CSV file(s) in '{data_s_dir}' to deduplicate.\n")

    for csv_file in csv_files:
        csv_path = os.path.join(data_s_dir, csv_file)
        print(f"Processing '{csv_file}'...")

        try:
            df = pd.read_csv(csv_path)
            print(f"  - Read {len(df)} rows.")

            if 'Filename' not in df.columns:
                print(f"  - Warning: 'Filename' column not found in '{csv_file}'. Skipping deduplication for this file.\n")
                continue

            before_dedup = len(df)
            # Remove duplicates based on 'Filename', keeping the first occurrence
            dedup_df = df.drop_duplicates(subset=['Filename'], keep='first')
            after_dedup = len(dedup_df)
            duplicates_removed = before_dedup - after_dedup

            if duplicates_removed > 0:
                dedup_df.to_csv(csv_path, index=False)
                print(f"  - Removed {duplicates_removed} duplicate(s). Saved deduplicated CSV.")
            else:
                print(f"  - No duplicates found. No changes made.")

            print()  # Blank line for readability

        except Exception as e:
            print(f"  - Error processing '{csv_file}': {e}\n")

    print("Deduplication process completed.")

if __name__ == "__main__":
    # Define the data_s directory path
    data_s = "/home/base/code/box/data_s_new"  # Adjust the path if necessary

    print("Starting deduplication of merged CSV files...")
    deduplicate_csvs(data_s)
