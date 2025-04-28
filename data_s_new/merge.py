import os
import pandas as pd

def merge_csvs(data_f_dir, data_p_dir, data_s_dir):
    """
    Merges CSV files from data_f and data_p directories based on application names
    and outputs the merged CSVs to the data_s directory.
    
    Parameters:
    - data_f_dir (str): Path to the data_f directory.
    - data_p_dir (str): Path to the data_p directory.
    - data_s_dir (str): Path to the data_s directory where merged CSVs will be saved.
    """
    # Ensure the output directory exists
    os.makedirs(data_s_dir, exist_ok=True)
    print(f"Output directory '{data_s_dir}' is ready.")

    # Get list of applications by listing subdirectories in data_f and data_p
    apps_f = set(os.listdir(data_f_dir)) if os.path.isdir(data_f_dir) else set()
    apps_p = set(os.listdir(data_p_dir)) if os.path.isdir(data_p_dir) else set()
    all_apps = apps_f.union(apps_p)
    
    if not all_apps:
        print("No application directories found in data_f or data_p.")
        return

    print(f"Found applications: {', '.join(all_apps)}")

    for app in all_apps:
        app_f_dir = os.path.join(data_f_dir, app)
        app_p_dir = os.path.join(data_p_dir, app)
        
        csv_files = []
        
        # Collect CSV files from data_f/<app>
        if os.path.isdir(app_f_dir):
            files_f = [f for f in os.listdir(app_f_dir) if f.endswith('.csv')]
            csv_files.extend([os.path.join(app_f_dir, f) for f in files_f])
            print(f"  {app}: Found {len(files_f)} CSV files in '{data_f_dir}/{app}'.")
        else:
            print(f"  {app}: No directory found in '{data_f_dir}'.")
        
        # Collect CSV files from data_p/<app>
        if os.path.isdir(app_p_dir):
            files_p = [f for f in os.listdir(app_p_dir) if f.endswith('.csv')]
            csv_files.extend([os.path.join(app_p_dir, f) for f in files_p])
            print(f"  {app}: Found {len(files_p)} CSV files in '{data_p_dir}/{app}'.")
        else:
            print(f"  {app}: No directory found in '{data_p_dir}'.")
        
        # If there are CSV files to merge
        if csv_files:
            df_list = []
            for csv_file in csv_files:
                try:
                    df = pd.read_csv(csv_file)
                    df_list.append(df)
                    print(f"    Successfully read '{csv_file}'.")
                except Exception as e:
                    print(f"    Error reading '{csv_file}': {e}")
            
            if df_list:
                try:
                    merged_df = pd.concat(df_list, ignore_index=True)
                    output_file = os.path.join(data_s_dir, f"{app}_merged.csv")
                    merged_df.to_csv(output_file, index=False)
                    print(f"  {app}: Merged {len(df_list)} files into '{output_file}'.\n")
                except Exception as e:
                    print(f"  {app}: Error writing merged CSV: {e}\n")
            else:
                print(f"  {app}: No data to merge.\n")
        else:
            print(f"  {app}: No CSV files found to merge.\n")

if __name__ == "__main__":
    # Define directory paths
    data_f = "/home/base/code/box/data_f"    # Path to the data_f directory
    data_p = "/home/base/code/box/data_p"    # Path to the data_p directory
    data_s = "/home/base/code/box/data_groundtruth"    # Path to the data_s directory

    print("Starting CSV merge process...")
    merge_csvs(data_f, data_p, data_s)
    print("CSV merge process completed.")
