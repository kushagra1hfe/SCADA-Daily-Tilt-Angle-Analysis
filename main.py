
import pandas as pd
import os
import re
import logging
from datetime import datetime

# Setup logging
log_file = "tracker_classification_log.txt"
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def setup_logging():
    """Initial log to indicate script start"""
    logging.info("Script started.")

# Function to transform tracker names
def transform_tracker_name(name):
    match = re.search(r'ICR(\d+)-INV(\d+)-SMB(\d+)-NCU(\d+)-MSAT(\d+)-Gr(\d+)-T(\d+)', name)
    if match:
        return f"ICR{match.group(1)}-INV{match.group(2)}-SMB{match.group(3)}-NCU{match.group(4)}-MSAT{match.group(5)}-Gr{match.group(6)}-T{match.group(7)}"
    return name

# Function to classify each tracker
def classify_tracker(values):
    """Classify tracker based on values"""
    values = values.fillna(0).values
    if all(abs(value) == 0 for value in values):
        return 'zeros'
    zero_count = sum(abs(value) == 0 for value in values)
    if zero_count > 0 and zero_count < len(values):
        return 'intermittent'
    non_zero_values = [value for value in values if abs(value) != 0]
    if len(non_zero_values) > 0 and len(set(non_zero_values)) == 1:
        return 'constant'
    return 'proper'

# Main function for executing the process
def main():
    try:
        setup_logging()

        # File paths (use dynamic naming based on date)
        today_str = datetime.now().strftime('%Y_%m_%d')
        csv_file = f'Korat_Tilt_Angel_{today_str}.csv'
        filtered_output_path = f'filtered_data_{today_str}.xlsx'
        final_output_path = f'Final_{today_str}.xlsx'

        # Load CSV file
        df = pd.read_csv(csv_file)
        logging.info(f"Loaded data from {csv_file}")

        # Convert 'DateTime' column to datetime
        df['DateTime'] = pd.to_datetime(df['DateTime'], format='%d-%m-%Y %H:%M')

        # Define the time range for filtering
        start_time = '13:00'
        end_time = '14:15'

        # Filter data based on time range
        df_filtered = df[(df['DateTime'].dt.strftime('%H:%M') >= start_time) &
                         (df['DateTime'].dt.strftime('%H:%M') <= end_time)]

        # Save filtered data to Excel
        df_filtered.to_excel(filtered_output_path, index=False)
        logging.info(f"Filtered data saved to {filtered_output_path}")

        # Transform tracker column names and classify
        trackers = [transform_tracker_name(col) for col in df_filtered.columns if col != 'DateTime']
        df_filtered.columns = ['DateTime'] + trackers

        # Classify each tracker
        tracker_classifications = {col: classify_tracker(df_filtered[col]) for col in trackers}

        # Group trackers by classification
        grouped_trackers = {'zeros': [], 'intermittent': [], 'constant': [], 'proper': []}
        for tracker, cls in tracker_classifications.items():
            grouped_trackers[cls].append(tracker)

        # Create summary DataFrame for classification counts and percentages
        summary_df = pd.DataFrame(index=['Total', 'Percentage'])
        for cls in grouped_trackers:
            count = len(grouped_trackers[cls])
            summary_df[cls] = [count, (count / len(trackers)) * 100]

        # Create DataFrame for listing trackers by group (excluding 'proper')
        categories_to_list = ['zeros', 'intermittent', 'constant']
        max_trackers = max(len(grouped_trackers[cls]) for cls in categories_to_list)
        tracker_rows = pd.DataFrame(index=range(max_trackers))
        for cls in categories_to_list:
            tracker_rows[cls] = pd.Series(grouped_trackers[cls])

        # Combine summary and tracker listing into final DataFrame
        final_df = pd.concat([summary_df, tracker_rows])

        # Add description column for final output
        description_column = ['Total', 'Percentage'] + [''] * max_trackers
        final_df.insert(0, 'Description', description_column)

        # Export final summary to Excel
        final_df.to_excel(final_output_path, index=False)
        logging.info(f"Tracker classification summary saved to {final_output_path}")

    except Exception as e:
        logging.error(f"Error occurred: {e}")
        raise

if __name__ == "__main__":
    main()
