import os
import pandas as pd

# Path to input CSV file
INPUT_FILE = "/home/starlord/Subhasis_Tasks/postGreSQL-DataPipeLine-API/Src_to_LND/Archive_Path/KaggleV2-May-2016.csv"  # Replace with the path to your source CSV file

# Output directory for segregated files
OUTPUT_DIR = "/home/starlord/Subhasis_Tasks/postGreSQL-DataPipeLine-API/File segregator"  # Replace with the desired output directory

def segregate_by_days(input_file, output_dir):
    """
    Segregate patient records into Day 1, Day 2, etc., files based on PatientID and AppointmentID.
    """
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Load the source CSV file into a DataFrame, treating PatientId as a string
    df = pd.read_csv(input_file, dtype={"PatientId": str})

    # Sort the data by PatientId and AppointmentID
    df = df.sort_values(by=["PatientId", "AppointmentID"]).reset_index(drop=True)

    # Group data by PatientId
    grouped = df.groupby("PatientId")

    # Create a DataFrame for each "Day N" file
    day_files = {}

    # Iterate over each patient group
    for patient_id, patient_data in grouped:
        # Enumerate records for the patient to segregate by "Day"
        for day, record in enumerate(patient_data.itertuples(index=False), start=1):
            if day not in day_files:
                day_files[day] = []  # Initialize a list for each day
            
            # Append the record to the corresponding Day file
            day_files[day].append(record._asdict())  # Convert namedtuple to dictionary

    # Save each Day file
    for day, records in day_files.items():
        day_df = pd.DataFrame(records)  # Convert to DataFrame
        day_file_path = os.path.join(output_dir, f"Day_{day}.csv")  # Define file path
        day_df.to_csv(day_file_path, index=False, header=True)  # Save CSV
        print(f"Day {day} file saved: {day_file_path}")


def main():
    print("Processing and segregating patient records into Day files...")
    segregate_by_days(INPUT_FILE, OUTPUT_DIR)
    print(f"Segregation complete. Files are saved in: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
