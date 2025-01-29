import psycopg2
from psycopg2.extras import DictCursor
import logging
from datetime import datetime

# Generate timestamp for logs
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file_path = f'/home/starlord/ETL_PostgreSQL/Subhasis_Tasks/postGreSQL-DataPipeLine-API/LND_to_CORE/log/lnd_to_core_{timestamp}.log'

# Configure logging
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

# Database A (Source) connection details
DB_A_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'dbname': 'hospital',
    'user': 'postgres',
    'password': 'postgres'
}

# Database B (Target) connection details
DB_B_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'dbname': 'core',
    'user': 'postgres',
    'password': 'postgres'
}

TARGET_TABLE = '"FINAL_LAYER".pat_attendance_core'


def fetch_source_data():
    """Fetch data from Database A (Source)."""
    logging.info("Fetching data from Database A (Source).")
    try:
        conn = psycopg2.connect(**DB_A_CONFIG)
        cursor = conn.cursor(cursor_factory=DictCursor)  # Fetch data as dictionaries
        logging.info("Source Connection established.")
        query = """
            SELECT 
                PatientId, 
                AppointmentID, 
                Gender, 
                ScheduledDay, 
                AppointmentDay, 
                Age, 
                Neighbourhood, 
                Scholarship, 
                Hipertension, 
                Diabetes, 
                Alcoholism, 
                Handcap, 
                SMS_received, 
                No_show
            FROM "LANDING_LAYER".appointments;
        """
        cursor.execute(query)
        records = cursor.fetchall()
        cursor.close()
        conn.close()
        return records  # Return fetched rows as dictionaries
    except Exception as e:
        logging.error(f"Error fetching source data: {e}")
        return []


def upsert_scd2(records):
    """Insert or update data in Database B (Target) with SCD Type 2 logic."""
    try:
        conn = psycopg2.connect(**DB_B_CONFIG)
        cursor = conn.cursor()
        logging.info("Target Connection established.")

        for record in records:
            patient_id = record['patientid']
            appointment_id = record['appointmentid']  # Included but not part of CDC checks
            gender = record['gender']
            scheduled_day = record['scheduledday']
            appointment_day = record['appointmentday']
            age = record['age']
            neighbourhood = record['neighbourhood']
            scholarship_status = bool(record['scholarship'])
            hipertension_status = bool(record['hipertension'])
            diabetes_status = bool(record['diabetes'])
            alcoholism_status = bool(record['alcoholism'])
            handcap_status = bool(record['handcap'])
            sms_received_status = bool(record['sms_received'])
            no_show = record['no_show']

            # Check if the patient record already exists and is active
            query_check = f"""
                SELECT * 
                FROM {TARGET_TABLE} 
                WHERE patientid = %s AND IsActive = TRUE;
            """
            cursor.execute(query_check, (patient_id,))
            existing_record = cursor.fetchone()

            if existing_record:
                # Compare the incoming record with the existing one (excluding AppointmentID)
                if (
                    existing_record[2] != gender or
                    existing_record[3] != scheduled_day or
                    existing_record[4] != appointment_day or
                    existing_record[5] != age or
                    existing_record[6] != neighbourhood or
                    existing_record[7] != scholarship_status or
                    existing_record[8] != hipertension_status or
                    existing_record[9] != diabetes_status or
                    existing_record[10] != alcoholism_status or
                    existing_record[11] != handcap_status or
                    existing_record[12] != sms_received_status or
                    existing_record[13] != no_show
                ):
                    # Mark the current record as inactive
                    query_update = f"""
                        UPDATE {TARGET_TABLE}
                        SET IsActive = FALSE, RecordEndDate = NOW(), UPDT_DB_TS = NOW()
                        WHERE PatientId = %s AND IsActive = TRUE;
                    """
                    cursor.execute(query_update, (patient_id,))

                    # Insert the new version of the record
                    query_insert = f"""
                        INSERT INTO {TARGET_TABLE} (
                            PatientId, AppointmentID, Gender, ScheduledDay, AppointmentDay, Age,
                            Neighbourhood, ScholarshipStatus, HipertensionStatus, DiabetesStatus,
                            AlcoholismStatus, HandcapStatus, SMSReceivedStatus, NoShow,
                            RecordStartDate, IsActive, CR_DB_TS, UPDT_DB_TS
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), TRUE, NOW(), NOW());
                    """
                    cursor.execute(query_insert, (
                        patient_id, appointment_id, gender, scheduled_day, appointment_day, age,
                        neighbourhood, scholarship_status, hipertension_status, diabetes_status,
                        alcoholism_status, handcap_status, sms_received_status, no_show
                    ))
            else:
                # Insert a new record as it's not found in the table
                query_insert = f"""
                    INSERT INTO {TARGET_TABLE} (
                        PatientId, AppointmentID, Gender, ScheduledDay, AppointmentDay, Age,
                        Neighbourhood, ScholarshipStatus, HipertensionStatus, DiabetesStatus,
                        AlcoholismStatus, HandcapStatus, SMSReceivedStatus, NoShow,
                        RecordStartDate, IsActive, CR_DB_TS, UPDT_DB_TS
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), TRUE, NOW(), NOW());
                """
                cursor.execute(query_insert, (
                    patient_id, appointment_id, gender, scheduled_day, appointment_day, age,
                    neighbourhood, scholarship_status, hipertension_status, diabetes_status,
                    alcoholism_status, handcap_status, sms_received_status, no_show
                ))

        # Commit the transaction
        conn.commit()
        logging.info("ETL process completed successfully.")
    except Exception as e:
        logging.error(f"Error in ETL process: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# Main ETL process
if __name__ == "__main__":
    logging.info("Starting ETL process from Landing to Core.")
    source_data = fetch_source_data()
    if source_data:
        logging.info("Data is available in the Landing layer. Proceeding with upsert.")
        upsert_scd2(source_data)
    else:
        logging.warning("No data fetched from source.")
