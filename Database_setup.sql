CREATE TABLE "LANDING_LAYER".appointments (
    PatientId NUMERIC NOT NULL,
    AppointmentID BIGINT PRIMARY KEY,
    Gender CHAR(1) NOT NULL,
    ScheduledDay TIMESTAMP NOT NULL,
    AppointmentDay TIMESTAMP NOT NULL,
    Age INTEGER NOT NULL CHECK (Age >= 0),
    Neighbourhood VARCHAR(255) NOT NULL,
    Scholarship INTEGER NOT NULL,
    Hipertension INTEGER NOT NULL,
    Diabetes INTEGER NOT NULL,
    Alcoholism INTEGER NOT NULL,
    Handcap INTEGER NOT NULL CHECK (Handcap >= 0),
    SMS_received INTEGER NOT NULL,
    No_show BOOLEAN NOT NULL,
    CR_DB_TS TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UPDT_DB_TS TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

--Target Table defintiton
CREATE TABLE "FINAL_LAYER".pat_attendance_core (
    PatientId NUMERIC NOT NULL,
    AppointmentID BIGINT NOT NULL, -- Natural Key
    Gender CHAR(1) NOT NULL,
    ScheduledDay TIMESTAMP NOT NULL,
    AppointmentDay TIMESTAMP NOT NULL,
    Age INTEGER NOT NULL CHECK (Age >= 0),
    Neighbourhood VARCHAR(255) NOT NULL,
    ScholarshipStatus BOOLEAN NOT NULL,
    HipertensionStatus BOOLEAN NOT NULL,
    DiabetesStatus BOOLEAN NOT NULL,
    AlcoholismStatus BOOLEAN NOT NULL,
    HandcapStatus BOOLEAN NOT NULL  ,
    SMSReceivedStatus BOOLEAN NOT NULL,
    NoShow BOOLEAN NOT NULL,
    RecordStartDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Start date of the record
    RecordEndDate TIMESTAMP, -- End date of the record (NULL for active record)
    IsActive BOOLEAN NOT NULL DEFAULT TRUE, -- Indicates if this is the current active record
    CR_DB_TS TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Created timestamp
    UPDT_DB_TS TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Updated timestamp
    PRIMARY KEY (AppointmentID, RecordStartDate) -- Composite Key for SCD
);

CREATE INDEX idx_patientid ON "FINAL_LAYER".pat_attendance_core (PatientId);
CREATE INDEX idx_isactive ON "FINAL_LAYER".pat_attendance_core (IsActive);
