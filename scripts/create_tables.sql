-- create_tables.sql

CREATE TABLE IF NOT EXISTS DimensionDriver (
    driverId INT PRIMARY KEY,
    name VARCHAR,
    surname VARCHAR,
    dob DATE,
    nationality VARCHAR
);

CREATE TABLE IF NOT EXISTS DimensionConstructor (
    constructorId INT PRIMARY KEY,
    name VARCHAR,
    nationality VARCHAR
);

CREATE TABLE IF NOT EXISTS DimensionRace (
    raceId INT PRIMARY KEY,
    name VARCHAR,
    round INT,
    time TIME,
    date DATE
);

CREATE TABLE IF NOT EXISTS DimensionCircuit (
    circuitId INT PRIMARY KEY,
    name VARCHAR,
    lat FLOAT,
    lng FLOAT,
    alt INT
);

CREATE TABLE IF NOT EXISTS DimensionLocation (
    locationId INT PRIMARY KEY,
    location VARCHAR,
    country VARCHAR
);

CREATE TABLE IF NOT EXISTS DimensionStatus (
    statusId INT PRIMARY KEY,
    status VARCHAR
);

CREATE TABLE IF NOT EXISTS Fact_RaceResults (
    resultId INT PRIMARY KEY,
    raceId INT REFERENCES DimensionRace(raceId),
    driverId INT REFERENCES DimensionDriver(driverId),
    constructorId INT REFERENCES DimensionConstructor(constructorId),
    circuitId INT REFERENCES DimensionCircuit(circuitId),
    locationId INT REFERENCES DimensionLocation(locationId),
    statusId INT REFERENCES DimensionStatus(statusId),
    startPosition INT,
    endPosition VARCHAR,
    rank INT,
    points INT,
    laps INT,
    duration BIGINT,
    fastestLap INT,
    fastestLapTime BIGINT,
    fastestLapSpeed FLOAT,
    averageLapTime FLOAT,
    pitStopDurationTotal BIGINT
);

CREATE TABLE IF NOT EXISTS Fact_DriverStanding (
    driverStandingId INT PRIMARY KEY,
    raceId INT REFERENCES DimensionRace(raceId),
    driverId INT REFERENCES DimensionDriver(driverId),
    points FLOAT,
    position INT,
    wins INT
);

CREATE TABLE IF NOT EXISTS Fact_ConstructorStanding (
    constructorStandingId INT PRIMARY KEY,
    raceId INT REFERENCES DimensionRace(raceId),
    constructorId INT REFERENCES DimensionConstructor(constructorId),
    points FLOAT,
    position INT,
    wins INT
);

CREATE TABLE IF NOT EXISTS DimensionSession (
    sessionId INT PRIMARY KEY,
    raceId INT REFERENCES DimensionRace(raceId),
    sessionType VARCHAR,
    sessionDate DATE,
    sessionTime TIME
);

CREATE TABLE StagingDrivers (
    driverId VARCHAR(255) PRIMARY KEY,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

