#!/bin/bash


DB_NAME="race_db"
DB_USER="postgres"
DB_PASSWORD="andrea1"
SQL_FILE="create_tables.sql"

echo "Prekidanje aktivnih sesija za bazu $DB_NAME..."
psql -U "$DB_USER" -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$DB_NAME';"

psql -U "$DB_USER" -c "DROP DATABASE IF EXISTS $DB_NAME;"

psql -U "$DB_USER" -c "CREATE DATABASE $DB_NAME;"

psql -U "$DB_USER" -d "$DB_NAME" -f "$SQL_FILE"

echo "Baza podataka i tabele su uspešno kreirane."
