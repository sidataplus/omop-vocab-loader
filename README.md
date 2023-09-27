# OMOP Vocabulary Loader

Currently supports PostgreSQL only.
This Python script is a modified version of the [LoadVocabFromCsv() from OHDSI/ETL-Synthea](https://github.com/OHDSI/ETL-Synthea/blob/main/R/LoadVocabFromCsv.r) written in R, licensed under the Apache License 2.0.

## What It Does

- Reads environment variables for database connection details and file paths.
- Connects to a database using the provided details.
- Processes specified CSV files in chunks, efficiently handling large files.
- Loads each CSV file into its respective table in the database.
- Provides feedback on the processing status, including errors.

## Requirements

Ensure you have Python 3.x installed. Then, install the necessary packages:

```{bash}
pip install -r requirements.txt
```

Download your vocabulary CSV files from the [Athena website](http://athena.ohdsi.org/vocabulary/list) and place them in a directory of your choice, e.g., `./vocab`. The script will read the files from this directory.

## Instructions

1. **Set Up Environment Variables**:
   Create a `.env` file in the same directory as the script. This file should have the following structure:

   ```{bash}
   SERVER=your_server
   PORT=your_port
   USERNAME=your_username
   PASSWORD=your_password
   DATABASE=your_database_name
   CDM_SCHEMA=your_schema
   VOCAB_FILE_DIR=path_to_your_csv_files
   ```

   Replace the placeholders with your actual details.

2. **Run the Script**:
   Navigate to the script's directory and execute:

   ```{bash}
   python load_vocab.py
   ```

3. **Monitor the Output**:
   The script provides status updates and error messages in the console. It will notify you of the start time, processed lines, remaining lines, and end time for each file.

## Error Handling

The script is equipped to handle both database-specific errors (using `psycopg2.Error`) and general exceptions. If an error occurs, a descriptive message will be printed to the console, providing details about the file and the nature of the error.
