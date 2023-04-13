# salesforce
## _reporting_server_to_sf.py_
This tool will go to the reporting server and read one or more tables containing daily totals from BridgePay and push that data to the Salesforce endpoint.  If the variable `tables` at the top of the file is set to be a list of table names, the script will iterate through each name, asking the user whether to process each table.  It will process a table then prompt for the next. If this variable is an empty list (or any _Falsey_ value), it will display a list of ALL tables, sorted, prompt the user to give the name of a _single_ table to process.
Two things are required that are more or less external to the Python code.
1. An ODBC driver must be present on the host system
 - https://docs.microsoft.com/en-us/sql/connect/odbc/windows/system-requirements-installation-and-driver-files?view=sql-server-ver16#sql-version-compatibility
2. Either a `.env` file must exist with specific values, OR the host environment variables must be set.  Most likely an `.env` file will be retrieved from someone who has already used this.

### Environment Variables
The Salesforce-specific names are common with the `gateway-importer` Lambda function environment, but because `username` has an existing value in Windows, the reporting server values are renamed.
- odbc_driver: ODBC driver string for your installed version like `"{ODBC Driver 17 for SQL Server}"`
- reporting_server: Server hostname for the database on the reporting server
- reporting_database: The name of the database used for daily BridgePay data
- reporting_user: Database username
- reporting_password: Database password for that user
- salesForceConsumerKey: Also referred to as the client ID in some places
- salesForceConsumerSecret: Also referred to as the client secret in some places
- salesForceLoginUrl: Should be `"https://login.salesforce.com"` for production
- salesForceUerName: Username for Salesforce login
- salesForcePassword: Password for that user
