#RedShift Information
redshiftURL = "jdbc:redshift://xxx.xxx.us-east-1.redshift.amazonaws.com:12345/xxx" #ex. jdbc:redshift://clustername.123.us-east-1.redshift.amazonaws.com:5439/databasename
redShiftTempFolder = "s3a://.../.../" #ex. s3a://bucketname/redshiftAccess/
userName = "" #Username
password = "" #Password

#S3A Access Requirements
s3aAccessKey = "" #Key
s3aSecretKey = "" #Secret Key
s3aEndpoint = "s3.amazonaws.com"

#File/Dataset locations
prtlCodes_file = "Datasets/Port_Codes/prtlCodes.json"
stateCodes_file = "Datasets/States/State_Codes.csv"
airportCodes_file = "Datasets/Airport_Code/airport-codes_csv.csv"
globalLandTemp_file = "Datasets/Tempature/GlobalLandTemperaturesByState.csv"
usDemograph_file = "Datasets/US_Demographics/us-cities-demographics.csv"
immig_file = "Datasets/Immigration_Data/*.snappy.parquet"

#Limit the inserts into the Database to 10, True or False
limitDFtoTen = False
