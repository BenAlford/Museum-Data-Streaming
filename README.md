# Museum Data Streaming

This repository contains the code and tests for an ETL pipeline, processing real-time Kafka messages and uploading them to a remote database.

## Setup

To set up the environment for the ETL pipeline, follow these steps:

1. Clone and enter the repository:
   ```bash
   git clone https://github.com/BenAlford/Museum-Data-Streaming
   cd Museum-Data-Streaming
   ```
2. Create a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
3. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Set up Kafka and ensure it is running.
5. Set up your PostgreSQL database with the schema file located in the pipeline folder (`schema.sql`)
6. Create a .env file with these values:
   ```
   DB_HOST - the hostname of the database server
   DB_NAME - the name of the database you want to write to
   DB_USERNAME - the username for the database
   DB_PASSWORD - the password for the database
   
   
   BOOTSTRAP_SERVERS - the Kafka bootstrap servers
   SECURITY_PROTOCOL - the security protocol for Kafka (e.g. SASL_SSL)
   SASL_MECHANISM - the SASL mechanism for Kafka (e.g. PLAIN)
   USERNAME - the username for Kafka authentication
   PASSWORD - the password for Kafka authentication
   ```
   An example can be seen in the `.env.example` file located in the root of the repository.
7. Run the ETL pipeline:
   ```bash
   cd pipeline
   python3 etl_pipeline.py
   ```

## AWS Setup (Optional)

This project can optionally be set up to run on AWS. Included in this repository is a Terraform configuration to provision the necessary infrastructure. To deploy the infrastructure, navigate to the `terraform` directory and follow these steps:
1. Navigate to the `terraform` directory:
   ```bash
   cd terraform
   ```
2. Create a terraform.tfvars file with these values:
   ```
   db_username = "your_db_username"
   db_password = "your_db_password"
   ```
3. Add in these required values in `variables.tf`:
   ```
   rds_instance_name = "your_rds_instance_name_here"
   vpc_name = "your_vpc_here"
   public_subnet_1_name = "your_public_subnet_1_here"
   public_subnet_2_name = "your_public_subnet_2_here"
   key_name = "your_key_name_here"
   ```
   Store the key file securely as it will be needed to SSH into the EC2 instance.
4. Initialize and apply the Terraform configuration:
   ```bash
   terraform init
   terraform apply
   ```
   Once initialised, you should see the public IP of the EC2 instance and the hostname of the RDS instance. Save these for later use.
5. SCP parts of this repository into the EC2 instance:
   ```bash
   scp -r -i /path/to/your/key.pem /path/to/your/repository/pipeline ec2-user@<IP>:.
   scp -r -i /path/to/your/key.pem /path/to/your/repository/requirements.txt ec2-user@<IP>:.
   scp -r -i /path/to/your/key.pem /path/to/your/repository/reset_data.bash ec2-user@<IP>:.
   scp -r -i /path/to/your/key.pem /path/to/your/repository/.env ec2-user@<IP>:.
   ```
6. SSH into the EC2 instance:
   ```bash
   ssh -i /path/to/your/key.pem ec2-user@<IP>
   ```
7. Install PostgreSQL:
   ```bash
   sudo dnf install postgresql15
   ```
8. Setup the database, entering the password when prompted:
   ```bash
   psql -h <RDS_HOSTNAME> -U <DB_USERNAME> -d museum -f setup.sql
   ```
9. Install necessary python packages:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
10. Run the ETL pipeline:
   ```bash
   cd pipeline
   nohup python3 etl_pipeline.py &
   ```


## Running the Script

Once the script is running, data will be processed in real-time and written to the specified database.
To stop the script, simply type ```quit``` in the terminal. This will cleanly close all connections to Kafka and the database. Exiting with CTRL+C will not close the connections cleanly so is not recommended.

The script batches messages before writing them to the database to improve performance and reduce the number of database transactions. A batch is sent off to be processed after the maximum batch size is reached or the maximum wait time has elapsed. To alter the batch size, or maximum time before writing, call the script with the appropriate command-line arguments. For Example:
```bash
python3 etl_pipeline.py --max-batch-size 100 --max-wait-time 5
```

This example sets the batch size to 100 messages and the maximum wait time to 5 seconds before writing to the database.
These arguments are not required and have the default values of:
--max-batch-size 50 --max-wait-time 10


## Debug

All debug information is stored in the `etl_pipeline.log` file located in the `pipeline` directory. If you wish to adjust the level of logging, you can call the script with the --log-level argument followed by the desired logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL).
For Example:
```bash
python3 etl_pipeline.py --log-level DEBUG
```

This argument is not required and has the default value of `INFO`.

## Resetting the Database

The database can be reset using the `reset_data.bash` script. This will clear all existing data and reinitialize the database schema.
```bash
bash reset_data.bash
```