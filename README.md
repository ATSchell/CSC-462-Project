# CSC-462-Project
Group project for CSC 462, distributed computing course at UVic

### Architecture

#### The Arbutus Mosaic Processing System
- Initial tiling of input files is sequential

- Processing of tiles NDVI/NDSI is done in parallel

- Since I didn't set up a distributed file system and shared managed drives that can have more than 2 simultaneous connections are large and expensive I used base64 encoding and decoding to store images inside gRPC messages

- Can run on Azure free Ubuntu VMs but takes over 35 minutes to complete with 1 master, 2 workers and 2 input Sentinel-2 files to calculate NDVI/NDSI

- Currently, processing occurs on Arbutus inside Docker containers with 1 master and 2 works but can scale up

- Query coordinates for two corners from EarthDaily mosaic, downloads the 32-bit image then processed NDVI and returns a processed 8-bit PNG

#### The Client Local Web App
- Flask Web Application that can take in and generate local data
- Can display data as overlay on Leaflet map
- Can upload and retrieve data from centralized Azure PostgreSQL database
- Send requests for mosaic processing to central Azure database

#### The Azure PostgreSQL Database
- Data share for all clients
- Accepts requests and responses from the creation of new mosaics

### Running the System


###### The Arbutus Mosaic Processing System
- To run with docker:
  - Copy the ```arbutus_processing/``` folder on Arbutus
  - In the ```master/connection.py``` file add API keys for:
    - Azure Data Lake API key
    - Data Lake folder path
    - Connection string for Azure PostgreSQL Database
    - API key for accessing the Earth Daily mosaics 
  - Make sure the make sure you have Docker installed, and the service is running
  - From the main directory with docker-compose.yml
    
    ```docker-compose up -d```
  - View active docker containers that are running
    
    ```docker ps``` and with ```-a``` flag for stopped containers

  - When running in Docker, files are output to a shared volume located in the /Output/output_merged sub-directory from the main directory

- To run without Docker:
  - Install Python3 and all the modules in ```master/requirements.txt``` and ```worker/requirements.txt``` files for whichever system is running the master and worker systems
  - Run master from master/master.py then start desired number of workers from worker/worker.py
  - When running locally files are saved into master/Output/output_merged directory
  
###### The Local Flask Web App
  - Setup a PostgreSQL database locally and run the ```localdb.sql``` file to create the database table
  - Install Python3 and the modules from requirements.txt, the main requirements are flask, psycopg2 and azure-storage-file-datalake
   - In the ```connection.py``` file add:
      - Azure Data Lake API Key
      - Data Lake folder path
      - Connection string for local PostgreSQl database
      - Connection string for Azure PostgreSQL database
     
###### The Azure PostgreSQL Server
  - Through the Azure portal, pgAdmin or your platform of choice run the ```utils/azure_psql.sql``` file to set up the database

#### Notes:
- IP address can be changed from localhost to run on servers, I used internal 10.X.X.X IP addresses and all my machines were on the same Azure VNet
- worker.py is designed to run continuously requesting tasks, master.py waits until it is ready to assign and send tasks to the worker
- Recovering from failures and stalled or stuck workers needs to be improved 
- Protocol Buffers are compiled with:
  
  ```python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. dist_processing.proto```

