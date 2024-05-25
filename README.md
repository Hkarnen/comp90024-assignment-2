# COMP90024 TEAM-14

This repository contains all the components for Team 14's traffic and air quality correlation analysis project for COMP90024 - Cluster and Cloud Computing. 

## Team Members
- Houston Karnen - 1254942
- Lachlan Hugo - 696147
- Jaden Ling - 1263521
- Klarissa Jutivannadevi - 1571487
- Brian King - 1537997

## Repository Structure
- **frontend**: Contains Jupyter notebook source code which serve as the client part of the application. The notebook is used for simple data analysis and all visualizations.
- **backend**: Contains all back-end source code, including Fission harvester scripts and Elasticsearch RESTful APIs. These components are responsible for data harvesting, pre-processing, fetching, and aggregating processes.
- **test**: Includes all tests for back-end of the application.
- **database**: Stores Elasticsearch type mappings needed to create the indices within our Elasticsearch instance.
- **data**: Stores any static data files relevant to our project. This includes SUDO vehicle-registration data and ABS SA2 shape files.
- **docs**: Contains all project documentation, including the report, Elasticsearch RESTful API documentation, and any other documentation needed.

## Installation
Installation and setup instructions for this project follow the guidelines provided during the workshop sessions. Detailed instructions are available within the workshop documentation. 
### Fission Routes
Refer to the fission specs and README file provided in the [Fission](./backend/fission) folder. Simply run:

`fission specs apply`

### Elasticsearch Mappings
Refer to the database mappings given in the [Database](./database/) folder. 

## Usage Instructions
To use the application:
1. Clone this repository.
2. Connect to the University of Melbourne VPN and create four terminal instances.
3. Run the following commands, one for each instance:
- `ssh -i <path to private key> -L 6443:192.168.10.5:6443 ubuntu@172.26.132.108`
- `kubectl port-forward service/router -n fission 9090:80`
- `kubectl port-forward service/elasticsearch-master -n elastic 9200:9200`
- `kubectl port-forward service/kibana-kibana -n elastic 5601:5601`
4. You should now be connected to the bastion and have port forwarding set up. Create another terminal instance and navigate to this repository.
5. Open the [Jupyter Notebook](./frontend) in a Python editor of your choice, and run the code accordingly to generate visualizations.