Project Title :
	Apache airflow capstone project 
	
Description :

	-> learned and developed the airflow project for T1-T2 bigdata development plan in apache      
		airflow module 
	-> In this project Explored on creating a dag, subdag, taskgroup , functional dag, 
	   operators(bash ,python, custom, postgres, filesensor) , connection , variable, XCOm(push, pull)
	   ,worked queue and cretaing and accesing the secrets in vault 
	

Prerequisites:

	1.Docker version 
	2.Docker Compose version 
	3.Python version 
	4.VS Code
	
	
Installation :
	
	-> Docker and Docker Compose:
	
		Download the docker-Compose.yaml (curl 'https://airflow.apache.org/docs/apache-airflow/2.9.0/docker-compose.yaml' -o 'docker-compose.yaml')
		Check the airflow.cfg file
		
	-> Airflow with Docker: 
	
		Once install the docker start the docker container using below command 
			
			docker compose up -d
		
			
Information for developers : 
	
	1. Dags (airflow-docker\dags)
		This folder contains python files which provides features like code complition and debugging within the container environment 
		
	2. Config (airflow-docker\config\airflow.cfg)
		This config file contains configuration information and environment variables
		
	3. docker compose file 
		This file contains docker configuration and services details 
	
	4. Access  web UI :
		Airflow : By default, the Airflow web UI is accessible at http://localhost:8080 in your browser.
					Username : airflow 
					password : airflow
				  You can use the web UI to view and trigger DAGs.
				  
		vault : By default, the vault web UI is accessible at http://vault:8200 in your browser.
					Token : ZyrP7NtNw0hbLUqu7N3IlTdO
				You can use the web UI to view and add the secrets.

		flower : By default, the Airflow web UI is accessible at http://localhost:5555 in your browser.
				 You can use the web UI to view running status of the dags

	5. Accessing Database : 
	
		Postgres DB credentials :
			Host : localhost 
			port : 5432
			username : airflow
			password : airflow 

		
	