# -*- coding: iso-8859-15 -*-

# import requests

import json

from croniter import croniter

from datetime import datetime

import time

import logging

import sys

import requests

# Build will be in the structure below, each line will be one job

# build types - full, by_table , schema_changes

# On Failure - exit, ignore

# Structure - Cron [“namecube”:{”typeOfBuild”,”onFailure”},”namecubeN”:{”typeOfBuild”,”onFailure”}]

# Example - ***** - [“Sample Ecommerce”:{”full”,”exit”},”Sample Retail”:{“schema_changes”,”ignore”} 
#  NOTE: */N defines the frequency for jobs to run: example: */2 = every 2 minutes

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s',datefmt='%Y-%m-%d %H:%M:%S')

# Define the server to run jobs
def load_config_file(config_file):
	config_data = None
	try:
		with open(config_file) as f:
			config_data = json.load(f)
			return config_data
	except:
		logger.error(f'Unable to load config file: {config_file}')
		return None


def do_request(url, data, method) -> dict:
	'''
		Function will return a dictionary
		of the JSON response from  GET or POST
		request
		
		input: url (api url for request)
			   data (data passed into response)
			   method (GET,POST)

		output: dictionary of json response from request 
	'''

	headers = {'authorization':token}

	if method.lower()=='get':

		response = requests.get(url, headers=headers)

	else:

		response = requests.post(url, headers=headers, json=data)

	if response.status_code in [200,201]:

		return json.loads(response.content)

	else:

		logging.error(f'Failed to get {url} with HTTP status {response.status_code}. Error {response.content}')

		return None



def get_modelid(cube_name):
    '''
    sub function that will return the model id (oid) for a given cube
    '''

    # Define the endpoint to get the oid for a given cube
    url = f'https://{deployment}/api/v2/datamodels/schema?title={cube_name}&fields=oid'

    # make request to endpoint, if the oid is available, return the oid. Else log the error
    try:
        return do_request(url, '', 'get').get('oid')

    except:
        logging.error(f'Failed to get oid for {cube_name}. Please check schema')
        return None


def rebuild_cube(cube_name, build_type='full'):	
	try:
		datamodel_id = get_modelid(cube_name)
		data = {"datamodelId": datamodel_id, "buildType": build_type, "rowLimit": 0}
		do_request(url, data, 'post')
		logging.info(f'Attempting to rebuild {cube_name}...')
	except:
		logging.error(f'Failed to rebuild {cube_name}. Please check schema')

		
def build_cube(cube_name, build_type='full',curr_try=1):
	max_tries = 4
	url = f'https://{deployment}/api/v2/builds'
	
	# call sub function to get the datamodelID needed for the subsquent builds endpoint
	datamodel_id = get_modelid(cube_name)
	if datamodel_id:
		data = {"datamodelId": datamodel_id, "buildType": build_type, "rowLimit": 0}
	# make initial build call
		logging.info(f'Attempting inital build for cube: {cube_name }...')
		try:
			return do_request(url, data, 'post').get('oid')
		except:
			curr_try +=1			
			if curr_try <= max_tries:
				time.sleep(15)
				build_cube(datamodel_id, build_type,curr_try)
			else:
				logging.error(f'Failed to build {cube_name}. Max attempts reached. Please check schema')
				sys.exit(1)
	else:
		logging.error(f'No datamodel found for {cube_name}.')


def get_build_status(oid, cube_name, curr_try=1):
	'''
	Function will return build status

	'''
	max_tries = 4
	url = f'https://{deployment}/api/v2/builds/{oid}'
	try:
		status = do_request(url, '', 'get').get('status')
	except:
		status = None


	if status == 'failed':
		curr_try +=1
		if curr_try <= max_tries:
			logger.info(f'Cube {cube_name} failed.Sleeping....')
			time.sleep(120)
			logger.info(f'Attempting to rebuild cube {cube_name}....')
			rebuild_cube(cube_name, build_type='full')
			time.sleep(20)
			return get_build_status(oid, cube_name, curr_try)

	if status and status != 'building':
		return status
	else:
		curr_try +=1
		if curr_try <= max_tries:
			time.sleep(15)
			return get_build_status(oid,curr_try)
		else:
			return status


def get_token(username,password):
	""""
	Function will return the token needed to make subsequent api request
	input : username
			password
	output: token
	"""
	# 1)  Define the correct endpoint
	
	endpoint = 'api/v1/authentication/login'

	# 2) Define headers
	headers =  {
		 'Content-Type': 'application/x-www-form-urlencoded',
		 'Accept': 'application/json'}

	# 3) define query url
	query_url = f'http://{deployment}/{endpoint}?'
	
	
	# 4) define data/body of request
	data = f'username={username}&password={password}'

	# 5) make post request
	response = requests.post(query_url,headers=headers,data=data).json()

	# 6) grab token if the request was a success
	if response['success']:
		access_token = response['access_token']
		print(f'Bearer {access_token}')
		return f'Bearer {access_token}'

	else:
		return None

# run the build process
def process_builds(config_file):
	global deployment
	global token
	global to_process
	# 1 Get config data
	config_data = load_config_file(config_file)
	# 2 if config file is valid, get setup variables
	if config_data != None:
		deployment = config_data['server']
		token = config_data['token']
		to_process = config_data['jobs']
		# 3 Get oid
		for process in to_process:
			logger.info(f'Cube {process["cube_name"]} is building...')
			oid = build_cube(process["cube_name"],process["type_of_build"])
			# 4 check whether oid returned
			if oid:
				status = get_build_status(oid,process["cube_name"])
			else:
				status = None
			# Log out the status
			if status:
				logger.info(f'Cube {process["cube_name"]} built with status {status}')
			
			if status and status != 'done' and process["onFailure"].lower()=='exit':
				logger.info(f'Process stopped due to cube {process["cube_name"]} failed to build')




