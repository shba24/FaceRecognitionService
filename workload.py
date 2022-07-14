from concurrent.futures import ThreadPoolExecutor
from glob import glob
import multiprocessing
from boto3 import client as boto3_client
import os
from time import perf_counter
from botocore.exceptions import ClientError

input_bucket = "cse546-final-input-bucket"
output_bucket = "cse546-final-output-bucket"
test_cases = "test_cases/"
s3 = boto3_client('s3')

def clear_input_bucket():
	global input_bucket, s3
	list_obj = s3.list_objects_v2(Bucket=input_bucket)
	try:
		for item in list_obj["Contents"]:
			key = item["Key"]
			s3.delete_object(Bucket=input_bucket, Key=key)
	except:
		print("Nothing to clear in input bucket")
	
def clear_output_bucket():
	global output_bucket, s3
	list_obj = s3.list_objects_v2(Bucket=output_bucket)
	try:
		for item in list_obj["Contents"]:
			key = item["Key"]
			s3.delete_object(Bucket=output_bucket, Key=key)
	except:
		print("Nothing to clear in output bucket")

def get_ouput_bucket(folder_path, key):
	global output_bucket, s3
	try:
		waiter = s3.get_waiter('object_exists')
		waiter.wait(Bucket=output_bucket, Key = key,
					WaiterConfig={'Delay': 0.1, 'MaxAttempts': 50})
		os.remove(folder_path + key + ".csv")
		s3.download_file(output_bucket, key, folder_path + key + ".csv")
	except Exception as e:
		print("Output not found.", e)

def upload_to_input_bucket_s3(path, name):
	global input_bucket, s3
	s3.upload_file(path + name, input_bucket, name)

def upload_file_to_s3(data):
	test_dir = data[0]
	filename = data[1]
	print("Uploading to input bucket..  name: " + str(filename)) 
	t1_start = perf_counter()
	upload_to_input_bucket_s3(test_dir, filename)
	t1_stop = perf_counter()
	print("Request Latency:", filename, t1_stop-t1_start)
	t1_start = perf_counter()
	get_ouput_bucket(test_dir, filename.split(".")[0])
	t1_stop = perf_counter()
	print("Output Latency:", filename, t1_stop-t1_start)
	
def upload_files(test_case):	
	global input_bucket
	global output_bucket
	global test_cases
	
	
	# Directory of test case
	test_dir = test_cases + test_case + "/"
	
	# Iterate over each video
	# Upload to S3 input bucket
	file_path_list = []
	for filename in os.listdir(test_dir):
		if filename.endswith(".mp4") or filename.endswith(".MP4"):
			file_path_list.append([test_dir, filename])
	
	with ThreadPoolExecutor(max_workers = 8) as executor:
		executor.map(upload_file_to_s3, file_path_list)

def validate(test_case):
	total = 0
	correct = 0
	with open("mapping", "r") as fp:
		for line in fp.readlines():
			total+=1
			exp_txt = line.split(",")
			exp_major = exp_txt[0].split(":")[1].strip()
			exp_year = exp_txt[1].strip()
			filename = test_cases + test_case + "/" + exp_txt[0].split(".")[0] + ".csv"
			fout = open(filename, 'r')
			out_txt = fout.read().split(',')
			out_major = out_txt[1].strip()
			out_year = out_txt[2].strip()
			if exp_major==out_major and exp_year==out_year:
				correct+=1
			else:
				print(exp_major, out_major)
				print(exp_year, out_year)
	
	print("Accuracy: ", (correct/float(total))*100)
			
	
def workload_generator():
	
	print("Running Test Case 1")
	clear_input_bucket()
	clear_output_bucket()
	upload_files("test_case_1")

	print("Running Test Case 2")
	clear_input_bucket()
	clear_output_bucket()
	upload_files("test_case_2")
	
workload_generator()
validate("test_case_2")

