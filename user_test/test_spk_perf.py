#!/usr/bin/env python3

import json, http.client as httplib, urllib.parse as urllib
from ssl import SSLError
import time,sys

import urllib.request as urllib2
import socket
import ssl

# For internal CAs
context = ssl._create_unverified_context()

HOST="localhost:8443"

def test(host: str, path:str, tsname):
	query = f"/{path}/timeseries?"
	params = urllib.urlencode( {
		"name": tsname,
		"timezone": "PST8PDT",
		"pageSize": -1,					# always fetch all results
                "office": "SPK"
	})
	print("Fetching: %s" % host+query+params)
	conn = None
	headers = { 'Accept': "application/json;version=2" }

	try:
		conn = httplib.HTTPSConnection( host, context=context)
		conn.request("GET", query+params, None, headers )
	except SSLError as err:
		print(type(err).__name__ + " : " + str(err))
		print("Falling back to non-SSL")
		# SSL not supported (could be standalone instance)
		conn = httplib.HTTPConnection( host )
		conn.request("GET", query+params, None, headers )

	r1 = conn.getresponse()
	data = r1.read()
	
	if r1.status != 200:
		print("HTTP Error " + str(r1.status) + ": " + str(data))
		return

	data_dict = None

	try:
		data_dict = json.loads(data)
		#print(repr(data_dict));
	except json.JSONDecodeError as err:
		print(str(err))
		print(repr(data))

def test2(host: str, path:str):
	query = f"/{path}/offices/SPK?"
	params = urllib.urlencode( {
		"pageSize": -1,					# always fetch all results
	})
	print("Fetching: %s" % host+query+params)
	conn = None
	headers = { 'Accept': "application/json;version=2" }

	try:
		conn = httplib.HTTPSConnection( host, context=context )
		conn.request("GET", query+params, None, headers )
	except SSLError as err:
		print(type(err).__name__ + " : " + str(err))
		print("Falling back to non-SSL")
		# SSL not supported (could be standalone instance)
		conn = httplib.HTTPConnection( host )
		conn.request("GET", query+params, None, headers )

	r1 = conn.getresponse()
	data = r1.read()
	
	if r1.status != 200:
		print("HTTP Error " + str(r1.status) + ": " + str(data))
		return

	data_dict = None

	try:
		data_dict = json.loads(data)
		print(repr(data_dict));
	except json.JSONDecodeError as err:
		print(str(err))
		print(repr(data))

def runtest(path: str):
	tsnames = [
		"Black Butte-Pool.Elev.Inst.1Hour.0.Calc-val",
		"Black Butte-Pool.Stor.Inst.1Hour.0.Calc-val",
		"Black Butte.Flow-Res Out.Ave.1Hour.1Hour.Calc-val",
		"Black Butte-Outflow.Stage.Inst.1Hour.0.Calc-val",
		"Black Butte-Outflow.Flow.Ave.1Hour.1Hour.Calc-val",
		"Black Butte-South Diversion Canal.Stage.Inst.1Hour.0.Calc-val",
		"Black Butte-South Diversion Canal.Flow.Ave.1Hour.1Hour.Calc-val",
		"Black Butte-South Diversion Canal.Flow-Sontek.Ave.1Hour.1Hour.Calc-val",
		"Black Butte-Wackerman.Flow.Ave.1Hour.1Hour.Calc-val",
		"Black Butte.Flow-Res In.Ave.1Hour.1Hour.Calc-val",
		"Noel Springs.Depth-SWE.Inst.1Hour.0.Calc-val",
		"Noel Springs.Depth-SWE INC.Inst.1Hour.0.Calc-val",
	]
	
	# Do one request untimed to "grease the pipe", and get the connection pool ready
	test(HOST, path, tsnames[0])

	start = time.time();

	for x in range(12):
		test(HOST, path, tsnames[x])

	end = time.time();
	print("Total time: %.3f s\n" % (end - start))

def runtest2(path: str):
	# Do one request untimed to "grease the pipe", and get the connection pool ready
	test2(HOST, path)

	start = time.time();

	for x in range(10):
		test2(HOST, path)

	end = time.time();
	print("Total time: %.3f s\n" % (end - start))

if __name__ == "__main__":
	if len(sys.argv) > 1:
		HOST = sys.argv[0]
	runtest("cwms-data")
	time.sleep(0.5)
#	runtest("spk-data-dev")

	# Note, /offices/ doesn't exhibit this behavior
	# Uncomment the following lines to test that
	#time.sleep(1)
	#runtest2("spk-data")
	#time.sleep(0.5)
	#runtest2("spk-data-dev")

# vim: ts=4 ai sw=4 noexpandtab

