# Copyright 2012 Raj Vishwanathan (rajvish@stoser.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import re
import sys
import getopt
import time
import datetime

		
class taskclass:
	""" A simple class to implement Map and Reduce Task details"""
	def __init__(self):
		self.hname="UNKNOWN HOST"
		self.hdfs_read=0
		self.hdfs_write=0
		self.fs_read=0
		self.fs_write=0
		self.sort_finished=0
		self.shuf_finished=0
		self.stime=0
		self.etime=0
		self.tstatus="UNKNOWN"
		self.shuffle_bytes=0
	def set_ttype(self,m):
		self.type=m
		return
	def set_stime(self,t):
		self.stime=int(t);
		return
	def set_etime(self,t):
		self.etime=int(t);
		return
	def set_hname(self,hname):
		self.hname=hname;
		return
	def set_tstatus(self,s):
		self.tstatus = s;
		return
	def set_sort_finished(self,t):
		self.sort_finished = int(t);
		return;
	def set_shuf_finished(self,t):
		self.shuf_finished = int(t);
		return
	def set_taskid(self,tid):
		self.tid=tid
		return
#
# Map Reduce Framework
#
	def set_input_bytes(self,ibytes):
		self.ibytes=ibytes
		return
	def set_irecords(self,irecs):
		self.irecs=irecs
		return
	def set_obytes(self,obytes):
		self.obytes=obytes
		return
	def set_orecs(self,orecs):
		self.obytes=orecs
		return
	def set_shuffle_bytes(self,sbytes):
		self.shuffle_bytes=sbytes;
		return
#
# File System Counters
#
	def set_hdfs_read(self,hread):
		self.hdfs_read=hread
		return
	def set_hdfs_write(self,hwrite):
		self.hdfs_write=hwrite
		return

	def set_fs_read(self,hread):
		self.fs_read=hread
		return
	def set_fs_write(self,hwrite):
		self.fs_write=hwrite
		return
		
	def get_ttype(self):
		return self.type;
	def get_stime(self):
		return self.stime;
	def get_etime(self):
		return self.etime;
	def get_hname(self):
		return self.hname;
	def get_tstatus(self):
		return self.tstatus;
	def get_sort_finished(self):
		return self.sort_finished;
	def get_shuf_finished(self):
		return self.shuf_finished;
	def get_taskid(self):
		return self.tid;
#
# Map Reduce Framework
#
	def get_input_bytes(self):
		return self.ibytes
	def get_irecords(self):
		return self.irecs
	def get_obytes(self):
		return self.obytes
	def get_obytes(self):
		return self.obytes
	def get_shuffle_bytes(self):
		return self.shuffle_bytes

#
# File System Counters
#
	def get_hdfs_read(self):
		return self.hdfs_read
	def get_hdfs_write(self):
		return self.hdfs_write
	def get_fs_read(self):
		return self.fs_read
	def get_fs_write(self):
		return self.fs_write
	def __repr__(self):
		return repr((self.tid,self.stime,self.etime,self.sort_finished,self.shuf_finished))
			
#
# Parse a string and pick up all 'Name="Value"' pairs.
def parsenamevalue(string):
	pattern=re.compile('(?P<name>[^=]+)="(?P<value>[^"]*)" *')
	result={}
	for n,v in re.findall(pattern,string):
		result[n]=v
	return result

p3 = re.compile('(?P<symbol>\\(.+?\\))(?P<name>\\(.+?\\))(?P<value>\\(.+?\\))')
def set_MRFramework(t,rest):
	for symbol,name,value in re.findall(p3,rest):
		tname=symbol.strip(' \(\)')
		tvalue=value.strip(' \(\)')
 		if tname == "REDUCE_SHUFFLE_BYTES":
			t.set_shuffle_bytes(tvalue)
			
	return
def set_FSCounters(t,rest):
	for symbol,name,value in re.findall(p3,rest):
		tname=symbol.strip(' \(\)')
		tvalue=value.strip(' \(\)')
 		if tname == "HDFS_BYTES_READ":
			t.set_hdfs_read(int(tvalue))
 		if tname == "HDFS_BYTES_WRITTEN":
			t.set_hdfs_write(int(tvalue))
 		if tname == "FILE_BYTES_READ":
			t.set_fs_read(int(tvalue))
 		if tname == "FILE_BYTES_WRITTEN":
			t.set_fs_write(int(tvalue))
	
def set_counters(result,t):
	p1=re.compile('\\{.+?\\}')
	p2 = re.compile('(?P<gen>\\(.+?\\))(?P<str>\\(.+?\\))(?P<rest>\\[.+\\])')
	r1=re.findall(p1,result["COUNTERS"])
	for r in r1:
		for gen,str,rest,in re.findall(p2,r):
			if str == "(Map-Reduce Framework)":
					set_MRFramework(t,rest)
			if str =="(FileSystemCounters)":
					set_FSCounters(t,rest)
			else:
				pass
		

def analyze_task(key,result,t):
	if key == "MapAttempt":
		if result.has_key("TASK_TYPE"):
			t.set_ttype(result["TASK_TYPE"])
		if result.has_key("START_TIME"):
			t.set_stime(int(result["START_TIME"]))
		if result.has_key("FINISH_TIME"):
			t.set_etime(int(result["FINISH_TIME"]))
		if result.has_key("TASK_STATUS"):
			t.set_tstatus(result["TASK_STATUS"])
		if result.has_key("HOSTNAME"):
			H=result["HOSTNAME"].split("/")[-1]
			t.set_hname(H.upper())
		if result.has_key("COUNTERS"):
			set_counters(result,t)
		return
	if key == "ReduceAttempt":
		if result.has_key("TASK_TYPE"):
			t.set_ttype(result["TASK_TYPE"])
		if result.has_key("START_TIME"):
			t.set_stime(int(result["START_TIME"]))
		if result.has_key("FINISH_TIME"):
			t.set_etime(int(result["FINISH_TIME"]))
		if result.has_key("TASK_STATUS"):
			t.set_tstatus(result["TASK_STATUS"])
		if result.has_key("SORT_FINISHED"):
			t.set_sort_finished(int(result["SORT_FINISHED"]))
		if result.has_key("SHUFFLE_FINISHED"):
			t.set_shuf_finished(int(result["SHUFFLE_FINISHED"]))
		if result.has_key("HOSTNAME"):
			H=result["HOSTNAME"].split("/")[-1]
			t.set_hname(H.upper())
		if result.has_key("COUNTERS"):
			set_counters(result,t)
	return 
