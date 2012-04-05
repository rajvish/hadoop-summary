#!/usr/bin/env python

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
import tasks

p1=re.compile('\\{.+?\\}')
p2 = re.compile('(?P<gen>\\(.+?\\))(?P<str>\\(.+?\\))(?P<rest>\\[.+\\])')
p3 = re.compile('(?P<symbol>\\(.+?\\))(?P<name>\\(.+?\\))(?P<value>\\(.+?\\))')

index=["atime","mintime","maxtime","ahdfs_read","ahdfs_write","max_hdfs_read","max_hdfs_write","min_hdfs_read","min_hdfs_write"]
title=["Average Time","Minimum Time","Maximum Time ","Average HDFS Read","Average HDFS Write","MAX HDFS Read","MAx HDFS Write","MIN HDFS Read","MIN HDFS Write"]

jfile= None
ofile= None
verbose=0

##
## Class for the hadoop job
##
class HadoopJobClass:
	""" Class to Store the Job Statistics"""
	def __init__(self):
		self.stime=0;
		self.etime=0
		self.status=""
		self.jobid=""
		self.fin_mapjobs=0
		self.fail_mapjobs=0
		self.fin_redjobs=0
		self.fail_redjobs=0
		self.hdfs_read=0
		self.hdfs_write=0

# All the times are in milliseconds.
	def set_stime(self,t):
		self.stime=int(t)
		return
	def set_etime(self,t):
		self.etime=int(t)
		return
	def set_status(self,status):
		self.status=status
		return

	def set_fin_mapjobs(self,fmapjobs):
		self.fin_mapjobs=fmapjobs
		return
	def set_fail_mapjobs(self,fmapjobs):
		self.fail_mapjobs=fmapjobs
		return

	def set_fin_redjobs(self,fredjobs):
		self.fin_redjobs=fredjobs
		return
	def set_fail_redjobs(self,fredjobs):
		self.fail_redjobs=fredjobs
		return

	def set_jobid(self,jobid):
		if self.jobid !="" and jobid!=self.jobid:
			print "JobiD is already set to", self.jobid, ".New Job iD = jobid"
			return
		self.jobid=jobid
		return
	def set_hdfs_read(self,bread):
			self.hdfs_read=bread;
			return
	def set_hdfs_write(self,bread):
			self.hdfs_write=bread;
			return
	def set_fs_read(self,bread):
			self.fs_read=bread;
			return
	def set_fs_write(self,bread):
			self.fs_write=bread;
			return

# Get methods

	def get_stime(self):
		return self.stime
	def get_etime(self):
		return self.etime
	def get_status(self):
		return self.status
	def get_jobid(self):
		return self.jobid

	def get_fin_mapjobs(self):
		return self.fin_mapjobs
	def get_fail_mapjobs(self):
		return self.fail_mapjobs

	def get_fin_redjobs(self):
		return self.fin_redjobs
	def get_fail_redjobs(self):
		return self.fail_redjobs
	def get_hdfs_read(self):
		return self.hdfs_read
	def get_hdfs_write(self):
		return self.hdfs_write
	def get_fs_read(self):
		return self.fs_read
	def get_fs_write(self):
		return self.fs_write


class taskAverage:
	""" A class to maintain the averages for all classes"""
	def __init__(self):
		self.maptime=0
		self.redtime=0
		self.maxmaptime=0.0
		self.maxredtime=0.0
		self.minmaptime=float(sys.maxint)
		self.minredtime=float(sys.maxint)
		self.nmaps=0
		self.nreds=0
# HDFS MAP Data
		self.hdfs_av_mapread=0.0
		self.hdfs_av_mapwrite=0.0
		self.hdfs_max_mapread=0.0
		self.hdfs_min_mapread=float(sys.maxint)
		self.hdfs_max_mapwrite=0.0
		self.hdfs_min_mapwrite=float(sys.maxint)

# HDFS REDUCE Data
		self.hdfs_av_redread=0.0
		self.hdfs_av_redwrite=0.0
		self.hdfs_max_redread=0.0
		self.hdfs_min_redread=float(sys.maxint)
		self.hdfs_max_redwrite=0.0
		self.hdfs_min_redwrite=float(sys.maxint)
	
	def set_maptime(self,mtime,hread,hwrite,fread,fwrite,taskid):
		self.maptime+=mtime;
		self.nmaps+=1
		if mtime > self.maxmaptime:
 			self.maxmaptime=mtime
		if  mtime < self.minmaptime:
			self.minmaptime= mtime

		self.hdfs_av_mapread +=hread 
		self.hdfs_av_mapwrite+=hwrite
		
		if  hread > self.hdfs_max_mapread :
			self.hdfs_max_mapread=hread
		if hread < self.hdfs_min_mapread:
			self.hdfs_min_mapread=hread

		if  hwrite > self.hdfs_max_mapwrite :
			self.hdfs_max_mapwrite=hwrite
		if hwrite < self.hdfs_min_mapwrite:
			self.hdfs_min_mapwrite=hwrite
		return

	def set_redtime(self,rtime,hread,hwrite,fread,fwrite,taskid):
		self.redtime+=rtime;
		self.nreds+=1
		if rtime > self.maxredtime:
 			self.maxredtime=rtime
		if  rtime < self.minredtime:
			self.minredtime= rtime

		self.hdfs_av_redread +=hread 
		self.hdfs_av_redwrite+=hwrite
		
		if  hread > self.hdfs_max_redread :
			self.hdfs_max_redread=hread
		if hread < self.hdfs_min_redread:
			self.hdfs_min_redread=hread

		if  hwrite > self.hdfs_max_redwrite :
			self.hdfs_max_redwrite=hwrite
		if hwrite < self.hdfs_min_redwrite:
			self.hdfs_min_redwrite=hwrite
		return


	def get_maps(self):
		map={}
		map["atime"]  =  self.maptime/self.nmaps
		map["mintime"] = self.minmaptime
		map["maxtime"] = self.maxmaptime
		map["ahdfs_read" ]=self.hdfs_av_mapread/self.nmaps
		map["ahdfs_write" ]=self.hdfs_av_mapwrite/self.nmaps
		map["max_hdfs_read" ]=self.hdfs_max_mapread
		map["max_hdfs_write" ]=self.hdfs_max_mapwrite
		map["min_hdfs_read" ]=self.hdfs_min_mapread
		map["min_hdfs_write" ]=self.hdfs_min_mapwrite

		return map
		
	def get_reds(self):
		red={}
		red["atime"]  =  self.redtime/self.nreds
		red["mintime"] = self.minredtime
		red["maxtime"] = self.maxredtime
		red["ahdfs_read" ]=self.hdfs_av_redread/self.nreds
		red["ahdfs_write" ]=self.hdfs_av_redwrite/self.nreds
		red["max_hdfs_read" ]=self.hdfs_max_redread
		red["max_hdfs_write" ]=self.hdfs_max_redwrite
		red["min_hdfs_read" ]=self.hdfs_min_redread
		red["min_hdfs_write" ]=self.hdfs_min_redwrite

		return red


def usage(progname):
	print progname, " [-v| --verbose] -j| --job <job history>  [-o | --output  output_file ]"


def parseopts():
	global jfile,ofile,verbose
	try:
		opts,args =getopt.getopt(sys.argv[1:],"vhj:o:",["verbose","help","job=","output="])
	except getopt.GetoptError,err:
		usage(sys.argv[0])
		sys.exit(2)
	if len(opts) == 0:
		usage(sys.argv[0])
		sys.exit(2)
	for o,a in opts:
		if o in ('-h', '--help'):
			usage(sys.argv[0])
			sys.exit(0)
		elif o in ("-j","--job"):
			jfile =a
		elif o in ("-o","--outut"):
			ofile =a
			print "Output log file = ",ofile
		elif o in ('-v',"--verbose"):
			verbose=1
		else:
			assert False,"Incorrect Option"
	
	if jfile == None:
		print "Job Log file necessary for analysis"
		sys.exit(2)

#
# Parse a string and pick up all 'Name="Value"' pairs.
def parsenamevalue(string):
	pattern=re.compile('(?P<name>[^=]+)="(?P<value>[^"]*)" *')
	result={}
	for n,v in re.findall(pattern,string):
		result[n]=v
	return result

def analyze_job(job,jobline):
	result = parsenamevalue(jobline)
	if result.has_key("JOBID"):
		job.set_jobid(result["JOBID"])
	if result.has_key("LAUNCH_TIME"):
		job.set_stime(int(result["LAUNCH_TIME"]))
	if result.has_key("FINISH_TIME"):
		job.set_etime(int(result["FINISH_TIME"]))
	if result.has_key("JOB_STATUS"):
		job.set_status(result["JOB_STATUS"])
	if result.has_key("FINISHED_MAPS"):
		job.set_fin_mapjobs(result["FINISHED_MAPS"])
	if result.has_key("FAILED_MAPS"):
		job.set_fail_mapjobs(result["FAILED_MAPS"])
	if result.has_key("FINISHED_REDUCES"):
		job.set_fin_redjobs(result["FINISHED_REDUCES"])
	if result.has_key("FAILED_REDUCES"):
		job.set_fail_redjobs(result["FAILED_REDUCES"])
	if result.has_key("COUNTERS"):
		r1=re.findall(p1,result["COUNTERS"])
		for r in r1:
			for gen,str,rest,in re.findall(p2,r):
				for symbol,name,value in re.findall(p3,rest):
					if name.strip(' \(\)') == "HDFS_BYTES_READ":
						job.set_hdfs_read(value.strip(' \(\)'))
					if name.strip(' \(\)') == "HDFS_BYTES_WRITTEN":
						job.set_hdfs_write(value.strip(' \(\)'))
					if name.strip(' \(\)') == "FILE_BYTES_READ":
						job.set_fs_read(value.strip(' \(\)'))
					if name.strip(' \(\)') == "FILE_BYTES_WRITTEN":
						job.set_fs_write(value.strip(' \(\)'))
	return 

	
def print_average(tasks,avg):
	for t in tasks:
		if  t.get_ttype() == "MAP" and t.get_tstatus() == "SUCCESS":
			avg.set_maptime(t.get_etime() - t.get_stime(),t.get_hdfs_read(),t.get_hdfs_write(),t.get_fs_read(),t.get_fs_write(),t.get_taskid())
		if  t.get_ttype() == "REDUCE" and t.get_tstatus() == "SUCCESS":
			avg.set_redtime(t.get_etime() - t.get_stime(),t.get_hdfs_read(),t.get_hdfs_write(),t.get_fs_read(),t.get_fs_write(),t.get_taskid())
	return 
			
			
def print_tasks(tasks):
	t1=sorted(task,key=lambda x:x.stime)
	print "Task Type,Status,hostname,Start Time, Sort Finish, Shuffle Finished, End Time, HDFS Bytes read,HDFS bytes written, File Bytes read, File Bytes written,Bytes Shuffled"
	for t in t1:
		if t.get_tstatus() == "SUCCESS":
			print t.get_ttype(),",",t.get_tstatus(),",",t.get_hname(),",",t.get_stime(),",",t.get_sort_finished(),",",t.get_shuf_finished(),",",t.get_etime(),",",t.get_hdfs_read(),",",t.get_hdfs_write(),",",t.get_fs_read(),",",t.get_fs_write(),",",t.get_shuffle_bytes()

def print_fs_stats(tasks):
	mhdfs_read=0
	mhdfs_write=0
	rhdfs_read=0
	rhdfs_write=0
	mfs_read=0
	mfs_write=0
	rfs_read=0
	rfs_write=0
	for t in tasks:
		if t.get_ttype() =="MAP":
			mhdfs_read+=t.get_hdfs_read()
			mhdfs_write+=t.get_hdfs_write()
			mfs_read+=t.get_fs_read()
			mfs_write+=t.get_fs_write()
		else:
			rhdfs_read+=t.get_hdfs_read()
			rhdfs_write+=t.get_hdfs_write()
			rfs_read+=t.get_fs_read()
			rfs_write+=t.get_fs_write()
	return mhdfs_read,mhdfs_write,mfs_read,mfs_write,rhdfs_read,rhdfs_write,rfs_read,rfs_write
		
	
task = []
def parsefile(jfile):
	lno=0
	try:
		jf = open(jfile,'r')
	except:
		print >> sys.stderr, "Unexpected error opening", jfile
		sys.exit(0)

	job=HadoopJobClass()
	for line in jf:
		lno+=1
		tasktype=line.split(" ",1)
		if tasktype[0] == "Job":
			analyze_job(job,tasktype[1])
		elif tasktype[0] == "MapAttempt" or tasktype[0] == "ReduceAttempt":
			result = parsenamevalue(tasktype[1])
			if result.has_key("TASK_ATTEMPT_ID"):
				idx=result["TASK_ATTEMPT_ID"]
				e=0
				for t in task:
					if idx in t.tid:
						e=1
				if e == 0:
					t=tasks.taskclass()
#					task[idx]=t
					task.append(t)
					t.set_taskid(idx)
			tasklist= tasks.analyze_task(tasktype[0],result,t)
		elif tasktype[0] == "Task":
			pass
		else:
			print >> sys.stderr, "Unknown Task", tasktype[0], " line numer =",lno
	
	stime  = job.get_stime()
	etime  = job.get_etime()
	status = job.get_status()

	st=time.localtime(stime/1000)
	et=time.localtime(etime/1000)
	diff=datetime.datetime(et[0],et[1],et[2],et[3],et[4],et[5]) - datetime.datetime(st[0],st[1],st[2],st[3],st[4],st[5])	
	smaps=job.get_fin_mapjobs()
	fmaps=job.get_fail_mapjobs()
	sreds=job.get_fin_redjobs()
	freds=job.get_fail_redjobs()
	mhdfs_read,mhdfs_write,mfs_read,mfs_write,rhdfs_read,rhdfs_write,rfs_read,rfs_write=print_fs_stats(task)
	print "Status =", status

	if status == "SUCCESS":
		print "Log File                        =",jfile
		print "Job ID                          =",job.get_jobid()
		print "Job Status                      =",status
		print "Start Time                      =",datetime.datetime(st[0],st[1],st[2],st[3],st[4],st[5]).strftime("%Y/%m/%d-%H:%M:%S"),"(",stime,")"
		print "End  Time                       =",datetime.datetime(et[0],et[1],et[2],et[3],et[4],et[5]).strftime("%Y/%m/%d-%H:%M:%S"),"(",etime,")"
		print "Time taken                      =",diff
		print "Completed Maps                  =",smaps
		print "Completed Reduces               =",sreds
		print "Failed Maps                     =",fmaps
		print "Failed Reduces                  =",freds
		print "HDFS bytes read(Map,Reduce,Total)    =",mhdfs_read,rhdfs_read,job.get_hdfs_read()
		print "HDFS bytes Written(Map,reduce,total) =",mhdfs_write,rhdfs_write,job.get_hdfs_write()
		print "FS bytes read(map,reduce,total)      =",mfs_read,rfs_read,job.get_fs_read()
		print "FS bytes Written(Map,Reduce,Total)   =",mfs_write,rfs_write,job.get_fs_write()
	if verbose ==1:
		print_tasks(task)

def main():
	parseopts()
	parsefile(jfile)
	return 
if __name__ == "__main__":
	main()
