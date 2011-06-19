#!/usr/bin/env python
# Ian Stokes-Rees, December 2009
# http://abitibi.sbgrid.org/devel/projects/pycondor/pycondor
#
# usage: condorg_log_analyze.py log1 log2 log3 ...
#
# Generates lifeline.png and lifeline.svg
#
# X-axis is hours (since UNIX epoch), Y-axis is job index.  Number at the start
# of a lifeline is the sub-job ID (in condor language, this is the job ID).
# green lines indicate a running process that completes "successfully" (as far
# as OSG is concerned).  dashed lines are queued jobs (blue = initial queue
# period, yellow = re-queued).  red lines are jobs that were running and then
# exempted.  Notice interestingly many jobs never started (gaps in the
# horizontal lines).
#
# $Head$
# $Id$
#
# Dependencies:
#   shex        http://abitibi.sbgrid.org/devel/projects/shex/shex
#   matplotlib  http://matplotlib.sourceforge.net/

import sys
import re
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as     plt
from   matplotlib.lines  import Line2D
from   matplotlib.text   import Text
from   matplotlib.pylab  import *

from   shex              import *

width_in    = 100
height_in   = 100

class GridJobState:
    def __init__(self):
        self.state          = "INIT"
        self.jobid          = "0"
        self.subjobid       = "0"
        self.targethost     = "unknown"
        self.remotehost     = "unknown"
        self.host_submit_ts = "0"
        self.grid_submit_ts = "0"
        self.exec_ts        = "0"
        self.finish_ts      = "0"
        self.error_ts       = "0"
        self.events         = []
        self.fig            = None

    def setstate(self, attr, pat, line):
        result = re.findall(pat, line)
        if result:
            setattr(self, attr, result[0])

    def getevent(self, event, pat, line):
        result = re.search(pat, line)
        if result is not None:
            (mmdd, hhmmss) = result.group(1).split()[0:2]
            t   = strptime("%s/2009 %s" % (mmdd, hhmmss), "%m/%d/%Y %H:%M:%S")
            ts  = mktime(t)
            ts += 3600 * 5 # adjust for UTC to EST
            self.events.append((ts/3600.0, event))

    def genjoblifeline(self, yoffset):
        """ Events:
                local_submit
                grid_submit
                start
                hold
                release
                evicted
                terminated
        """

        start = None
        last  = None
        fmt   = 'k:'
        for e in self.events:
            (ts, event) = e
            if event == 'local_submit':
                text(ts, yoffset, self.subjobid, size=6)
                fmt = 'c:'
            elif event == 'grid_submit':
                text(ts, yoffset, "2", size=6)
                fmt = 'c--'
            elif event == 'start':
                text(ts, yoffset, "3", size=6)
                fmt = 'y--'
            elif event == 'evict':
                text(ts, yoffset, "E", size=6)
                fmt = 'r-'
            elif event == 'hold':
                text(ts, yoffset, "H", size=6)
                fmt = 'r:'
            elif event == 'release':
                text(ts, yoffset, "R", size=6)
                fmt = 'b:'
            elif event == 'terminate':
                text(ts, yoffset, "4", size=6)
                fmt = 'g-'
            else:
                text(ts, yoffset, "?", size=6)
                fmt = 'k:'
                pass
            if start is not None:
                plot([start,ts],[yoffset,yoffset], fmt, linewidth=1)
            start = ts
            last  = event

jobslastevent = {}

sites = {}

class Site:
    def __init__(self, siteid, figureNum):
        self.figureNum = figureNum
        self.nextY = 0
        self.jobids = {}
        SetSubplot(figureNum)
        title(siteid)
        xlabel('Hours')
        ylabel('Jobs')
        #print siteid + ":" + str(figureNum)

    def GetYOffset(self,jobid):
        if not self.jobids.has_key(jobid):
            self.jobids[jobid] = self.nextY
            self.nextY += 1
        return self.jobids[jobid]

    def GetFigureNum(self):
        return self.figureNum
        

nextfignum = 1
def GetSite(siteid):
    global nextfignum
    if not sites.has_key(siteid):
        sites[siteid] = Site(siteid, nextfignum)
        nextfignum += 1
    
    return sites[siteid]

def SetSubplot(figure):
    #if (figure < 1) or (figure > 9):
        #print "error"
        #print figure
        #raise Exception("Figure number out of range")
    print figure
    subplot(4, 4, figure)



def getTime(ts):
    #12/16 12:32:17
    # EventTime = "2011-06-14T02:29:31"
    t = strptime(ts, "\"%Y-%m-%dT%H:%M:%S\"")
    return mktime(t)/3600

jobs = []
def setEvent(event, time, jobid, site = ""):
    global jobs
    ts = getTime(time)
    

    if (event != 'local_submit') and (event != 'start'):
        try:
            lastplot = jobslastevent[jobid]
        except:
            return
        yoffset = lastplot[3]
        figure = lastplot[2]
        if figure == 0:
            return
        SetSubplot(lastplot[2])

    else:
        yoffset = figure = 0

    if event == 'local_submit':
#        text(ts, yoffset, subjobid, size=6)
        fmt = 'c:'
        
    elif event == 'grid_submit':
        fmt = 'y--'
        site_obj = GetSite(site)
        figure = site_obj.GetFigureNum()
        yoffset = site_obj.GetYOffset(jobid)
        #if figure == 0:
        #    print site_obj
        SetSubplot(figure)
        #text(0, yoffset, jobid, size=6)
        text(ts, yoffset, "G", size=6)
    elif event == 'start':
        site_obj = GetSite(site)
        figure = site_obj.GetFigureNum()
        yoffset = site_obj.GetYOffset(jobid)
        SetSubplot(figure)
        text(ts, yoffset, jobid, size=6)
        #SetSubplot(1)
        #text(ts, yoffset, jobid, size=6)
        #text(ts, yoffset, "R", size=6)
        fmt = 'g--'
    elif event == 'evict':
        #text(ts, yoffset, "E", size=6)
        text(ts, yoffset, "E", size=6)
        fmt = 'r-'
    elif event == 'hold':
        text(ts, yoffset, "H", size=6)
        fmt = 'r:'
    elif event == 'release':
        #text(ts, yoffset, "Re", size=6)
        fmt = 'b:'
    elif event == 'terminate':
        #text(ts, yoffset, "D", size=6)
        if (ts - lastplot[0]) > 8.0:
           jobs.append("%s: %lf\n" % (jobid, ts-lastplot[0]))
        text(ts, yoffset, "T", size=6)
           #text(ts, yoffset, jobid, size=6)
        fmt = 'g-'
    else:
        #text(ts, yoffset, "?", size=6)
        fmt = 'k:'
    if (event != 'local_submit') and (event != 'start'):
        #plot([lastplot[0],ts],[lastplot[3],lastplot[3]], lastplot[1], linewidth=1)
        plot([lastplot[0],ts],[lastplot[3],lastplot[3]], fmt, linewidth=1)

    jobslastevent[jobid] = (ts, fmt, figure, yoffset)

def proc_log( log_fp):
    job_event = {}
    
    local_submit = re.compile("\((\d+\.\d+).*\)\s+(.*)\s+Job submitted from host")
    grid_submit = re.compile("\((\d+\.\d+).*\)\s+(.*)\s+Job submitted to Globus") #\s*GridResource: gt2 ([\w|\-|\/|\.]+)")
    grid_site = re.compile("\s+GridResource: gt2 ([\w|\-|\/|\.]+)")
    start = re.compile("\((\d+\.\d+).*\)\s+(.*)\s+Job executing on host")
    hold = re.compile("\((\d+\.\d+).*\)\s+(.*)\s+Job was held")
    release = re.compile("\((\d+\.\d+).*\)\s+(.*)\s+Job was released")
    evict = re.compile("\((\d+\.\d+).*\)\s+(.*)\s+Job was evicted")
    terminate = re.compile("\((\d+\.\d+).*\)\s+(.*)\s+Job terminated")
    log_fh = open(log_fp)
    print "opened %s" % log_fp
    start_index = 0
    done = False
    id = ""
    re_grid = ""
    for line in log_fh:
        if "..." in line:
            # Do stuff, end of event
            if job_event.has_key("MyType"):
               if job_event["MyType"] == "\"ExecuteEvent\"":
                   setEvent('start', job_event["EventTime"], ".".join([job_event["Cluster"], job_event["Proc"]]) , job_event["GLIDEIN_GatekeeperB"]) 
               elif job_event["MyType"] == "\"JobTerminatedEvent\"":
                   setEvent('terminate', job_event["EventTime"], ".".join([job_event["Cluster"], job_event["Proc"]]), job_event["GLIDEIN_GatekeeperB"])
               elif job_event["MyType"] == "\"JobEvictedEvent\"" or job_event["MyType"] == "\"JobReconnectFailedEvent\"":
                   setEvent('evict', job_event["EventTime"], ".".join([job_event["Cluster"], job_event["Proc"]]))


            job_event = {}
            continue
        
        # Else read in the event
        try:
            job_event[line.split('=')[0].strip()] = line.split('=')[1].strip()
        except:
            pass
        continue


        #self.setstate("host_submit_ts", "\(%s\..*\)\s+(.*)\s+Job submitted from host"   % self.jobid, line)
        #self.setstate("grid_submit_ts", "\(%s\..*\)\s+(.*)\s+Job submitted to Globus"   % self.jobid, line)
        #self.setstate("exec_ts",        "\(%s\..*\)\s+(.*)\s+Job executing on host"     % self.jobid, line)
        #self.setstate("finish_ts",      "\(%s\..*\)\s+(.*)\s+Job terminated"            % self.jobid, line)
        #self.setstate("remotehost",     "\(%s\..*\).*Job executing on host:\s+(.*)\s*"  % self.jobid, line)

#        local_submit.search("\(%s\..*\)\s+(.*)\s+Job submitted from host"   % self.jobid, line)
#        self.getevent("grid_submit",    "\(%s\..*\)\s+(.*)\s+Job submitted to Globus"   % self.jobid, line)
#        self.getevent("start",          "\(%s\..*\)\s+(.*)\s+Job executing on host"     % self.jobid, line)
#        self.getevent("hold",           "\(%s\..*\)\s+(.*)\s+Job was held"              % self.jobid, line)
#        self.getevent("release",        "\(%s\..*\)\s+(.*)\s+Job was released"          % self.jobid, line)
#        self.getevent("evict",          "\(%s\..*\)\s+(.*)\s+Job was evicted"           % self.jobid, line)
#        self.getevent("terminate",      "\(%s\..*\)\s+(.*)\s+Job terminated"            % self.jobid, line)
        if local_submit.search(line):
            setEvent("local_submit", local_submit.search(line))
        elif grid_submit.search(line):
#            print "Grid Submit"
            re_grid = grid_submit.search(line)
        elif grid_site.search(line):
            setEvent("grid_submit", re_grid, grid_site.search(line).group(1))
#            setEvent("grid_site", grid_site.search(line))

        elif start.search(line):
            setEvent("start", start.search(line))
        elif hold.search(line):
            setEvent("hold", hold.search(line))
        elif release.search(line):
            setEvent("release", release.search(line))
        elif evict.search(line):
            setEvent("evict", evict.search(line))
        elif terminate.search(line):
            setEvent("terminate", terminate.search(line))

    del log_fh

targethost  = None
jobid       = None

fig = plt.figure(figsize=(width_in,height_in))

ind = 1
entries = []
for arg in sys.argv:
    entries.extend(ls(arg))

for e in entries:
    print e

for log_fp in sys.argv[1:]:
    if not exists(log_fp):
        print i("WARNING: $log_fp not found")
        continue
    done = False
    searchIndex = 0
    fileContents = open(log_fp).read()
    #print fileContents
#    start_job = re.compile(".*\((\d+)\.(\d+).*submitted from")
    proc_log(log_fp)
#    fig.canvas.draw()
#savefig('lifeline.png')
print " ".join(jobs)
#savefig('lifeline.svg')
#savefig('lifeline.eps')
savefig('lifeline.pdf')

sys.exit(0)

