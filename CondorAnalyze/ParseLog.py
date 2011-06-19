#!/usr/bin/python

#
# Written by Derek Weitzel
#
#
#


import os, sys
import optparse
import re
from time import *

from graphtool.graphs.basic import *

submissions = {'Submissions': {}, 'Terminations': {}}
def AddSubmission(site, interval, value):
    global submissions
        
    if value > 0:
        if not submissions['Submissions'].has_key(interval):
            submissions['Submissions'][interval] = 1
        else:
            submissions['Submissions'][interval] += 1
        
    else:
        if not submissions['Terminations'].has_key(interval):
            submissions['Terminations'][interval] = 1
        else:
            submissions['Terminations'][interval] += 1

    
    return
        
    if len(submissions[site]) == 0:
        if value > 0:
            submissions[site][str(interval)] = ( 1, 0 )
        else:
            submissions[site][str(interval)] = ( 0, 1 )
    else:
        if submissions[site].has_key(str(interval)):
            (subs, dones) = submissions[site][str(interval)]
            if value > 0:
                subs += 1
            else:
                dones += 1
            submissions[site][str(interval)] = (subs, dones)
        else:
            if value > 0:
                submissions[site][str(interval)] = ( 1, 0 )
            else:
                submissions[site][str(interval)] = ( 0, 1 )
    
    

sites = {}
def ModifySite(site, ts, num):
    global sites
    if not sites.has_key(site):
        sites[site] = []
        
    if len(sites[site]) == 0:
        cur_num = num
    else:
        cur_num = sites[site][len(sites[site])-1][1] + num
    sites[site].append( (ts, cur_num) )
    
    

def SummarizeSites(interval):
    global sites
    sites_return = {}
    
    # For every site
    for key in sites.keys():
        sites_return[key] = []
        cur_int = 1
        cur_tot = 0
        cur_num = 0
        cur_max = 0
        sub_val = 0
        
        # For every event in site
        while len(sites[key]):
            
            # If the first event is inside the interval
            if sites[key][0][0] > (interval * cur_int):
                
                # if there has been no events in this time frame
                if cur_num == 0:
                    
                    # If it is the first event in the list
                    if len(sites_return[key]) == 0:
                        sites_return[key].append( 0.0 )
                    else:
                        # Copy the last number
                        #sys.stderr.write("Value of last: %lf\n%s\n\n" % (sites_return[key][len(sites_return[key])-1], str(sites_return[key])))
                        sites_return[key].append(sites_return[key][len(sites_return[key])-1])
                else:
                    # If there have been events in this time frame
                    sites_return[key].append( cur_max )
                    #sites_return[key].append( float(cur_tot) / float(cur_num) )
                cur_num = 0
                cur_tot = 0
                cur_max = 0
                cur_int += 1
            else:
                # Else if there is an event in the interval
                cur_num += 1
                
                AddSubmission(key, interval*cur_int, sites[key][0][1] - sub_val )
                sub_val = sites[key][0][1]
                
                # Grab the max
                if sites[key][0][1] > cur_max:
                    cur_max = sites[key][0][1]
                cur_tot += sites[key][0][1]
                
                # Delete the first element
                sites[key].pop(0)
                
    # Extend the shorter sites with 0's
    max_len = max(len(sites_return[i]) for i in sites_return.keys())
    for key in sites_return.keys():
        last_value = sites_return[key][len(sites_return[key])-1]
        while len(sites_return[key]) < max_len:
            sites_return[key].append(0)
            #sites_return[key].append(last_value)
           
    new_sites = {} 
    for key in sites_return.keys():
        new_sites[key] = {}
        for i in range(max_len):
            new_sites[key][(i*interval) + min_time] = sites_return[key][i]
        
    #print [len(sites_return[i]) for i in sites_return.keys()]
    return new_sites


class Job:
    """
    This class Represents a job, and all of the events along with it.
   
    Constants:
    LOCAL_SUBMIT
    GRID_SUBMIT
    RUNNING
    STOP
    HOLD
    RELEASE
    EVICT
    """
    LOCAL_SUBMIT = 1
    GRID_SUBMIT = 2
    RUNNING = 3
    STOP = 4
    HOLD = 5
    RELEASE = 6
    EVICT = 7
    def __init__(self, jobid):
        """Initializer
        
        Arguments:
        jobid - Unique string given to this job (usually
                the condor job number)
        
        """
        self.jobid = jobid
        self.events = []
        self.last_site = ""
    
    
    def AddEvent(self, event, time, site=None):
        """
        Add an event to the event list 
        
        Arguments:
        event - One of the Job.* constants.
        time - time of the event, usually in seconds since epoch
        site - A site the attributed to the event (usually just the grid submission)
        
        """
        if site:
            self.last_site = site
        self.events.append( (event, time, self.last_site) )
        global min_time
        if event == Job.RUNNING:
            #if (len(self.events) > 2) and (self.events[len(self.events) - 2][0] != self.GRID_SUBMIT):
            #   ModifySite(self.last_site, time - min_time, -1)
            #   print "running at new site %s" % self.last_site
            ModifySite(self.last_site, time - min_time, 1)
            #print "running at site %s" % self.last_site
        elif (event == Job.EVICT) or (event == Job.STOP):
            ModifySite(self.last_site, time - min_time, -1)
            #print "ending at site %s" % self.last_site
        elif (event == Job.HOLD) and (self.events[len(self.events) - 2][0] == self.RUNNING):
            ModifySite(self.last_site, time - min_time, -1)
            

    
    def GetSite(self):
        """
        Return the last site this job used.
        """
        return self.last_site
    
    def GetEvents(self, event):
        """
        Return the events that match 'event'
        """
        toReturn = []
        for ev in self.events:
            if ev[0] == event:
                toReturn.append(ev)
        
        return toReturn
        
            
    def GetTimeOfLast(self, eventa, eventb=None):
        """ Get the last event of type eventa (or eventa & eventb)
        
        If eventb is specified, it will look for the last
        combination of eventa & eventb in that order.
        
        Arguments:
        eventa - event to search for
        eventb - event to search for.
        
        """
        if eventb == None:
            index = len(self.events) - 1
        else:
            index = len(self.events) - 2
        while index >= 0:
            if eventb == None:
                if self.events[index][0] == eventa:
                    return self.events[index][1]
            else:
                if (self.events[index][0] == eventa) and (self.events[index+1][0] == eventb):
                    return (self.events[index+1][1] - self.events[index][1])
            index -= 1
        return 0
        
    def GetTimeOfFirst(self, event):
        """ Get the time of the first 'event' """
        index = 0
        while index < len(self.events):
            if self.events[index][0] == event:
                return self.events[index][1]
            else:
                index += 1
        return None
        
    def GetTimeBetween(self, eventa, eventb):
        """ Get the sum of the time between sequential occurrences of 'eventa' and 'eventb'
        
        NOTE: sequence eventa & eventb could happen multiple times, this will sum those times.
        
        """
        if len(self.events) == 0:
            return 0
        
        total_time = 0
        index = 0
        while index < (len(self.events) - 1):
            
            if (self.events[index][0] == eventa) and (self.events[index+1][0] == eventb):
                total_time += self.events[index+1][1] - self.events[index][1]
                index += 1
            else:
                index += 1
                
        return total_time
    
    def GetEventOccurances(self, eventa, eventb=None):
        """
        Get then number of occurrences of 'eventa', or the sequence eventa & eventb.
        
        """
        total_events = 0
        if eventb != None:
            return self.GetMultEventOccurances(eventa, eventb)
        
        for event in self.events:
            if event[0] == eventa:
                total_events += 1
        return total_events
    
    def GetMultEventOccurances(self, eventa, eventb):
        """
        Internal function used by GetEventOccurances when multiple events are specified.
        """
        total_events = 0
        index = 0
        while index < len(self.events) - 2:
            if (self.events[index][0] == eventa) and (self.events[index+1][0] == eventb):
                total_events += 1
            index += 1
        return total_events


jobs = {}


def SetEvent(event, time, jobid, site=None):
    """
    Fill out the 'jobs' global dictionary.
    """
    global jobs
    #jobid = event_re.group(1)
    ts = getTime(time)
    if not jobs.has_key(jobid):
        jobs[jobid] = Job(jobid)
    
    if site:
        jobs[jobid].AddEvent(event, ts, site)
    else:
        jobs[jobid].AddEvent(event, ts)
    
    
min_time = 0
max_time = 0

def getTime(ts):
    """
    Get a condor timestamp, and translate to seconds since unix epoch.
    """
    #12/16 12:32:17
    # EventTime = "2011-06-14T02:29:31"
    t = strptime(ts, "\"%Y-%m-%dT%H:%M:%S\"")
    #t = strptime(ts + " 2009", "%m/%d %H:%M:%S %Y")
    cur_time = int(mktime(t))
    global max_time
    global min_time
    if cur_time > max_time:
        max_time = cur_time
    if min_time == 0:
        min_time = cur_time
    return cur_time
    #return mktime(t)/3600


def ParseFile(file):
    """
    Parse the file in string 'file', and fill out global 'jobs' variable.
    """
    local_submit = re.compile("\((\d+\.\d+).*\)\s+(.*)\s+Job submitted from host")
    grid_submit = re.compile("\((\d+\.\d+).*\)\s+(.*)\s+Job submitted to Globus") #\s*GridResource: gt2 ([\w|\-|\/|\.]+)")
    grid_site = re.compile("\s+GridResource: gt2 ([\w|\-|\/|\.]+)")
    start = re.compile("\((\d+\.\d+).*\)\s+(.*)\s+Job executing on host")
    hold = re.compile("\((\d+\.\d+).*\)\s+(.*)\s+Job was held")
    release = re.compile("\((\d+\.\d+).*\)\s+(.*)\s+Job was released")
    evict = re.compile("\((\d+\.\d+).*\)\s+(.*)\s+Job was evicted")
    terminate = re.compile("\((\d+\.\d+).*\)\s+(.*)\s+Job terminated")
    
    f = open(file)
    line_counter = 0
    job_event = {}
    while 1:
        line = f.readline()
        
        if not line:
            break
 
        if "..." in line:
            # Do stuff, end of event
            if job_event.has_key("MyType"):
               if job_event["MyType"] == "\"ExecuteEvent\"": 
                   SetEvent(Job.RUNNING, job_event["EventTime"], ".".join([job_event["Cluster"], job_event["Proc"]]) , job_event["GLIDEIN_GatekeeperB"])
               elif job_event["MyType"] == "\"JobTerminatedEvent\"":
                   SetEvent(Job.STOP, job_event["EventTime"], ".".join([job_event["Cluster"], job_event["Proc"]]), job_event["GLIDEIN_GatekeeperB"])
               elif job_event["MyType"] == "\"JobEvictedEvent\"" or job_event["MyType"] == "\"JobReconnectFailedEvent\"":
                   SetEvent(Job.EVICT, job_event["EventTime"], ".".join([job_event["Cluster"], job_event["Proc"]]))

            
            job_event = {}
            continue
        
        # Else read in the event
        try:
            job_event[line.split('=')[0].strip()] = line.split('=')[1].strip()
        except:
            pass
        continue

        try:
            if local_submit.search(line):
                SetEvent(Job.LOCAL_SUBMIT, local_submit.search(line))
            elif grid_submit.search(line):
#            print "Grid Submit"
                re_grid = grid_submit.search(line)
            elif grid_site.search(line):
                site = grid_site.search(line).group(1)
                try:
                    SetEvent(Job.GRID_SUBMIT, re_grid, site)
                except UnboundLocalError:
                    pass
#               SetEvent("grid_site", grid_site.search(line))
            elif start.search(line):
                SetEvent(Job.RUNNING, start.search(line))
            elif hold.search(line):
                SetEvent(Job.HOLD, hold.search(line))
            elif release.search(line):
                SetEvent(Job.RELEASE, release.search(line))
            elif evict.search(line):
                SetEvent(Job.EVICT, evict.search(line))
            elif terminate.search(line):
                SetEvent(Job.STOP, terminate.search(line))
        except:
            raise Exception("Error while parsing line %i:\n%s\n" % ( line_counter, line ) )

        line_counter += 1
        

def AddOptions(parser):
    parser.add_option('-l', '--latex', help="Output in a latex compatible format", default=False, dest="latex", action="store_true")
    pass

def GetTotalTime(*events):
    total_time = 0
    for key in jobs.keys():
        job = jobs[key]
        for eventa, eventb in events:
            total_time += job.GetTimeBetween(eventa, eventb)
    return int(total_time)

def GetLastTotalTime(*events):
    total_time = 0
    for key in jobs.keys():
        job = jobs[key]
        for eventa, eventb in events:
            try:
                total_time += job.GetTimeOfLast(eventa, eventb)
            except:
                pass
    return int(total_time)

def GetEventOccurances(*events):
    total_events = 0
    for key in jobs.keys():
        job = jobs[key]
        for event in events:
            try:
                total_events += job.GetEventOccurances(event[0], event[1])
            except TypeError:
                total_events += job.GetEventOccurances(event)
                
    return int(total_events)

def GetEvictPlaces():
    places = {}
    for key in jobs.keys():
        job = jobs[key]
        evicts = job.GetEvents(Job.EVICT)
        for evict in evicts:
            if places.has_key(evict[2]):
                places[evict[2]] += 1
            else:
                places[evict[2]] = 1
    return places
        

def GetTotalRemoteQueueTime():
    return GetTotalTime(    (Job.GRID_SUBMIT, Job.RUNNING), \
                            (Job.EVICT, Job.RUNNING), \
                            (Job.GRID_SUBMIT, Job.HOLD), \
                            (Job.EVICT, Job.HOLD) )

    
def GetTotalMatchingTime():
    return GetTotalTime(    (Job.LOCAL_SUBMIT, Job.GRID_SUBMIT), \
                            (Job.LOCAL_SUBMIT, Job.HOLD), \
                            (Job.HOLD, Job.RELEASE), \
                            (Job.RELEASE, Job.HOLD), \
                            (Job.HOLD, Job.GRID_SUBMIT) )
    
def GetTotalQueueTime():
    return GetTotalTime(    (Job.LOCAL_SUBMIT, Job.GRID_SUBMIT), \
                            (Job.GRID_SUBMIT, Job.RUNNING), \
                            (Job.EVICT, Job.RUNNING), \
                            (Job.GRID_SUBMIT, Job.HOLD), \
                            (Job.LOCAL_SUBMIT, Job.HOLD), \
                            (Job.RELEASE, Job.HOLD), \
                            (Job.HOLD, Job.GRID_SUBMIT), \
                            (Job.HOLD, Job.RELEASE), \
                            (Job.LOCAL_SUBMIT, Job.RUNNING) )
    
def GetTotalRunningTime():
    return GetTotalTime(    (Job.RUNNING, Job.EVICT), \
                            (Job.RUNNING, Job.HOLD), \
                            (Job.RUNNING, Job.STOP) )
    
def GetTotalWastedTime():
    return GetTotalTime(    (Job.RUNNING, Job.EVICT) )
                            #(Job.RUNNING, Job.HOLD) )
    

def GetTotalGoodRunningTime():
    return GetLastTotalTime( (Job.RUNNING, Job.STOP) )

def GetTotalPreemptions():
    return GetEventOccurances( (Job.EVICT),  \
                               (Job.RUNNING, Job.HOLD) )
    
    
latex = False

def OutputCols(*cols):
    if latex:
        print cols[0],
        if len(cols) > 1:
            print " & " + cols[1] + " \\\\"
        else:
            print " & \\\\ "
    else:
        for col in cols:
            print col + " ",
        print ""

def main():
    parser = optparse.OptionParser()
    AddOptions(parser)
    (opts, args) = parser.parse_args()
    
    for file in args:
        if not os.path.exists(file):
            print "File %s not found" % file
        else:
            ParseFile(file)

    global latex
    latex = opts.latex
    site_data = SummarizeSites(60*5)
    bsl = BasicStackedLine()
    f=open("sites.png", 'w')
    #sys.stderr.write(str(site_data))
    bsl.run(site_data, f, { 'title': 'Sites Used for Job', 'starttime': min_time, 'endtime': max_time, 'xlabel': 'Time', 'ylabel': 'Running Jobs'})
    #bsl.run(site_data, f, { 'title': 'Sites Used for Job', 'starttime': min_time, 'endtime': max_time, 'xlabel': 'Time', 'ylabel': 'Running Jobs', 'text_size': 12, 'title_size': 18 })
    #bsl.run(site_data, f, { 'title': 'Sites Used for Job', 'starttime': min_time, 'endtime': min_time + 118800, 'xlabel': 'Time', 'ylabel': 'Running Jobs' })
    
    global submissions
    sbg = StackedBarGraph()
    f = open("SubHist.png", 'w')
    sbg.run(submissions, f, {'title': 'Histogram of submissions', 'span': 60*5, 'text_size': 12, 'title_size': 18})
    #print site_data
    #new_site = [site_data[key] for key in site_data.keys()]
    #from condor_running import stacked_graph, pos_only
    #stacked_graph(new_site, color_seq='random', baseline_fn=pos_only, cmap=cm.get_cmap("Accent"), items=site_data.keys())
    #savefig("sites.pdf")
    
    
    
    if latex:
        print "\\small \\begin{table}[h!] \centering"
        print "\\begin{tabular}{l r}"
    
    OutputCols( "Ratios" )
    OutputCols( "Throughput (Avg. number of proceses running)", "%0.2lf" % ((float(GetTotalRunningTime()) / (3600)) /  ((float(max_time - min_time) / 3600.0))))
    OutputCols("Goodput (TotalRunningTime / AppRunningTime)", "%0.2lf" % ((float(GetTotalGoodRunningTime()) / (3600)) / (float(GetTotalRunningTime()) / (3600))))
    OutputCols("X Factor (QueueTime / RunningTime)", "%0.2lf" % ((float(GetTotalQueueTime()) / (3600)) / (float(GetTotalRunningTime()) / (3600))))
    OutputCols("")
    
    OutputCols( "Totals" )
    OutputCols("Workflow Wallclock Time", "%.2lf H" % ((float(max_time - min_time) / 3600.0)))
    OutputCols( "Pre-emptions",  "%i" % (GetTotalPreemptions()))
    OutputCols( "Queue Time", "%0.2lf H" % (float(GetTotalQueueTime()) / (3600)))
    OutputCols( "Aggregate Running Time", "%0.2lf H" % (float(GetTotalRunningTime()) / (3600)))
    OutputCols( "Wasted Time", "%0.2lf H" % (float(GetTotalWastedTime()) / (3600)))
    OutputCols( "Application Running Time", "%0.2lf H" % (float(GetTotalGoodRunningTime()) / (3600)))
    OutputCols( "Job Starts Per Hour", "%0.2lf" % ( float(GetEventOccurances(Job.RUNNING)) / ( (max_time - min_time) / 3600.0)))
    
    OutputCols("")
    OutputCols( "Divided by Number of jobs")
    num_jobs = len(jobs)
    OutputCols( "Remote Queue Time", "%0.2lf M" % (float(GetTotalRemoteQueueTime()) / (60*num_jobs)))
    OutputCols( "Matching Time", "%0.2lf H" % (float(GetTotalMatchingTime()) / (3600*num_jobs)))
    OutputCols( "Queue Time", "%0.2lf H" % (float(GetTotalQueueTime()) / (3600*num_jobs)))
    OutputCols( "Running Time", "%0.2lf H" % (float(GetTotalRunningTime()) / (3600*num_jobs)))
    OutputCols( "Wasted Time", "%0.2lf H" % (float(GetTotalWastedTime()) / (3600*num_jobs)))
    OutputCols( "Running Time", "%0.2lf H" % (float(GetTotalGoodRunningTime()) / (3600*num_jobs)))
    OutputCols( "Job Starts Per Job", "%0.2lf" % ( float(GetEventOccurances(Job.RUNNING)) / (num_jobs)))
    
    OutputCols("")
    OutputCols( "Evictions ---------")
    evicts = GetEvictPlaces()
    for evict in evicts.keys():
        OutputCols( "%s" % evict,  "%i" % (evicts[evict]))
    
    if latex:
        print "\\end{tabular} \\end{table}"
    
    
    
    

if __name__ == "__main__":
    main()


