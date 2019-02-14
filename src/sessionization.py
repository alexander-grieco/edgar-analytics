import csv
import sys
from datetime import datetime, date, time
import collections

headerTitles = ['ip', 'date', 'time']

# Session Object
''' Creates object to store in active session Dictionary
    Contains ip of user, start for the start of the session, last for the current
    last request time in the current session, and the number of docs requested
    in the current session '''
class Session:
    def __init__(self, ip, start):
        self.ip = ip
        self.start = start
        self.last = start
        self.numDocs = 1


    # updates the last request time and number of documents acccessed
    def updateLast(self, time):
        self.last = time
        self.numDocs += 1

    # writes the facts about the session to the output inputFile
    def writeToOutput(self, output):
        output.write('{},{},{},{},{}\n'.format(self.ip, self.start, self.last,
                    int((self.last - self.start).total_seconds()) + 1,self.numDocs))


# Returns the index of all relevant columns as defined by global variable headerTitles
def getHeaderInfo(headerInfo):
    return (headerInfo.index(headerTitles[0]),headerInfo.index(headerTitles[1]),
            headerInfo.index(headerTitles[2]))

# sets the session bounds dict which is used to keep sessions in order by session start time
def setDicts(ip, date, boundsDict, activeDict):
    # if ip is not part of an active session create one and add it to the bounds dict
    # also create a session object and add it to active session dictionary
    if ip not in activeDict:
        activeDict[ip] = Session(ip, date) #creating instance of a Session

        # if date is not in the bounds dict, add it to bounds dict and make
        # value another ordered dictionary (will be used to order ips)
        if date not in boundsDict:
            boundsDict[date] = collections.OrderedDict()

        # initialize last datetime of the session to current time, this will be updated if new
        # records for this ip appear within the inactivity window
        boundsDict[date][ip] = date

    # else ip already exists in an active session, must update session end time to current time
    else:
        boundsDict[activeDict[ip].start][ip] = date #update in bounds dict
        activeDict[ip].updateLast(date) #update in active dict

# returns true if the session is older than the defined inactivity period, false if not
def oldSession(requestDate, time, inact):
    return int((requestDate - time).total_seconds()) > inact

# writes the information from a finished session to the output file
# further deletes entries in activeDict and boundsDict
# if boundsDict[time] entry is empty, will also delete this from boundsDict
def writeFinishedSession(ip, time, boundsDict, activeDict, output):
    activeDict[ip].writeToOutput(output)
    del activeDict[ip]
    del boundsDict[time][ip]
    if not boundsDict[time]:
        del boundsDict[time]


# finds all completed sessions and writes their information to the output file
def findCompletedSessions(requestDate, boundsDict, activeDict, output, inact):
    for time in boundsDict:
        # if current time minus start time is out of session window
        if oldSession(requestDate, time, inact):
            # loop through all the ips in this starting timeslot
            for ip in boundsDict[time]:
                # if the session end time is out of the session window, write to output file
                if oldSession(requestDate, boundsDict[time][ip], inact):
                    writeFinishedSession(ip, time, boundsDict, activeDict, output)
        # if time is not an old session, then no subsequent times will be old (since they are ordered)
        # thus to avoid extra searching, break from loop
        else:
            break

# Once input file has ended, flush the remaining active sessions in order of their start time
def flushActiveSessions(time, boundsDict, activeDict, output):
    # loop over all sessions in boundsDict
    # NOTE: will loop in order of start time due to structure of boundsDict
    for time in boundsDict:
        # loop over each ip in each time bin of boundsDict
        for ip in boundsDict[time]:
            # write sessions to file
            writeFinishedSession(ip, time, boundsDict, activeDict, output)

# loop through each line of input file and write all completed sessions to output file
# when file is complete, flush all records to output file
def getSessionization(inputFile, outputFile, inact):

    # Creates two level ordered dictionary with session start time as the key of the first level and
    # the ip as the key of the second level. The value of the second level will be the last request
    # time in the session. This structure allows quick queries by start time and ip and allows it
    # to flush records in order of start time when the input file is complete
    sessionBoundsDict = collections.OrderedDict()

    # Dictionary of all active sessions. Key is ip and value will be a Session object
    activeSessionDict = dict()

    # initiate current time to minumum possible
    current_time = datetime.min

    # open both input and output file
    with open(inputFile, 'r') as input, open(outputFile, "w+") as output:
        # input file reader
        inputReader = csv.reader(input, delimiter=',')

        # getting the header and finding the indices of the relevant info
        headerInfo = inputReader.next()
        ipIndex, dateIndex, timeIndex = getHeaderInfo(headerInfo)

        # go through each line in input file
        for request in inputReader:
            # extracting only the necessar info: the user ip and the date and time the request occurred
            ip = request[ipIndex]
            requestDate = datetime.strptime(request[dateIndex] + " " + request[timeIndex], '%Y-%m-%d %H:%M:%S')

            # updates/sets the dictionaries
            setDicts(ip, requestDate, sessionBoundsDict, activeSessionDict)

            # if the date has advanced, checks for completed sessions and writes them to output
            if requestDate > current_time:
                current_time = requestDate
                findCompletedSessions(requestDate, sessionBoundsDict, activeSessionDict, output, inact)

        # Once file has no more requests, flushes the remaining sessions to output
        flushActiveSessions(current_time, sessionBoundsDict, activeSessionDict, output)



# main function
if __name__ == '__main__':
    with open(sys.argv[2]) as inactivityFile:
        # get the inactivity period in seconds
        inactivity = int(inactivityFile.read())


    # 'stream' the data and output each session to output file
    getSessionization(sys.argv[1], sys.argv[3], inactivity)
