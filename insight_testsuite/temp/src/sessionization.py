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


# Returns the index of all relevant columns
def getHeaderInfo(headerInfo):
    return (headerInfo.index(headerTitles[0]),headerInfo.index(headerTitles[1]),
            headerInfo.index(headerTitles[2]))


def setDicts(ip, date, boundsDict, activeDict):
    if ip not in activeDict:
        activeDict[ip] = Session(ip, date)

        if date not in boundsDict:
            boundsDict[date] = collections.OrderedDict() #maybe change

        boundsDict[date][ip] = date

    else:

        boundsDict[activeDict[ip].start][ip] = date
        activeDict[ip].updateLast(date)


def oldSession(requestDate, time, inact):
    return int((requestDate - time).total_seconds()) > inact

def writeFinishedSession(ip, time, boundsDict, activeDict, output):
    activeDict[ip].writeToOutput(output)
    del activeDict[ip]
    del boundsDict[time][ip]


def findCompletedSessions(requestDate, boundsDict, activeDict, output, inact):
    for time in boundsDict:
        if oldSession(requestDate, time, inact):
            for ip in boundsDict[time]:
                if oldSession(requestDate, boundsDict[time][ip], inact):
                    writeFinishedSession(ip, time, boundsDict, activeDict, output)
            if not boundsDict[time]:
                del boundsDict[time]


def flushActiveSessions(time, boundsDict, activeDict, output):
    for time in boundsDict:
        for ip in boundsDict[time]:
            writeFinishedSession(ip, time, boundsDict, activeDict, output)


def getSessionization(inputFile, outputFile, inact):

    # Creates two level ordered dictionary with time as the key of the first level and
    # the ip as the key of the second level. This set up allows quick querying by time and ip
    sessionBoundsDict = collections.OrderedDict()

    # Dictionary of all active sessions
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
