import csv
import sys
from datetime import datetime
import collections

headerTitles = ['ip', 'date', 'time']


class Session:
    def __init__(self, ip, start):
        self.ip = ip
        self.start = start
        self.last = start
        self.numDocs = 1

    def updateLast(self, time):
        self.last = time
        self.numDocs += 1

    def writeToOutput(self, output):
        output.write('{},{},{},{},{}\n'.format(self.ip, self.start,
            self.last,(self.last - self.start).TotalSeconds + 1,self.numDocs))


# Returns the index of all relevant columns
def getHeaderInfo(headerInfo):
    return (headerInfo.index(headerTitles[0]),headerInfo.index(headerTitles[1]),
            headerInfo.index(headerTitles[2]))



def getSessionization(inputFile, outputFile, inact):

    # Creates two level ordered dictionary with time as the key of the first level activeSessionDict
    # the ip as the key of the second level. This set up allows quick querying by time and ip
    sessionBoundsDict = collections.OrderedDict()

    # Dictionary of all active sessions
    activeSessionDict = dict()

    with open(inputFile, 'r') as input, open(outputFile, "w") as output:
        # input file reader
        inputReader = csv.reader(input, delimiter=',')

        # getting the header and finding the indices of the relevant info
        headerInfo = inputReader.next()
        ipIndex, dateIndex, timeIndex = getHeaderInfo(headerInfo)


        for request in inputReader:
            ip = request[ipIndex]
            date = datetime.strptime(request[dateIndex] + " " + request[timeIndex], '%Y-%m-%d %H:%M:%S')

            if ip not in activeSessionDict:
                activeSessionDict[ip] = Session(ip, date)

                if date not in sessionBoundsDict:
                    sessionBoundsDict[date] = collections.OrderedDict()

                sessionBoundsDict[date][ip] = date
            else:
                sessionBoundsDict[activeSessionDict[ip].start][ip] = date
                activeSessionDict[ip].updateLast(date)


# main function
if __name__ == '__main__':
    with open(sys.argv[2]) as inactivityFile:
        # get the inactivity period in seconds
        inactivity = int(inactivityFile.read())


    # 'stream' the data and output each session to output file
    getSessionization(sys.argv[1], sys.argv[3], inactivity)
