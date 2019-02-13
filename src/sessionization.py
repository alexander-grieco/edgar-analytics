import csv
import sys
import datetime
import collections

headerTitles = ['ip', 'date', 'time', 'cik', 'accession','extention']


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



def getHeaderInfo(headerInfo):
    return (headerInfo.index(headerTitles[0]),headerInfo.index(headerTitles[1]),
            headerInfo.index(headerTitles[2]),headerInfo.index(headerTitles[3]),
            headerInfo.index(headerTitles[4]),headerInfo.index(headerTitles[5]))







def getSessionization(inputFile, outputFile, inact):

    sessionBoundsDict = collections.OrderedDict()
    activeSessionDict = dict()

    with open(inputFile, 'r') as input, open(outputFile, "w") as output:
        inputReader = csv.reader(input, delimiter=',')

        headerInfo = inputReader.next()

        ipIndex, dateIndex, timeIndex, cikIndex, accIndex, extIndex = getHeaderInfo(headerInfo)
        print(timeIndex)


# main function
if __name__ == '__main__':
    with open(sys.argv[2]) as inactivityFile:
        inactivity = int(inactivityFile.read())



    getSessionization(sys.argv[1], sys.argv[3], inactivity)
