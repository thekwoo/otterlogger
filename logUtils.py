from dataclasses import dataclass, field
from datetime import datetime
import sqlite3
from typing import List,Tuple

import dpsReport

@dataclass
class logTime():
    ''' A dataclass that represents the time on a log
    '''
    mins: int
    secs: int
    ms: int
    negative:bool = False

    def __toMs__(self) -> int:
        ''' Returns the total logtime in milliseconds
        '''

        ms = self.ms + (1000 * self.secs) + (60 * 1000 * self.mins)
        return ms

    def __repr__(self):
        ''' Returns the string representation of the log time in MM:SS:mm
            Where:
            M = Minutes
            S = Seconds
            m = milliseconds
        '''

        if (self.negative):
            return '-{:01d}:{:02d}.{:03d}'.format(self.mins, self.secs, self.ms)
        else:
            return '{:01d}:{:02d}.{:03d}'.format(self.mins, self.secs, self.ms)

    def __conform__(self, protocol):
        ''' Returns logtime in milliseconds for SQL queries
        '''

        if protocol is sqlite3.PrepareProtocol:
            msTotal = (self.mins * 60 * 1000) + (self.secs * 1000) + self.ms
            return msTotal

    def __add__(self, other):
        ''' Creates a new logtime object that is the sum of this logtime plus other
        '''

        selfMs = self.__toMs__()
        otherMs = other.__toMs__()
        newMs = selfMs + otherMs

        return logTime.fromMs(ms=newMs)

    def __sub__(self, other):
        ''' Creates a new logtime object that is the difference of this logtime minus other
        '''

        selfMs = self.__toMs__()
        otherMs = other.__toMs__()
        newMs = selfMs - otherMs

        return logTime.fromMs(ms=newMs)

    @classmethod
    def fromLog(cls, logParser:dpsReport, log:dpsReport.dpsReportObj):
        ''' Creates a logTime object from a log.

            Duration within the log is stored in a string within the JSON. This method will parse the string and split
            out the time accordingly. If this step has already been done for the log, just use the parsed time.
        '''

        if (log.encounter.accurateDuration is None):
            # Fetch EI Raw Output if not already done
            if (log.encounter.json is None):
                log.encounter.json = logParser.getJson(id=log.id)

            # Grab the duration string from the JSON
            # There is technically a durationMS in newer logs which would make this easier, but because
            # the class stores the broken down values, we'll stick to the old method
            duration = log.encounter.json['duration']

            # Encounter time is a string, so parse it out of the JSON
            mins = int(duration[:2])
            secs = int(duration[4:6])
            ms   = int(duration[7:-2])

            totalMs = ms + (1000*secs) + (60*1000*mins)

            # Store the result in case we need it later
            log.encounter.accurateDuration = totalMs

            # Return the packed class
            return cls(mins=mins, secs=secs, ms=ms)
        else:
            return logTime.fromMs(log.encounter.accurateDuration)

    @classmethod
    def fromMs(cls, ms:int):
        ''' Createa a logTime object from milliseconds
        '''

        if (ms < 0):
            negative = True
            ms = (-1 * ms)
        else:
            negative = False

        # Find as many whole minutes as possible
        mins = ms // 1000 // 60
        msRemaining = ms - (mins * 1000 * 60)

        # Find as many whole seconds as possible
        secs = msRemaining // 1000
        msRemaining = msRemaining - (secs * 1000)

        # Return the packed class
        return cls(mins=mins, secs=secs, ms=msRemaining, negative=negative)

def getPercentage(logParser:dpsReport, log:dpsReport.dpsReportObj, allowedIDs:List[int]) -> List[float]:
    ''' Returns the remaining health percentage of each target within a log. If there are multiple targets,
        the percentages will be returned in the order they were encountered in
    '''
    # Fetch EI Raw Output if not already done
    if (log.encounter.json is None):
        log.encounter.json = logParser.getJson(id=log.id)

    healthPercentages = []
    for target in log.encounter.json['targets']:
        # Trash Targets are set to negative numbers
        if (target['id'] < 0):
            continue

        # Skip non-boss targets
        if (target['id'] not in allowedIDs):
            continue

        healthPercentages.append(100.0 - target['healthPercentBurned'])

    return healthPercentages

def getEmboldened(logParser:dpsReport, log:dpsReport.dpsReportObj) -> int:
    ''' Returns if this log is Emboldened and by how much. If the log is not Emboldened, it will
        return 0.
    '''
    # Fetch EI Raw Output if not already done
    if (log.encounter.json is None):
        log.encounter.json = logParser.getJson(id=log.id)

    emboldenedStacksMax = 0

    # Search each player and each phase for the max emboldened stacks
    # Technically it can differ per player, and it shouldn't change per phase,
    # but the full fight values seem to differ slighly from the phase values
    for player in log.encounter.json['players']:
        # Skip NPCs
        try:
            if (player['friendlyNPC']):
                continue
        except:
            continue

        for buff in player['buffUptimes']:
            # Filter only for Emboldened
            if (buff['id'] != dpsReport.emboldenedID):
                continue

            for phase in buff['buffData']:
                if (phase['uptime'] > emboldenedStacksMax):
                    emboldenedStacksMax = phase['uptime']

    # Sometimes Emboldened stacks are not quite integers even though they should be
    emboldenedStacksMax = round(emboldenedStacksMax)

    return emboldenedStacksMax

def getStartAndEndTimes(logParser:dpsReport, log:dpsReport.dpsReportObj) -> Tuple[datetime, datetime]:
    ''' Returns a datetime for both the start and end time of the log.
    '''

    # Fetch EI Raw Output if not already done
    if (log.encounter.json is None):
        log.encounter.json = logParser.getJson(id=log.id)

    # Try to fast way first
    try:
        startTimeStr = log.encounter.json['timeStartStd']
        endTimeStr = log.encounter.json['timeEndStd']

        startTime = datetime.fromisoformat(startTimeStr)
        endTime = datetime.fromisoformat(endTimeStr)

    # Older logs don't have the standard field we we need to fix them up a bit
    except:
        startTimeStr = log.encounter.json['timeStart']
        endTimeStr = log.encounter.json['timeEnd']

        startTimeStr += ':00'
        endTimeStr += ':00'

        startTime = datetime.fromisoformat(startTimeStr)
        endTime = datetime.fromisoformat(endTimeStr)

    print('Start: {}, End {}'.format(startTime, endTime))

    return (startTime, endTime)

def linkToLogObject(parser:dpsReport.dpsReport, links:List[str]) -> List[dpsReport.dpsReportObj]:
    ''' Given a list of log links, will return a the parsed objects
    '''
    return parser.getUploadMetaDatas(identifiers=links, isId=False)