from dataclasses import dataclass, field
from datetime import datetime,timedelta,timezone
import sqlite3
import sys
from typing import Dict,List,Tuple

import dpsReport
import encounterSet as es
import logUtils
import postUtils

@dataclass
class encounterDb():
    filename:str
    db:sqlite3.Connection = field(init=False)

    def __post_init__(self):
        self.db = sqlite3.connect(self.filename)

        # Check if table exists, if not create it
        c = self.db.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS encounters
                    (log text PRIMARY KEY,
                    date integer,
                    boss text,
                    time integer,
                    success bool,
                    cm bool)''')
        self.db.commit()
        c.close()

    def importLogs(self, logs:list[dpsReport.dpsReportObj], parser:dpsReport.dpsReport=None):
        # Create cursor
        cursor = self.db.cursor()

        # Create Parser if needed
        if (parser is None):
            parser = dpsReport.dpsReport()

        # Load logs
        for l in logs:
            # Check that this log doesn't exist already to prevent an integrity error
            cursor.execute('''SELECT log FROM encounters WHERE log=?''',
                            (l.permalink, ))

            if (cursor.fetchone() is not None):
                print('Log: {:s} already in DB'.format(l.permalink))
                continue

            # Grab important log data
            date = l.encounterTime
            boss = l.encounter.boss
            time = logUtils.logTime.fromLog(logParser=parser, log=l)
            success = l.encounter.success
            cm = l.encounter.isCm

            # Insert log into table
            cursor.execute('''INSERT INTO encounters (log, date, boss, time, success, cm)
                              VALUES (?, ?, ?, ?, ?, ?)''',
                              (l.permalink, date, boss, time, success, cm, ))

            self.db.commit()

        cursor.close()

    '''
    Bulk load logs from an input file. This will not populate the metadata, only create
    the entry for it.

    The input format should be simply 1 log per line
    '''
    def loadFromFile(self, inFile:str):
        # Create cursor
        cursor = self.db.cursor()

        # Read file
        with open(inFile, 'r') as f:
            for l in f:
                # One log per line, remove the newline and whitespace
                log = l.strip()

                # Check that this log doesn't exist already to prevent an integrity error
                cursor.execute('''SELECT log FROM encounters WHERE log=?''',
                                (log, ))

                if (cursor.fetchone() is not None):
                    print('Log: {:s} already in DB'.format(log))
                    continue

                # Add log in, but it will have no additional data
                cursor.execute('''INSERT INTO encounters (log) VALUES
                                (?)''',
                                (log, ))

                self.db.commit()

        cursor.close()

    '''
    Searches entries in the database and reparses the log to fill in missing fields
    '''
    def updateFields(self):
        # Parser for log data
        parser = dpsReport.dpsReport()

        # Create cursors
        # We need one for the query and iterating through the results, and one to perform the updates
        q_cursor = self.db.cursor()
        w_cursor = self.db.cursor()

        # Get rows that needs updating
        q_cursor.execute('''SELECT log FROM encounters WHERE
                            coalesce(date,boss,time,success,cm) IS NULL''')
        for r in q_cursor:
            logPath = r[0]

            log = logUtils.linkToLogObject(parser=parser, links=[logPath])[0]

            date = log.encounterTime
            boss = log.encounter.boss
            time = logUtils.logTime.fromLog(logParser=parser, log=log)
            success = log.encounter.success
            cm = log.encounter.isCm

            w_cursor.execute('''UPDATE encounters SET
                                date = ?,
                                boss = ?,
                                time = ?,
                                success = ?,
                                cm = ?
                                WHERE log = ?''',
                                (date, boss, time, success, cm, logPath, ))

            self.db.commit()

        # Close the cursors
        q_cursor.close()
        w_cursor.close()

    def getEarliestDate(self) -> datetime:
        # Database Cursor
        cursor = self.db.cursor()

        cursor.execute('''SELECT date FROM encounters ORDER BY date ASC''')

        firstDate = datetime.fromtimestamp(cursor.fetchone()[0], tz=timezone.utc)

        return firstDate

    def getBestTime(self, boss:int, isCm:bool, startDate:datetime=None, endDate:datetime=None) -> Tuple[str, logUtils.logTime]:
        # Database Cursor
        cursor = self.db.cursor()

        # If there is no start date, we just start at the first log in the database
        if (startDate is None):
            startDate = self.getEarliestDate()

        # If there is no end date, we just end now
        if endDate is None:
            endDate = datetime.now()

        # Convert bossId to bossName
        bossName = dpsReport.bossNames[boss]

        # Find best time so far
        cursor.execute('''SELECT log,time FROM encounters WHERE
                          (date BETWEEN ? AND ?) AND boss = ? AND cm = ? AND success = ?
                          ORDER BY time ASC''',
                          (startDate.timestamp(), endDate.timestamp(), bossName, isCm, True, ))

        result = cursor.fetchone()
        if (result is None):
            return (None, None)
        else:
            (log, duration) = result

        # Convert duration to logtime
        durationLogTime = logUtils.logTime.fromMs(ms=duration)

        return (log, durationLogTime)

    def compareTime(self, compTime:logUtils.logTime, boss:int, isCm:bool, startDate:datetime=None, endDate:datetime=None) -> logUtils.logTime:
        # Get the best time
        (bestLog, bestDuration) = self.getBestTime(boss=boss, isCm=isCm, startDate=startDate, endDate=endDate)

        if ((bestLog is None) or (bestDuration is None)):
            return logUtils.logTime.fromMs(ms=0)

        return (compTime - bestDuration)

    def replayHistory(self, postConfig:Dict, encounterSet:es.encounterSet, startDate:datetime=None, endDate:datetime=None):
        # Log Parser
        logParser = dpsReport.dpsReport()

        # If there is no start date, we just start at the first log in the database
        # We will floor it to the start of the day so that we don't have a weird start time
        # We want to floor the start time to the start of the day, but since getEarliestDate returns
        # UTC time, we will want to do this in the local timezone
        if (startDate is None):
            startDate = self.getEarliestDate()

            # Get the local timezone and reset the startDate to be in that timezone
            localTz = datetime.utcnow().astimezone().tzinfo
            startDate = startDate.astimezone(tz=localTz)

            # Now zero-out the day
            startDate = startDate.replace(hour=0, minute=0, second=0, microsecond=0)


        # If there is no end date, we just end now
        if endDate is None:
            endDate = datetime.now(tz=timezone.utc)

        # Step Size of 1 day
        dateStep = timedelta(days=1)

        # Get a cursor to walk through the database
        cursor = self.db.cursor()

        curStartDate = startDate
        while (curStartDate < endDate):
            # Isolate the search to a single day
            curEndDate = curStartDate + dateStep

            cursor.execute('''SELECT * FROM encounters WHERE date BETWEEN ? AND ? ORDER BY date ASC''',
                            (curStartDate.timestamp(), curEndDate.timestamp(), ))

            # Light-weight
            parsed_logs = []
            for r in cursor:
                (log, date, boss, time, success, cm) = r

                bossId = next(key for key, value in dpsReport.bossNames.items() if value == boss)
                eObj = dpsReport.dpsReportObjEncounter(success=success, accurateDuration=time, isCm=cm, boss=boss, bossId=bossId)
                dObj = dpsReport.dpsReportObj(permalink=log, encounterTime=date, encounter=eObj)
                parsed_logs.append(dObj)

            # Kill this iteration if there was nothing
            if (len(parsed_logs) == 0):
                curStartDate = curEndDate
                continue

            # Reset the encounterSet
            encounterSet.clear()
            encounterSet.fillFromLogs(logs=parsed_logs)

            print('Session: {:s} to {:s}'.format(curStartDate.isoformat(), curEndDate.isoformat()))
            print(encounterSet)

            # Create a post for this set
            postUtils.postLogs(logParser=logParser, config=postConfig, encounterSet=encounterSet, db=self)

            # Effectively steps by the dateStep
            curStartDate = curEndDate

        # Close the cursor
        cursor.close()
