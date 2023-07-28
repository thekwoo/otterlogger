import argparse
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
import json
import os
import sys
import time
from typing import List

import dpsReport
import encounterDb
import encounterSet as es
import logUtils
import postUtils

def getLogs(logFolderPath:str, startTime:timedelta, logParser:dpsReport.dpsReport, encounterSet:es.encounterSet) -> List[dpsReport.dpsReportObj]:
    # Find all the logs we want to parse
    logsToParse = []

    with os.scandir(logFolderPath) as logFolder:
        # First directory in logs are the boss directories
        for bossDir in logFolder:
            bossDirStat = os.stat(bossDir.path)
            modifiedTS   = bossDirStat.st_mtime

            # Filter out bosses that haven't been done after the cutoff
            if (modifiedTS < startTime.timestamp()):
                continue

            # Filter out bosses that aren't in the encounterSet
            encounterFilterList = encounterSet.getEncounterShortNames()

            try:
                bossSN = dpsReport.dpsReportIds.folderNameToShortName(bossDir.name)
            except:
                print('Folder {:s} did not match known boss'.format(bossDir.name))
                continue

            if (bossSN not in encounterFilterList):
                continue

            print(bossDir.name)
            # Go through the boss folder now and look for logs
            with os.scandir(bossDir.path) as bossFolder:
                for log in bossFolder:
                    logStat = os.stat(log.path)
                    logModifiedTS = logStat.st_mtime
                    logModifiedTime = date.fromtimestamp(logModifiedTS)

                    # Filter out logs that are older than the cutoff
                    if (logModifiedTS < startTime.timestamp()):
                        continue

                    # Filter non-zevtc files in case something else has parsed the logs
                    logName, logExtension = os.path.splitext(log)
                    if logExtension != '.zevtc':
                        continue

                    print('--> {}, {}'.format(logName, logModifiedTime))
                    logsToParse.append(log.path)

    # Parse all the logs through dps.report
    uploadedLogs = logParser.uploadLogs(logsToParse)

    # Grab only the log object out of the response, notifiy if it failed to upload
    parsedLogs = []
    for (logName, obj) in uploadedLogs:
        if obj is not None:
            parsedLogs.append(obj)
        else:
            print('Log {:s} was skipped because it was too short'.format(logName))

    return parsedLogs

def postLogs(configName:str, cutoffTime:float=2, successTitle:str=None, failureTitle:str=None, file:str=None):
    # Open Configuration
    with open('config.json', mode='r') as f:
        config_json = f.read()

    config = json.loads(config_json)

    logFolderPath = config['log_folder']
    dpsReportUserToken = config['dpsReport']['userToken']

    # Set the Global Configuration
    globalConfig = config['globalConfig']

    # Grab the selected configuration
    try:
        configSettings = config['configs'][configName]
    except:
        print('Config {:s} not defined in config file.'.format(configName))
        print('Defined configs:')
        for k in config['configs'].keys():
            print('--> {:s}'.format(k))
        sys.exit()

    # Override the defaults if requested
    if (successTitle is not None):
        configSettings['overrideSuccessTitle'] = True
        configSettings['defaultSuccess'] = successTitle
    if (failureTitle is not None):
        configSettings['defaultFail'] = failureTitle

    includeFailures = configSettings['includeFails']

    # Load / Create the Encounter Database
    if ('encounterDb' in configSettings):
        db = encounterDb.encounterDb(filename=configSettings['encounterDb'])
    else:
        db = None

    # Search back the past X hours
    logCutoff = datetime.now() - timedelta(hours=cutoffTime)
    print('Cutoff time: {}'.format(logCutoff))

    logParser = dpsReport.dpsReport(token=dpsReportUserToken)

    # Load the output format specified by the selected config.
    # This builds the encounterSet that the logs are parsed into and used for final formatting
    selectedEncounterSet = configSettings['encounterSet']
    if (selectedEncounterSet in config['encounterSets']):
        print('Using {:s} encounter set'.format(selectedEncounterSet))
        encounterSet = es.encounterSet.fromFormat(format=config['encounterSets'][selectedEncounterSet])
    else:
        print('Encounter Sets {:s} not defined in config file.'.format(selectedEncounterSet))
        print('Defined Encounter Sets:')
        for k in config['encounterSets'].keys():
            print('--> {:s}'.format(k))
        sys.exit()

    # Upload source determination
    # Either grab the raw files from the session, or upload from the input text file
    if (file is None):
        parsed_logs = getLogs(logFolderPath=logFolderPath, startTime=logCutoff, logParser=logParser, encounterSet=encounterSet)

        for log in parsed_logs:
            print(log.permalink)
    else:
        print('Using input file')
        parsed_logs_input = []

        # Run through the file and separate out all strings that have http
        with open(file) as f:
            for line in f:
                lineElement = line.split()
                for le in lineElement:
                    if 'http' in le:
                        parsed_logs_input.append(le)

        parsed_logs = logUtils.linkToLogObject(logParser, parsed_logs_input)

    if (len(parsed_logs) == 0):
        print('No logs found after criteria applied, bailing early')
        return

    # Import into the db if it exists
    if (db is not None):
        db.importLogs(logs=parsed_logs, parser=logParser)


    #db.replayHistory(postConfig=configSettings, encounterSet=encounterSet)
    #db.replayHistory(postConfig=configSettings, encounterSet=encounterSet,
    #                 startDate=datetime(year=2020, month=10, day=19, tzinfo=datetime.utcnow().astimezone().tzinfo))
    #sys.exit()

    # Sort the logs into the encounters we care about
    encounterSet.fillFromLogs(logs=parsed_logs, includeFailures=includeFailures)

    print(encounterSet)

    # Pre-cache the JSONs to speed up posting
    # This allows us to fetch in bulk rather than one at a time, since we end up needing all of the JSONs anyway
    postUtils.prefetchLogJson(logParser=logParser, encounterSet=encounterSet)

    # Upload to webhook
    postUtils.postLogs(logParser=logParser, globalConfig=globalConfig, config=configSettings, encounterSet=encounterSet, db=db)

# Main Entry Point
if __name__ == '__main__':
    # Build Argument Parser
    parser = argparse.ArgumentParser(description='OtterLogger GW2 ArcDPS Log Uploader')
    parser.add_argument('config', help='The config name to use')
    parser.add_argument('-t', dest='time', type=float, default=3, help="Hours to go back for start of logs. Can be fractional hours.")
    parser.add_argument('--title', help='Custom title of post. Overrides config default')
    parser.add_argument('--fails', help='Custom failure title. Overrides config default')
    parser.add_argument('-f', '--file', help='Use logs from file')

    args = parser.parse_args()

    # Run the parser
    postLogs(configName=args.config, cutoffTime=args.time, successTitle=args.title, failureTitle=args.fails, file=args.file)