import aiohttp
import asyncio
from disnake import Colour, Embed, Webhook
from datetime import datetime,timezone
from typing import Dict,List

import dpsReport
import encounterDb as edb
import encounterSet as es
import logUtils

def extrapolateTitle(encounterSet:es.encounterSet) -> str:
    ''' Attempts to create a name given a format of "<common start> <number>" for
        encounters with the common prefix, and combining the number ranges as
        much as possible.

        For example, given the encounter set with names:
        "Wing 1"
        "Wing 3"
        "Wing 4"

        This function will create a string saying:
        "Wing 1, 3-4"

        This is unlikely to do anything useful with no common prefix
        nor numerical numbers.
    '''
    # Get a list of all the ground names
    groupNames = []
    for g in encounterSet.groups:
        if not g.isEmpty():
            groupNames.append(g.groupName)

    # If there is only one name, just return it
    if (len(groupNames) == 1):
        return groupNames[0]

    # Determine common prefix
    longest_pre = ''
    shortest_str = min(groupNames, key=len)
    for i in range(len(shortest_str)):
        if all([x.startswith(shortest_str[:i+1]) for x in groupNames]):
            longest_pre = shortest_str[:i+1]
        else:
            break

    # Get the numerical values left over and sort them ascending
    groupNumericalName = []
    for gn in groupNames:
        groupNumericalName.append(int(gn[len(longest_pre):]))
    groupNumericalName.sort()

    # Build a string with ranges
    numberStr = ''
    runStart = groupNumericalName[0]
    runEnd = groupNumericalName[0]
    for i in groupNumericalName:
        if (i == (runEnd+1)):
            runEnd = i
        elif (i > (runEnd+1)):
            if (runStart == runEnd):
                numberStr += '{:d},'.format(runStart)
            else:
                numberStr += '{:d}-{:d},'.format(runStart, runEnd)
            runStart = i
            runEnd = i

    if (runStart == runEnd):
        numberStr += '{:d},'.format(runStart)
    else:
        numberStr += '{:d}-{:d},'.format(runStart, runEnd)
    numberStr = numberStr.rstrip(',')

    # Add plural if it seems correct
    longest_pre = longest_pre.strip()

    if (len(groupNumericalName) > 1):
        longest_pre += 's'

    # Build the final string
    finalStr = '{:s} {:s}'.format(longest_pre, numberStr)
    return finalStr

def prefetchLogJson(logParser:dpsReport.dpsReport, encounterSet:es.encounterSet):
    ''' Since we need detailed JSONs for the logs to extract the correct data, more than the standard
        metadata would provide, this function prefetches the JSONs from the server and caches them
    '''
    logParser.getJsons(logs=encounterSet.getLogs())

def prepareMessage(logParser:dpsReport.dpsReport, globalConfig:Dict, config:Dict, encounterSet:es.encounterSet, db=None) -> Embed:
    # Only edit the success title if there is no override
    if ((config['useTitleExtrapolate']) and ('overrideSuccessTitle' not in config)):
        successTitle = extrapolateTitle(encounterSet=encounterSet)
    else:
        successTitle = config['defaultSuccess']

    title_str = '{:s} - {:s}'.format(encounterSet.date.strftime('%m/%d'), successTitle)

    # Create the rich embded
    message = Embed(title=title_str,
                    type="rich",
                    colour=Colour.green()
                    )

    # Run through all the encounters and parse them. Each encounter gets a field for successes
    # All failures (if tracked) will get appended at the end
    fail_str = ''
    sessionStartTime = None
    sessionEndTime = None
    for e in encounterSet.groups:

        success_str = ''
        for b in e.encounters.values():
            for s in b.success_logs:
                # Get the start and end times of the log
                # Update the session start and end times as needed
                (logStartTime, logendTime) = logUtils.getStartAndEndTimes(logParser=logParser, log=s)
                if ((sessionStartTime is None) or (logStartTime < sessionStartTime)):
                    sessionStartTime = logStartTime

                if ((sessionEndTime is None) or (logendTime > sessionEndTime)):
                    sessionEndTime = logendTime

                # Check if this is a CM
                cmStr = ''
                if (logUtils.getCm(logParser=logParser, log=s)):
                    cmStr = '__CM__ '

                # Check on Embolded Stacks
                isEmboldened = logUtils.getEmboldened(logParser=logParser, log=s)
                if (isEmboldened > 0):
                    emStr = '{:s} '.format(globalConfig['emboldenedEmote'])
                else:
                    emStr = ''

                # Get Encounter Time
                time = logUtils.logTime.fromLog(logParser=logParser, log=s)

                # Get kill time parameters
                # We floor the log time to the start of the day so it doesn't include the log itself in the query
                # We need to convert from the timestamp (UTC) to local time so that we don't have a weird "session"
                # This then filters all logs from BEFORE this session when searching for a best time
                pbStr = ''
                if (db is not None):
                    # Time shenanigans
                    endTime = datetime.fromtimestamp(s.encounterTime, tz=timezone.utc)
                    localTz = datetime.utcnow().astimezone().tzinfo
                    endTime = endTime.astimezone(tz=localTz)
                    endTime = endTime.replace(hour=0, minute=0, second=0, microsecond=0)

                    # See if this kill time is faster than any before
                    compTime = db.compareTime(compTime=time, boss=b.id, isCm=s.encounter.isCm, endDate=endTime)
                    if (compTime.negative):
                        pbStr = '{}: ({})'.format(globalConfig['pbEmote'], config['compTime'])

                success_str += '{:s} - {:s}{:s} {:s}{:s}\n'.format(str(time), cmStr, s.permalink, emStr, pbStr)

            for f in b.fail_logs:
                # Get the start and end times of the log
                # Update the session start and end times as needed
                (logStartTime, logendTime) = logUtils.getStartAndEndTimes(logParser=logParser, log=f)
                if ((sessionStartTime is None) or (logStartTime < sessionStartTime)):
                    sessionStartTime = logStartTime

                if ((sessionEndTime is None) or (logendTime > sessionEndTime)):
                    sessionEndTime = logendTime

                # Check if this is a CM
                cmStr = ''
                if (logUtils.getCm(logParser=logParser, log=f)):
                    cmStr = '__CM__ '

                # Check on Embolded Stacks
                isEmboldened = logUtils.getEmboldened(logParser=logParser, log=f)
                if (isEmboldened > 0):
                    emStr = '{:s} '.format(globalConfig['emboldenedEmote'])
                else:
                    emStr = ''

                # Get Encounter Time
                time = logUtils.logTime.fromLog(logParser=logParser, log=s)

                # Get fail percentages for this log
                healthLeft = logUtils.getPercentage(logParser=logParser, log=f, allowedIDs=b.ids)

                # Cull pre-steal / quick GG logs
                allAboveThresh = True
                for targetHealth in healthLeft:
                    if (targetHealth < 99.9):
                        allAboveThresh = False
                        break
                if (allAboveThresh):
                    continue

                healthStr = ''
                for hs in ['{:.2f}%'.format(x) for x in healthLeft]:
                    healthStr += hs
                    healthStr += ', '
                healthStr = healthStr.rstrip(' ,')

                fail_str += '{:s} - {:s}{:s} - {:s}({:s})\n'.format(str(time), cmStr, f.permalink, emStr, healthStr)

        # Create the field for the successes
        # Note: We aren't handing if this ever break the max characters (1024) like we do for
        #       failures, but we probably should
        if (success_str !=  ''):
            message.add_field(name=e.groupName, value=success_str, inline=False)

    failureTitle = config['defaultFail']
    if ((fail_str != '') and (len(fail_str) < 1024)):
        message.add_field(name=failureTitle, value=fail_str, inline=False)
    # Maximum field length is 1024 characters, so if we exeed it we will need to break things up
    elif ((fail_str != '')):
        fail_str_arr = []
        cline = ''
        for l in fail_str.splitlines(keepends=True):
            if (len(l) + len(cline) >= 1024):
                fail_str_arr.append(cline)
                cline = l
            else:
                cline += l
        else:
            fail_str_arr.append(cline)

        for (idx,fs) in enumerate(fail_str_arr):
            if (idx == 0):
                message.add_field(name=failureTitle, value=fs, inline=False)
            else:
                message.add_field(name='{:s}, continued'.format(failureTitle), value=fs, inline=False)

    # Calculate Total Session Time
    sessionTotalTime = (sessionEndTime - sessionStartTime)
    print('Session Start Time: {}'.format(sessionStartTime))
    print('Session End Time: {}'.format(sessionEndTime))
    print('Total Time: {}'.format(sessionTotalTime))

    if (config['includeTotalTime']):
        totalTimeStr = 'Total Time: {}'.format(sessionTotalTime)
        message.add_field(name='Total Time', value=totalTimeStr, inline=False)

    # Create Footer
    message.set_footer(text=config['botName'])

    return message

async def sendMessage(config:Dict, message:Embed):
    async with aiohttp.ClientSession() as session:
        # Fetch the webhook object
        webhook = Webhook.from_url(config['webhook'], session=session)

        # Upload to webhook
        await webhook.send(embed=message, username=config['botName'])

def postLogs(logParser:dpsReport.dpsReport, globalConfig:Dict, config:Dict, encounterSet:es.encounterSet, db=None):

    # Prepare the message we will send
    message = prepareMessage(logParser=logParser, globalConfig=globalConfig,
                             config=config, encounterSet=encounterSet, db=db)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(sendMessage(config=config, message=message))
