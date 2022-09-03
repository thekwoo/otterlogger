from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict,List

import dpsReport

'''
Represents a particular encounter during a session. This takes the form of multiple
success and fail attempts, where each can be none. The log objects are stored in
a list for success and failures.
'''
@dataclass
class encounter():
    prettyName:str
    ids:List[int] = field(default_factory=list)
    success_logs:List[dpsReport.dpsReportObj] = field(default_factory=list)
    fail_logs:List[dpsReport.dpsReportObj] = field(default_factory=list)

    '''
    Indicates if this encounter has no logs, success and failure
    '''
    def isEmpty(self) -> bool:
        if ((len(self.success_logs) == 0) and (len(self.fail_logs) == 0)):
            return True
        else:
            return False

    '''
    Clear all the logs in this encounter
    '''
    def clear(self):
        self.success_logs = []
        self.fail_logs = []

    def __str__(self):
        rtnStr = '--> {:s}\n'.format(self.prettyName)

        rtnStr += '----> Success:\n'
        for s in self.success_logs:
            rtnStr += '------> {:s}\n'.format(s.permalink)

        rtnStr += '----> Failures:\n'
        for f in self.fail_logs:
            rtnStr += '------> {:s}\n'.format(f.permalink)

        return rtnStr

'''
Represents a group of encounters during a session. This can be useful to break up a
related group of encounters for ouput processing.
'''
@dataclass
class encounterGroup():
    groupName:str
    encounters:Dict[str, encounter] = field(default_factory=dict)

    '''
    Indicates that none of the encounters in this group have any logs
    '''
    def isEmpty(self) -> bool:
        for e in self.encounters.values():
            if (not e.isEmpty()):
                return False

        return True

    '''
    Clear all the encounters in this group
    '''
    def clear(self):
        for e in self.encounters.values():
            e.clear()

    def __str__(self):
        rtnStr = '{:s}\n'.format(self.groupName)
        for e in self.encounters.values():
            rtnStr += e.__str__()

        return rtnStr

@dataclass
class encounterSet():
    groups:List[encounterGroup] = field(default_factory=list)
    date:datetime = None

    def fillFromLogs(self, logs:List[dpsReport.dpsReportObj], includeFailures:bool=True):
        ''' Fill datastructure with logs from a list. Optionally include/exclude failures. Any
            logs that don't match a boss that are in the encounter set/group will be ignored.
        '''
        for log in logs:
            # Match an encounter to the set
            bossId = log.encounter.bossId
            bossSN = dpsReport.dpsReportIds.idToShortName(bossId)

            # Get date
            logDate = datetime.fromtimestamp(log.encounterTime)

            for group in self.groups:
                if (bossSN in group.encounters):
                    if (log.encounter.success):
                        group.encounters[bossSN].success_logs.append(log)

                        if (self.date is None):
                            self.date = logDate

                        continue
                    elif (includeFailures):
                        group.encounters[bossSN].fail_logs.append(log)

                        if (self.date is None):
                            self.date = logDate

                        continue


    @classmethod
    def fromFormat(cls, format:Dict):
        ''' From a parsed JSON dictionary, reconstruct an encounter set, which is a list of encounterGroups.
        '''

        es = []
        for (group,enc) in format.items():
            encGroup = encounterGroup(groupName=group)

            for boss in enc:
                # Lookup Boss ID from the shortname in the JSON
                bossIds = dpsReport.dpsReportIds.shortNameToIds(shortName=boss)
                prettyName = dpsReport.dpsReportIds.shortNameToPrettyName(shortName=boss)

                # Store the IDs in the encounter
                encGroup.encounters[boss] = encounter(prettyName=prettyName, ids=bossIds)

            es.append(encGroup)

        return cls(groups=es)

    def isEmpty(self) -> bool:
        ''' Indicates that none of the groups in this set have any logs
        '''

        for g in self.groups:
            if (not g.isEmpty()):
                return False

        return True

    def getLogs(self) -> list[dpsReport.dpsReportObj]:
        ''' Gets a list of all logs in the object
        '''

        logList = []
        for e in self.groups:
            for b in e.encounters.values():
                for s in b.success_logs:
                    logList.append(s)

                for f in b.fail_logs:
                    logList.append(f)

        return logList

    def getEncounterShortNames(self) -> List[str]:
        ''' Gets a list of all the encounter short names in the object
        '''

        idList = []
        for e in self.groups:
            idList.extend(e.encounters.keys())

        return idList

    def clear(self):
        ''' Clear all the encounters in this set
        '''

        self.date = None

        for g in self.groups:
            g.clear()

    def __str__(self):
        '''
        '''

        rtnStr = ''

        for g in self.groups:
            rtnStr += g.__str__()

        return rtnStr
