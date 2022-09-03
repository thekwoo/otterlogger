from concurrent.futures import as_completed
from dataclasses import dataclass,field
from http import HTTPStatus
from logging import exception
from typing import Any,List
import requests
from requests_futures.sessions import FuturesSession
from requests.adapters import HTTPAdapter
from urllib3 import Retry

import time

'''
Mapping of boss IDs used in the logs to various other names for arcDPS outputs.

The key for the dictionary is the "Short Name" which is the abbreviation used for the singular encounter.

PrettyName = Displayed name of the encounter

FolderName = The arcDPS folder that holds the associated encounter.
             Note that multiple IDs may be in the same encounter.

IDs        = These are the ARC
'''
targetIdMap = {
    ###### Raids
    #### Wing 1
    'vg': {
        'PrettyName':  "Vale Guardian",
        'FolderNames': ["Vale Guardian"],
        'IDs':         [15438]
    },
    'gors': {
        'PrettyName':  "Gorseval",
        'FolderNames': ["Gorseval the Multifarious"],
        'IDs':         [15429]
    },
    'sab': {
        'PrettyName':  "Sabetha the Saboteur",
        'FolderNames': ["Sabetha the Saboteur"],
        'IDs':         [15375]
    },

    #### Wing 2
    'sloth': {
        'PrettyName':  "Slothasor",
        'FolderNames': ["Slothasor"],
        'IDs':         [16123]
    },
    'trio': {
        'PrettyName':  "Bandit Trio",
        'FolderNames': ["Berg", "Zane", "Narella"],
        'IDs':         [16088, 16137, 16125]
        # 0: Berg
        # 1: Zane
        # 2: Narella
    },
    'matt': {
        'PrettyName':  "Matthias Gabrel",
        'FolderNames': ["Matthias Gabrel"],
        'IDs':         [16115]
    },

    #### Wing 3
    'esc': {
        'PrettyName':  "Escort",
        'FolderNames': ["McLeod the Silent"],
        'IDs':         [16253]
    },
    'kc': {
        'PrettyName':  "Keep Construct",
        'FolderNames': ["Keep Construct"],
        'IDs':         [16235]
    },
    'tc': {
        'PrettyName':  "Twisted Castle",
        'FolderNames': ["Haunting Statue"],
        'IDs':         [16247]
    },
    'xera': {
        'PrettyName':  "Xera",
        'FolderNames': ["Xera"],
        'IDs':         [16246]
    },

    #### Wing 4
    'cairn': {
        'PrettyName':  "Cairn the Indomitable",
        'FolderNames': ["Cairn the Indomitable"],
        'IDs':         [17194]
    },
    'mo': {
        'PrettyName':  "Mursaat Overseer",
        'FolderNames': ["Mursaat Overseer"],
        'IDs':         [17172]
    },
    'sam': {
        'PrettyName':  "Samarog",
        'FolderNames': ["Samarog"],
        'IDs':         [17188]
    },
    'dei': {
        'PrettyName':  "Deimos",
        'FolderNames': ["Deimos"],
        'IDs':         [17154]
    },

    #### Wing 5
    'sh': {
        'PrettyName':  "Soulless Horror",
        'FolderNames': ["Soulless Horror"],
        'IDs':         [19767]
    },
    'rr': {
        'PrettyName': "Desmina Escort",
        'FolderNames': ["Desmina"],
        'IDs':         [19828]
    },
    'bk': {
        'PrettyName':  "Broken King",
        'FolderNames': ["Broken King"],
        'IDs':         [19691]
    },
    'se': {
        'PrettyName':  "Soul Eater",
        'FolderNames': ["Eater of Souls"],
        'IDs':         [19536]
    },
    'eyes': {
        'PrettyName':  "Eyes",
        'FolderNames': ["Eye of Judgment", "Eye of Fate"],
        'IDs':         [19651, 19844]
        # 0: Eye of Judgement
        # 1: Eye of Fate
    },
    'dhuum': {
        'PrettyName':  "Dhuum",
        'FolderNames': ["Dhuum"],
        'IDs':         [19450]
    },

    #### Wing 6
    'ca': {
        'PrettyName':  "Conjured Amalgamate",
        'FolderNames': ["Conjured Amalgamate"],
        'IDs':         [43974]
    },
    'twins': {
        'PrettyName':  "Twin Largos",
        'FolderNames': ["Nikare", "Kenut"],
        'IDs':         [21105, 21089]
    },
    'qadim': {
        'PrettyName':  "Qadim",
        'ShortName':  "qadim",
        'FolderNames': ["Qadim"],
        'IDs':         [20934, 21285, 21073, 21183, 20997]
        # 0: Qadim
        # 1: Hydra
        # 2: Destroyer
        # 3: Patriarch
        # 4: Matriarch
    },

    #### Wing 7
    'adina': {
        'PrettyName':  "Cardinal Adina",
        'FolderNames': ["Cardinal Adina"],
        'IDs':         [22006]
    },
    'sabir': {
        'PrettyName':  "Cardinal Sabir",
        'FolderNames': ["Cardinal Sabir"],
        'IDs':         [21964]
    },
    'qpeer': {
        'PrettyName':  "Qadim the Peerless",
        'FolderNames': ["Qadim the Peerless"],
        'IDs':         [22000]
    },

    ###### Fractals
    #### 98 CM
    'mama': {
        'PrettyName':  "M A M A",
        'FolderNames': ["MAMA"],
        'IDs':         [17021]
    },
    'siax': {
        'PrettyName':  "Siax the Corrupted",
        'FolderNames': ["Nightmare Oratuss"],
        'IDs':         [17028]
    },
    'enso': {
        'PrettyName':  "Ensolyss of the Endless Torment",
        'FolderNames': ["Ensolyss of the Endless Torment"],
        'IDs':         [16948]
    },

    #### 99 CM
    'skor': {
        'PrettyName': "Skorvald the Shattered",
        'FolderNames': ["Skorvald the Shattered"],
        'IDs':         [17632]
    },
    'arriv': {
        'PrettyName':  "Artsariiv",
        'FolderNames': ["Artsariiv"],
        'IDs':         [17949]
    },
    'arkk': {
        'PrettyName':  "Arkk",
        'FolderNames': ["Arkk"],
        'IDs':         [17759]
    },

    #### 100 CM
    'ai': {
        'PrettyName':  "Ai, Keeper of the Peak",
        'FolderNames': ["Sorrowful Spellcaster"],
        'IDs':         [23254, 20497]
    },

    ##### Strikes
    #### Icebrood Saga
    'ice': {
        'PrettyName':  "Icebrood Construct",
        'FolderNames': ["Icebrood Construct"],
        'IDs':         [22154, 22343]
    },
    'falln': {
        'PrettyName':  "The Voice and The Claw",
        'FolderNames': ["Voice of the Fallen", "Claw of the Fallen"],
        'IDs':         [22343, 22481, 22315]
        # 0: Voice of the Fallen
        # 1: Claw of the Fallen
        # 2: Voice and Claw
    },
    'frae': {
        'PrettyName':  "Fraenir of Jormag",
        'FolderNames': ["Fraenir of Jormag"],
        'IDs':         [22492, 22436]
        # 0: First phase
        # 1: Icebrood Construct Phase
    },
    'bone': {
        'PrettyName':  "Boneskinner",
        'FolderNames': ["Boneskinner"],
        'IDs':         [22521]
    },
    'whisp': {
        'PrettyName':  "Whisper of Jormag",
        'FolderNames': ["Whisper of Jormag"],
        'IDs':         [22711]
    },
    'varia': {
        'PrettyName':  "Cold War",
        'FolderNames': ["Varinia Stormsounder"],
        'IDs':         [22836]
    },

    #### End of Dragons
    'trin': {                                # Phase 1
        'PrettyName':  "Captain Mai Trin",
        'FolderNames': ["Captain Mai Trin"],
        'IDs':         [24033, 24768, 25247]
        # 0: Phase 1 - Mai Trin
        # 1: Phase 2 - Scarlet Briar, Normal Mode
        # 2: Phase 2 - Scarlet Briar, Challenge Mode
    },
    'ankka': {
        'PrettyName':  "Ankka",
        'FolderNames': ["Ankka"],
        'IDs':         [23957]
    },
    'li': {
        'PrettyName':  "Minister Li",
        'FolderNames': ["Minister Li"],
        'IDs':         [24485, 24266, 23612, 25259, 24660, 25271, 24261, 25236, 23618, 25242, 24254, 25280]
        # 0:  Minister Li - Normal Mode
        # 1:  Minister Li - Challenge Mode
        # 2:  Sniper - Normal Mode
        # 3:  Sniper - Challenge Mode
        # 4:  Mech Rider - Normal Mode
        # 5:  Mech Rider - Challenge Mode
        # 6:  Enforcer - Normal Mode
        # 7:  Enforcer - Chellenge Mode
        # 8:  Ritualist - Normal Mode
        # 9:  Ritualist - Challenge Mode
        # 10: Mindblade - Normal Mode
        # 11: Mindeblade - Challenge Mode
    },
    'void': {
        'PrettyName':  "Dragon Void",
        'FolderNames': ["The Dragonvoid"],
        'IDs':         [43488]
    },

    # Holiday Missions
    'frezi': {
        'PrettyName': "Freezie",
        'FolderNames': ["Freezie"],
        'IDs':         [21333]
    }
}

@dataclass
class dpsReportObjEtvc():
    version: str = ''
    bossId: int = -1

@dataclass
class dpsReportObjPlayer():
    displayName: str
    charName: str
    profession: int
    eliteSpec: int

@dataclass
class dpsReportObjEncounter():
    uniqueId: str = ''
    success: bool = False
    duration: int = -1
    compDps: int = -1
    numberOfPlayers: int = -1
    numberOfGroups: int = -1
    bossId: int = -1
    boss: str = ''
    isCm: bool = False
    gw2Build: int = -1
    jsonAvailable: bool = False

    # This field isn't in the API, but is placed here for convenience and efficiency. It should be used
    # to store the resulting json (IE: the raw EliteInsights output) for this encounter if fetched
    json: dict = None

    # This field isn't in the API, but placed here for convenience. The duration in the encounter object
    # super off, so you typically need to parse the actual JSON to determine the proper time. In the case
    # that this gets filled from another method, this allows us to shortcut needing to load a JSON
    accurateDuration: int = None

@dataclass
class dpsReportObj():
    id: str = None
    permalink: str = ''
    uploadTime: int = -1
    encounterTime: int = -1
    generator: str = ''
    generatorId: int = -1
    generatorVersion: int = -1
    language: str = ''
    languageId: int = -1
    etvc: dpsReportObjEtvc = field(default_factory=dpsReportObjEtvc)
    players: List[dpsReportObjPlayer] = field(default_factory=list)
    encounter: dpsReportObjEncounter = field(default_factory=dpsReportObjEncounter)

    def __post_init__(self):
        if (self.id is None):
            # Remove the first part of the URL
            idStr = self.permalink.removeprefix('https://dps.report/')

            suffixLoc = idStr.rfind('_')
            self.id = idStr[:suffixLoc]

class dpsReport():
    def __init__(self, token:str=None):
        self.baseUrl = 'https://dps.report/'

        # User Settings
        self.token = token

        # This sets the maximum retries the underlying requests library will attempt for any error
        self.maxRetries = 3

        # From others testing it seems that 4 is around what the dpsReport servers will take safely
        self.maxThreads = 4

        # Create a requests session since the dpsReport server occasionally fails, especially if we pack
        # too many requests in a row. This allows us to automatically retry with the library
        self.session = FuturesSession(max_workers=self.maxThreads)
        self.session.mount('https://',
                           adapter=HTTPAdapter(max_retries=Retry(total=3, backoff_factor=0.2,
                                               status_forcelist=[HTTPStatus.REQUEST_TIMEOUT,        # 408
                                                                 HTTPStatus.CONFLICT,               # 409
                                                                 HTTPStatus.INTERNAL_SERVER_ERROR,  # 500
                                                                 HTTPStatus.BAD_GATEWAY,            # 502
                                                                 HTTPStatus.SERVICE_UNAVAILABLE,    # 503
                                                                 HTTPStatus.GATEWAY_TIMEOUT])))     # 504

    def jsonToObject(self, json):
        ##### Parse Players
        playerObjs = []
        for player in json['players'].values():
            p = dpsReportObjPlayer(
                displayName = player['display_name'],
                charName = player['character_name'],
                profession = player['profession'],
                eliteSpec = player['elite_spec']
            )
            playerObjs.append(p)

        ##### Parse ETVC
        etvcObj = dpsReportObjEtvc(
            version = json['evtc']['version'],
            bossId  = json['evtc']['bossId']
        )

        ##### Parse Encounter

        # isCm parameter can be null, so set it to a valid boolean value
        if (json['encounter']['isCm'] is None):
            isCm = False
        else:
            isCm = json['encounter']['isCm']

        encounterObj = dpsReportObjEncounter(
            uniqueId = json['encounter']['uniqueId'],
            success = json['encounter']['success'],
            duration = json['encounter']['duration'],
            compDps = json['encounter']['compDps'],
            numberOfPlayers = json['encounter']['numberOfPlayers'],
            numberOfGroups = json['encounter']['numberOfGroups'],
            bossId = json['encounter']['bossId'],
            boss = json['encounter']['boss'],
            isCm = isCm,
            gw2Build = json['encounter']['gw2Build'],
            jsonAvailable = json['encounter']['jsonAvailable']
        )

        ##### Parse Main Object
        rtnObj = dpsReportObj(
            id = json['id'],
            permalink = json['permalink'],
            uploadTime = json['uploadTime'],
            encounterTime = json['encounterTime'],
            generator = json['generator'],
            generatorId = json['generatorId'],
            generatorVersion = json['generatorVersion'],
            language = json['language'],
            languageId = json['languageId'],
            etvc = etvcObj,
            players = playerObjs,
            encounter = encounterObj
        )

        return rtnObj

    def uploadLogs(self, logs:list[str]) -> list[tuple[str, dpsReportObj]]:
        """ Uploads a log. Optionally attaches a userToken to it for tracking
        """

        # dps.report recommends we always set this so the response is JSON formatted
        params = {'json': 1}

        # Attach the userToken if provided
        if (self.token is not None):
            params['userToken'] = self.token

        startTime = time.perf_counter()

        # Response List
        futures = []

        # Open logfile and transmit it
        for l in logs:
            #with open(l, mode='rb') as f:
            logFile = open(l, mode='rb')
            future = self.session.post(self.baseUrl + 'uploadContent', params=params, files={'file': logFile})
            future.origFile = l
            future.logFile = logFile
            futures.append(future)
            print('Queued {:s}'.format(l))

        # Collect the finished logs as we get them
        uploadedLogs = []
        for future in as_completed(futures):
            r = future.result()
            print('Finished {:s}'.format(future.origFile))

            # Close the file handle
            future.logFile.close()

            # It is possible that encounters that are too short return 403 errors since the site
            # will refuse to parse them
            if (r.status_code != requests.codes['ok']):
                uploadedLogs.append((future.origFile, None))
                continue

            uploadedLogs.append((future.origFile, self.jsonToObject(r.json())))

        endTime = time.perf_counter()
        print('Upload Total Time: {}'.format(endTime - startTime))

        return uploadedLogs

    def getUploadMetaData(self, identifier:str, isId:bool=False) -> dpsReportObj:
        """ Gets a previous encounter's meta data
            The identifier can either be the ID or the permalink, both are fairly similar.
            By default, the function assumes the identifier is the permalink. To treat it
            as an ID, you must set idId to true.
        """
        # Select the right identifier
        if (isId):
            params = {'id': identifier}
        else:
            params = {'permalink': identifier}

        # Get the data
        retryCnt = 0
        while (retryCnt < self.maxRetries):
            # Queue the Request
            future = self.session.get(self.baseUrl + 'getUploadMetadata', params=params)

            # Wait for response
            r = future.result()

            # Make sure we were successful
            r.raise_for_status()

            # Sometimes the JSON gets corrupted, in which case we should try again
            try:
                respJson = r.json()
            except:
                retryCnt+=1
                continue

            return self.jsonToObject(respJson)

    def getUploadMetaDatas(self, identifiers:list[str], isId:bool=False) -> list[dpsReportObj]:
        """ Gets a previous encounter's meta data. Similar to getUploadMetaData, but
            takes a list of logs to grab instead of a single one.
            The identifiers can either be the ID or the permalink, both are fairly similar.
            By default, the function assumes the identifier is the permalink. To treat it
            as an ID, you must set idId to true. All identifiers must be the same type.
        """

        startTime = time.perf_counter()

        # Loop until the requestsList is empty. Ideally the list gets emptied the first time around,
        # but incase the JSON gets corrupted, we will try again
        requestsList = identifiers
        resultsList = []
        while (requestsList):
            # Response List
            futures = []

            # Queue up the requests
            for request in requestsList:
                # Select the right identifier
                if (isId):
                    params = {'id': request}
                else:
                    params = {'permalink': request}

                future = self.session.get(self.baseUrl + 'getUploadMetadata', params=params)
                future.id = request
                futures.append(future)
                print('Queued {:s}'.format(request))

            # Clear the request list
            requestsList = []

            # Collect the completed tasks
            for future in as_completed(futures):
                r = future.result()

                try:
                    respJson = r.json()
                except:
                    requestsList.append(future.id)
                    print('JSON malformed, trying again for {:s}'.format(future.id))
                    continue

                resultsList.append(self.jsonToObject(respJson))
                print('Finished {:s}'.format(future.id))

        endTime = time.perf_counter()
        print('Fetch Metadata Total Time: {}'.format(endTime - startTime))
        return resultsList

    def getJson(self, id:str=None, link:str=None) -> dict:
        """ Gets a previous encounter's raw data
        """
        # Make sure we got at least one identifier
        if (id is not None):
            params = {'id': id}
        elif (link is not None):
            params = {'permalink': link}
        else:
            raise ValueError('Must pass either ID or Permalink to lookup metadata')

        # Get the data
        retryCnt = 0
        while (retryCnt < self.maxRetries):
            # Queue Request
            future = self.session.get(self.baseUrl + 'getJson', params=params)

            # Wait for response
            r = future.result()

            # Make sure we were successful
            r.raise_for_status()

            # Sometimes the JSON gets corrupted, in which case we should try again
            try:
                respJson = r.json()
            except:
                retryCnt+=1
                continue

            # This is Elite Insights output, so we just pass this as it
            return respJson

    def getJsons(self, logs:list[dpsReportObj]):
        """ Given a list of dpsReportObjs, fill in their JSON field with the EI raw JSON
        """
        startTime = time.perf_counter()

        # Loop until the requestsList is empty. Ideally the list gets emptied the first time around,
        # but incase the JSON gets corrupted, we will try again
        requestsList = logs
        while (requestsList):
            # Response List
            futures = []

            # Queue up the requests
            for request in requestsList:
                params = {'permalink': request.permalink}
                future = self.session.get(self.baseUrl + 'getJson', params=params)
                future.obj = request
                futures.append(future)
                print('Queued {:s}'.format(request.permalink))

            # Clear the request list
            requestsList = []

            # Collect the completed tasks
            for future in as_completed(futures):
                r = future.result()

                try:
                    respJson = r.json()
                    future.obj.encounter.json = respJson
                    print('Finished {:s}'.format(future.obj.permalink))
                except:
                    requestsList.append(future.obj)
                    print('JSON malformed, trying again for {:s}'.format(future.obj.permalink))

        endTime = time.perf_counter()
        print('Fetch JSON Total Time: {}'.format(endTime - startTime))

    def getUploads(self, page:int=1) ->list[dpsReportObj]:
        """ Returns previous logs
        """
        # Queue the request
        params = {'userToken':self.token, 'page':page}
        future = self.session.get(self.baseUrl + 'getUploads', params=params)

        # Wait for a response
        r = future.result()

        # Make sure we were successful
        r.raise_for_status()

        # The returned value is an array of results, so we need to break each one up
        data = r.json()

        rtnObjs = []
        for encounter in data['uploads']:
            rtnObjs.append(self.jsonToObject(encounter))

        return rtnObjs

    def getUserToken(self) -> str:
        """ Gets a User Token from DPS.report. Since this generates uniquely if you don't
            have a cookie, which we don't, this will always return a new token
        """
        # Queue the request
        future = self.session.get(self.baseUrl + 'getUserToken')

        # Wait for a response
        r = future.result()

        # Make sure we were successful
        r.raise_for_status()

        # Store token in self
        self.token = r.text

        return(r.text)

class dpsReportIds():
    '''
    A static class of utility functions to help with look up into the mapping table
    '''

    @staticmethod
    def folderNameToShortName(folderName:str) -> str:
        ''' Given a folder name, look up the corresponding short name.

            Will raise a KeyError is the folder name doesn't match
        '''

        # Iterate through all IDs in the map to find all folder name matches
        for (shortName, values) in targetIdMap.items():
            if (folderName in values['FolderNames']):
                return shortName
        else:
            raise KeyError('Folder Name {:s} does not match any known IDs'.format(folderName))

    @staticmethod
    def idToShortName(id:int) -> str:
        ''' Given a boss ID, will look up the shor name it is a part of.

            Will raise a KeyError if the ID isn't associated with any short name
        '''

        for (shortName, values) in targetIdMap.items():
            for i in values['IDs']:
                if (id == i):
                    return shortName
        else:
            raise KeyError('ID {:d} does not match any known IDs'.format(id))

    @staticmethod
    def shortNameToIds(shortName:str) -> List[int]:
        ''' Given a short name, look up the corresponding boss IDs. Since some folders contain multiple IDs,
            this function returns a list of IDs

            Will raise a KeyError if the short name doesn't match
        '''

        if (shortName in targetIdMap.keys()):
            return targetIdMap[shortName]['IDs']
        else:
            raise KeyError('Short Name {:s} not known'.format(shortName))

    @staticmethod
    def shortNameToPrettyName(shortName:str) -> str:
        ''' Given a short name, look up the pretty name. There should only be one matching pretty name to short
            name.

            Will raise a KeyError if the short name doesn't match
        '''

        if (shortName in targetIdMap.keys()):
            return targetIdMap[shortName]['PrettyName']
        else:
            raise KeyError('Short Name {:s} not known'.format(shortName))