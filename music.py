# This file is part of my PythonMusicOrganizer project
# 
# MIT License
# Copyright (c) 2025 Jacob Nuttall. All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import musicbrainzngs
import acoustid 
import typing 
import mutagen
import os 
import sys 
import logging 
import time 
import datetime 
import dateutil
import shutil
import traceback
import tqdm 
from collections import Counter
from nltk.metrics import edit_distance
import json
from threading import Thread 
import time 
from pydub import AudioSegment
from enum import Enum 
import shazamio
import nest_asyncio
nest_asyncio.apply()
import asyncio
from aiohttp_retry import ExponentialRetry

tmpfile = os.path.join(os.path.dirname(__file__), 'tmp', 'tmpfile')
os.makedirs(os.path.dirname(os.path.dirname(tmpfile)), exist_ok=True)
shazam = None

# Python decorator to run a function in a thread and prevent keyboard interrupts
def noInterrupt(func)->typing.Callable:
    def wrapper(*args, **kwargs)->None:
        t = Thread(target=func, args=args, kwargs=kwargs)
        t.start()
        t.join()
        
        
    return wrapper

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

ACOUST_ID_RATE_LIMIT = 3.0 # per second

def prompt_confirm(message):
    """Prompt the user for a yes/no confirmation."""
    while True:
        response = input(f"{message} (y/n): ").strip().lower()
        if response in ['y', 'yes']:
            return True
        elif response in ['n', 'no']:
            return False
        else:
            print("Please enter 'y' or 'n'.")

class SaveState():
    
    processed_data:dict
    
    def __init__(self, outpath, save_data=None, ):
        
        if save_data is None:
            self.processed_data = self.init_child(self.rootstring())
        else:
            self.processed_data = save_data
            
        self.outpath = outpath
        
        self.save_state()
    
    @staticmethod
    def load_save(save_file:str):
        save_data = None
        
        if os.path.exists(save_file):
            try:
                with open(save_file, 'r') as f:
                    save_data = json.load(f)
            except json.decoder.JSONDecodeError:
                logger.error(f"Could not read save file {save_file}.")
                if not prompt_confirm("Continue, overwriting save file?"):
                    sys.exit(1)
                save_data = None
        else:
            save_data = None 
            
        return SaveState(save_file, save_data=save_data)
    
    @staticmethod
    def rootstring():
        return 'root'
        
    @staticmethod
    def childstring():
        return 'children'
    
    @staticmethod
    def namestring():
        return 'name'
    
    @staticmethod
    def donestring():   
        return 'done'
    
    @noInterrupt
    def save_state(self):
        with open(self.outpath, 'w') as f:
            json.dump(self.processed_data, f, indent=4)
        
    def init_child(self, name, mark_done=False):
        return {self.namestring(): name, self.childstring(): {}, self.donestring(): mark_done}
    
    def update_path(self, path, mark_done=False)->None:
        split = path.split(os.sep)
        end = split[-1]
        root = self.processed_data
        
        for piece in split:
            if piece not in root[self.childstring()]:
                root[self.childstring()][piece] = self.init_child(piece)
                    
            root = root[self.childstring()][piece]
            if root[self.namestring()] == end and mark_done:
                root[self.donestring()] = True
                
                # Prune the children when we mark a branch as done
                root[self.childstring()] = {}
                
        self.save_state()
    
    def get_state(self, path)->bool:
        split = path.split(os.sep)
        end = split[-1]
        root = self.processed_data
    
        for piece in split:
            if root[self.donestring()]:
                return True # If parent is done, assume all children are done
            
            if piece not in root[self.childstring()]:
                return False
            root = root[self.childstring()][piece]
            if root[self.namestring()] == end:
                return root[self.donestring()]
        return False
 

def clean_filename(name):
    illegal_chars_replace_dict = {
        '/': ' - ',
        '\\': ' - ',
        '?': '',
        '%': '',
        '*': '',
        ':': ' - ',
        '|': ' - ',
        '"': "'",
        '<': '',
        '>': '',
        '.': ' ', # Replace dots with spaces to avoid issues in filenames
        '\t': ' ',
        '\n': ' ',
        '\r': ' ',
        '&': 'and'
    }
    for k, v in illegal_chars_replace_dict.items():
        name = name.replace(k, v)
        
    while '  ' in name:
        name = name.replace('  ', ' ')
    name = name.strip()
    return name

class MetaData():
    
    artists:str
    album:str
    title:str
    year:str
    decade:str
    filetype:str 
    
    _artists:str
    _album:str 
    _title:str 
    _date:datetime.date
    _filetype:str 
    
    def __init__(self, year:str=None, artists:str=None, album:str=None, title:str=None, filetype:str=None):
        self.artists = artists
        self.album = album
        self.title = title
        self.filetype = filetype
        self.year = year
        
    @property 
    def year(self)->str:
        if self._year is None:
            return self.unknownYear
        return self._year
    @year.setter
    def year(self, value):
        if value is None:
            value = self.unknownYear
        self._year = str(value)
    
    @property
    def decade(self)->str:
        if self.year != self.unknownYear:
            year_int = int(self.year)
            decade_start = (year_int // 10) * 10
            return f"{decade_start}s"
        
    @property 
    def artists(self)->str:
        return self._artists
    @artists.setter
    def artists(self, value):
        if value is None:
            value = self.unknownArtist
        self._artists = value
    
    @property
    def album(self)->str:
        return self._album
    @album.setter
    def album(self, value):
        self._album = str(value) if value else self.unknownAlbum
    
    @property
    def title(self)->str:
        return self._title
    @title.setter
    def title(self, value):
        if value is None:
            raise ValueError("Title cannot be empty")
        self._title = value
        
    @property 
    def unknownArtist(self)->str:
        return 'Unknown Artist'
    
    @property
    def unknownAlbum(self)->str:
        return 'Unknown Album'
    
    @property
    def unknownYear(self)->str:
        return 'Unknown Year'
    
    @property
    def filetype(self)->str:
        return self._filetype
    @filetype.setter
    def filetype(self, value):
        if not value:
            raise ValueError("Filetype cannot be empty")
        self._filetype = value
    
    @property 
    def filename(self)->str:
        file = clean_filename(f'{self.title}')
        file = f'{file}.{self.filetype}'
        return file 
    
    @property 
    def album_dir(self)->str:
        return clean_filename(self.album)
    
    @property 
    def artists_dir(self)->str:
        return clean_filename(self.artists)
    
    @property 
    def year_dir(self)->str:
        return clean_filename(self.year)
    
    @property 
    def relativeFileDir(self)->str:
        artists_dir = self.artists_dir
        year_dir = self.year_dir
        album_dir = self.album_dir
        path = os.path.join(self.filetype, artists_dir, year_dir, album_dir)
        return path
    
    @property 
    def relativeFilePath(self)->str:
        return os.path.join(self.relativeFileDir, self.filename)    
    
    def __str__(self):
        return f'Title: {self.title}\nArtists: {self.artists}\nAlbum: {self.album}\nYear: {self.year}\nFiletype: {self.filetype}\nPath: {self.relativeFilePath}'
    
class MatchError(Exception):
    pass

def score_string_match(candidate:str, target:str)->float:
    
    if candidate is None or target is None:
        return 0.0
    
    candidate = candidate.lower().strip()
    target = target.lower().strip()
    if candidate == target:
        return 1.0
    candidate_ms = Counter(candidate)
    target_ms = Counter(target)
    intersection = candidate_ms & target_ms
    intersection_size = sum(intersection.values())
    target_size = sum(target_ms.values())
    
    
    intersect_score = intersection_size / target_size
    edit_dist = edit_distance(target, candidate)
    max_len = max(len(candidate), len(target))
    if max_len == 0:
        edit_score = 1.0
    else:
        edit_score = 1 - (edit_dist / max_len)
    
    return (intersect_score + edit_score) / 2.0

def start_service(app_name:str, app_version:str, contact:str):
    global shazam 
    start_musicbrainz(app_name, app_version, contact)
    shazam = shazamio.Shazam(segment_duration_seconds=12)

def start_musicbrainz(app_name, app_version, contact):
    musicbrainzngs.set_useragent(app_name, app_version, contact)
    musicbrainzngs.set_rate_limit(1.0, new_requests=1)

def aidmatch(filename, AID_API_KEY)->typing.List[typing.Tuple[float, str, str, str]]:
    
    try:
        results = acoustid.match(AID_API_KEY, filename)
    except acoustid.NoBackendError as e:
        logger.error("chromaprint library/tool not found")
        raise e
    except acoustid.FingerprintGenerationError as e:
        logger.error("fingerprint could not be calculated")
        raise e
     
    except acoustid.WebServiceError as e:
        logger.error("web service request failed:", e.message)
        raise e
    
    results = list(results)
    
    return results

def process_aid_results(results, og_artist=None, og_title=None)->dict:
    
    aid_score, rid, title, artist = 0.0, None, None, None
    artist_score = 0.0
    title_score = 0.0
    combined_score = 0.0
    scores = {'rids': [], 'aid_scores': [], 'artist_scores': [], 'title_scores': [], 'combined_scores': []}
    
    for cand_aid_score, cand_rid, cand_title, cand_artist in results:
        
        cand_artist_score = 0.0
        if og_artist is not None:
            cand_artist_score = score_string_match(cand_artist, og_artist)
        cand_title_score = 0.0
        if og_title is not None:
            cand_title_score = score_string_match(cand_title, og_title)
        cand_combined_score = cand_aid_score + 2*cand_artist_score + 3*cand_title_score
        
        scores['rids'].append(cand_rid)
        scores['aid_scores'].append(cand_aid_score)
        scores['artist_scores'].append(cand_artist_score)
        scores['title_scores'].append(cand_title_score)
        scores['combined_scores'].append(cand_combined_score)
    
    for i in range(len(scores['rids'])):
        if scores['combined_scores'][i] > combined_score:
            aid_score = scores['aid_scores'][i]
            artist_score = scores['artist_scores'][i]
            title_score = scores['title_scores'][i]
            rid = scores['rids'][i]
            title = results[i][2]
            artist = results[i][3]
            combined_score = scores['combined_scores'][i] / 6.0    
    
    if rid is None:
        raise MatchError("No AcoustID match found.")
    
    logger.info('------')
    logger.info('AcoustID Match Found:')
    logger.info(f'Recording ID: {rid}')
    logger.info(f'Found Title: {title}')
    logger.info(f'Found Artist: {artist}')
    
    if og_artist is not None:
        logger.info('Original Artist: %s' % og_artist)
    if og_title is not None:
        logger.info('Original Title: %s' % og_title)
        
    logger.info('http://musicbrainz.org/recording/%s' % rid)
    logger.info('Combined Score: %i%%' % (int(combined_score * 100)))
    logger.info('AcoustID Score: %i%%' % (int(aid_score * 100)))
    logger.info('Artist Score: %i%%' % (int(artist_score * 100)))
    logger.info('Title Score: %i%%' % (int(title_score * 100)))
    logger.info('------')
        
    # Make sure to wait ~1/3 second between requests
    time.sleep(1.1 / ACOUST_ID_RATE_LIMIT)
    title = title
    artist = artist
    return rid, title, artist

def search_shazam_metadata(file)->dict:
    global shazam
    time.sleep(1.0 / ACOUST_ID_RATE_LIMIT) # try not to exceed rate limit
    out = asyncio.run(shazam.recognize(file))
    if 'track' not in out:
        raise MatchError("No Shazam match found.") 
    return out

def process_shazam_metadata(shazam_result, filetype)->MetaData:
    
    track = shazam_result['track']
    title = track['title'].strip()
    artists = track['subtitle'].strip()
    
    sections = track['sections'][0]
    section_metadata = sections['metadata']
    album = None
    year = None
    for smd in section_metadata:
        if smd['title'] == 'Album':
            album = smd['text'].strip()
            break
        if smd['title'].lower() == 'Released'.lower():
            date = dateutil.parser.parse(smd['text'])
            year = date.year
            break
            
    return MetaData(year=year, artists=artists, album=album, title=title, filetype=filetype)

def search_musicbrainz_metadata(acoustid_id)->typing.Tuple[MetaData, dict]:
    try:
        result = musicbrainzngs.get_recording_by_id(acoustid_id, includes=["artists", "releases"])
        metadata = process_musicbrainz_metadata(result, filetype='mp3')
        return metadata
    except musicbrainzngs.WebServiceError as e:
        logger.error("Something went wrong with the request: %s", traceback.format_exc())
        raise e
    except Exception as e: # TODO: Print stacktrace
        logger.error(f"Error processing MusicBrainz metadata: {traceback.format_exc()}")
        raise e

def process_musicbrainz_metadata(mb_result, artists, title, filetype) -> MetaData:
    recording = mb_result['recording']
    
    if len(recording['release-list']) == 0:
        album = None
        year = None
    
    else:
        first_release = recording['release-list'][0]
        album = first_release['title'].strip()
        
        # Get year from first release date
        year = None
        if 'date' in first_release:
            date = dateutil.parser.parse(first_release['date'].strip())
            year = date.year 
    
    processed = MetaData(year=year, artists=artists, album=album, title=title, filetype=filetype)
    return processed


def getValueIfNotNone(s1, s2):
    if s1 is None and s2 is None:
        return None
    if s1 is None:
        return s2
    if s2 is None:
        return s1
    return s1
    

def matchWithTargetMetadataArtist(candidate_artist, target_artist, threshold=0.8, message='')->bool:
    artist_score = score_string_match(candidate_artist, target_artist)
    logger.info(f"Artist Score{message}: {artist_score}")
    if artist_score >= threshold:
        return True
    
def matchWithTargetMetadataTitle(candidate_title, target_title, threshold=0.8, message='')->bool:
    title_score = score_string_match(candidate_title, target_title)
    logger.info(f"Title Score{message}: {title_score}")
    if title_score >= threshold:
        return True

def matchWithTargetMetadata(candidate_metadata, target_metadata, threshold=0.8, message='')->bool:
    return matchWithTargetMetadataArtist(candidate_metadata.artists, target_metadata.artists, threshold=threshold, message=message) \
        and matchWithTargetMetadataTitle(candidate_metadata.title, target_metadata.title, threshold=threshold, message=message)
   
def mergeMetadata(m1:MetaData, m2:MetaData)->MetaData:  
    if m1 is None and m2 is None:
        return None
    if m1 is None:
        return m2
    if m2 is None:
        return m1
    
    year = getValueIfNotNone(m1.year, m2.year)
    artists = getValueIfNotNone(m1.artists, m2.artists)
    album = getValueIfNotNone(m1.album, m2.album)
    title = getValueIfNotNone(m1.title, m2.title)
    filetype = getValueIfNotNone(m1.filetype, m2.filetype)
    
    return MetaData(year=year, artists=artists, album=album, title=title, filetype=filetype)

def search_online_metadata(file, aid_api_key, ogMetaData:MetaData=None)->typing.Tuple[MetaData, dict]:
    filetype = os.path.splitext(file)[1].lstrip('.')
    
    # Find match with shazarm
    shazam_metadata = None
    logging.info(f"Fetching metadata for \"{file}\". Trying first with Shazam.")
    try:
        shazamresults = search_shazam_metadata(file) 
        shazam_metadata = process_shazam_metadata(shazamresults, filetype)
        logger.info(f"Shazam Metadata for \"{file}\":\n{shazam_metadata}")
    except MatchError as e:
        logger.warning(f"No Shazam match found for \"{file}\". Trying AcoustID.")
   
    # Find match with AID
    aid_metadata = None
    try:
        aid_results = aidmatch(file, aid_api_key)
        aidrid, title, artists = process_aid_results(aid_results, og_artist=ogMetaData.artists, og_title=ogMetaData.title)
        result = musicbrainzngs.get_recording_by_id(aidrid, includes=["artists", "releases"])
        aid_metadata = process_musicbrainz_metadata(result, artists, title, filetype)
        logger.info(f"AcoustID / MusicBrainz Metadata for \"{file}\":\n{aid_metadata}")
    except musicbrainzngs.WebServiceError as e:
        logger.error("Something went wrong with the request: %s", traceback.format_exc())
    except MatchError as e:
        logger.warning(f"No AcoustID match found for \"{file}\".")
    except Exception as e: # TODO: Print stacktrace
        logger.error(f"Error processing MusicBrainz metadata: {traceback.format_exc()}")
        
        
    new_metadata = MetaData(year=ogMetaData.year, artists=ogMetaData.artists, album=ogMetaData.album, title=ogMetaData.title, filetype=filetype)
    
    if aid_metadata is not None and shazam_metadata is not None:
        # Compare the two and return the one with more complete metadata
        
        if matchWithTargetMetadata(aid_metadata, shazam_metadata, threshold=0.8, message=' between AcoustID and Shazam'):
            logger.info("AcoustID and Shazam results agree on artist and title. Using combined metadata, prefering AcoustID where available.")
            new_metadata = mergeMetadata(aid_metadata, shazam_metadata)
            return new_metadata, True
           
        else:
            logger.warning(f"Could not reconcile AcoustID and Shazam results for \"{file}\".")
            
    if aid_metadata is not None:
        logger.info("Comparing AcoustID results to provided artist and title.")
        match_title = matchWithTargetMetadataTitle(aid_metadata.title, ogMetaData.title, threshold=0.8, message=' for AcoustID')
        match_artist = matchWithTargetMetadataArtist(aid_metadata.artists, ogMetaData.artists, threshold=0.8, message=' for AcoustID')
        if match_artist and match_title:
            logger.info("AcoustID results match provided artist and title. Using AcoustID metadata.")
            new_metadata = aid_metadata
            if new_metadata.album == new_metadata.unknownAlbum:
                new_metadata.album = ogMetaData.album
            if new_metadata.year == new_metadata.unknownYear:
                new_metadata.year = ogMetaData.year
            return new_metadata, True
        elif match_artist:
            new_metadata.artists = aid_metadata.artists
            return new_metadata, False
        else:
            logger.warning(f"AcoustID results do not match provided artist and title for \"{file}\".")
        
    if shazam_metadata is not None:
        logger.info("Comparing Shazam results to provided artist and title.")
        match_title = matchWithTargetMetadataTitle(shazam_metadata.title, ogMetaData.title, threshold=0.8, message=' for Shazam')
        match_artist = matchWithTargetMetadataArtist(shazam_metadata.artists, ogMetaData.artists, threshold=0.8, message=' for Shazam')       
        if match_artist and match_title:
            logger.info("Shazam results match provided artist and title. Using Shazam metadata.")
            new_metadata = shazam_metadata
            if new_metadata.album == new_metadata.unknownAlbum:
                new_metadata.album = ogMetaData.album
            if new_metadata.year == new_metadata.unknownYear:
                new_metadata.year = ogMetaData.year
            return new_metadata, True
        elif match_artist:
            new_metadata.artists = shazam_metadata.artists
            return new_metadata, False
        
        else:
            logger.warning(f"Shazam results do not match provided artist and title for \"{file}\".")
            
    return None, False # Not confident we have a match

def extract_and_update_metadata(file, aid_api_key=None, update_from_mb=False)->typing.Union[MetaData, mutagen.FileType]:
    song = mutagen.File(file, easy=True)
    basename = os.path.basename(file)
    filename, filetype = os.path.splitext(basename)
    filetype = filetype.lstrip('.')
    if song is None: 
        raise ValueError(f"Cannot read metadata for {file}")
       
    unknown_title = False 
    if 'title' in song:
        title = ''.join(song['title'])
    else:
        unknown_title = True
        title = filename # Use filename as title if title is missing
    
    unknown_album = False
    if 'album' in song:
        album = ''.join(song['album'])
    else:
        # Use parent directory as album name
        unknown_album = True
        album = os.path.basename(os.path.dirname(file))
        
    unknown_artist = False 
    if 'artist' in song:    
        artists = ''.join(song['artist'])
    else:
        unknown_artist = True
        artists = album # Use album as artist if artist is missing
    
    unknown_year = False
    if 'date' in song:
        date = ''.join(song['date'])
        date = dateutil.parser.parse(date)
        year = date.year
    
    else:
        year = None
        unknown_year = True
       
    metadata = MetaData(year=year, artists=artists, album=album, title=title, filetype=filetype)
    logger.info('------')
    logger.info(f'Extracted Metadata for "{file}":\n{metadata}')
    logger.info('------')
    
    unknown_artist = unknown_artist or artists is None or 'various' in artists.lower() or 'unknown' in artists.lower()
    has_unknown = unknown_artist or unknown_album or unknown_title # Not too concerned about year being unknown
    update_artist = unknown_artist or update_from_mb
    update_album = unknown_album or update_from_mb
    update_title = unknown_title or update_from_mb
    update_year = update_from_mb or unknown_year
    doUpdate = False
    if update_from_mb and aid_api_key is None:
        logger.warning("AcoustID API key not provided. Cannot update metadata from MusicBrainz.")
    if has_unknown and aid_api_key is None:
        logger.warning(f"Metadata for \"{file}\" is incomplete, but AcoustID API key not provided. Cannot update metadata from MusicBrainz.")
    if (has_unknown or update_from_mb) and aid_api_key is not None:
        if has_unknown and not update_from_mb:
            logger.info(f"Metadata for \"{file}\" is incomplete. Attempting to update missing metadata from MusicBrainz.")
        if update_from_mb:
            logger.info(f"Updating metadata for \"{file}\" from MusicBrainz.")
        mb_metadata = None 
        try:
            
            # SEARCHING ONLINE FOR METADATA
            logger.info(f"Fetching metadata for \"{file}\" from MusicBrainz")
            mb_metadata, doUpdate = search_online_metadata(file, aid_api_key, ogMetaData=metadata)
            
            logger.info(f"Successfully fetched metadata for \"{file}\" from MusicBrainz.")
            logger.info(f"MusicBrainz Metadata for \"{file}\":\n{mb_metadata}")
            
            # update only the unknown fields
            # Update metadata
            if mb_metadata is not None:
                if update_artist:
                    
                    # Don't update if artist is unknown
                    mb_artists = mb_metadata.artists_dir
                    if mb_artists is not None and mb_artists != mb_metadata.unknownArtist:
                        logger.info(f"Updating artist for \"{file}\" w/ \"{mb_artists}\"")
                        metadata.artists = mb_artists
                        unknown_artist = False
                        
                if update_album:
                    mb_album = mb_metadata.album
                    # Don't update if album is unknown
                    if mb_album is not None and mb_album != mb_metadata.unknownAlbum:
                        logger.info(f"Updating album for \"{file}\" w/ \"{mb_album}\"")
                        metadata.album = mb_album
                        unknown_album = False
                
                if update_title:
                    mb_title = mb_metadata.title
                    if mb_title is not None:
                        logger.info(f"Updating title for \"{file}\" w/ \"{mb_title}\"")
                        metadata.title = mb_title
                        unknown_title = False
                    
                if update_year:
                    mb_year = mb_metadata.year
                    # Don't update if year is unknown
                    if mb_year is not None and mb_year != mb_metadata.unknownYear:
                        logger.info(f"Updating year for \"{file}\" w/ \"{mb_year}\"")
                        metadata.year = mb_year
                        unknown_year = False
                        
            logger.info('------')
            logger.info(f'Updated Metadata for "{file}":\n{metadata}')
            logger.info('------')
                
        except MatchError as e:
            logger.warning(f"No AcoustID match found for \"{file}\". Cannot update metadata from MusicBrainz.")
        except Exception as e:
            logger.error(f"Could not update metadata for \"{file}\": {traceback.format_exc()}")
            
    if unknown_artist and not doUpdate: 
        # If artist is still unknown, we don't want to make any changes to the metadata.
        # Will require a manual sorting.
        metadata.artists = '! Sort'
        return metadata, song
    
    if unknown_album:
        song['album'] = metadata.album
    if unknown_artist:
        song['artist'] = metadata.artists
    if unknown_title:
        song['title'] = metadata.title
    if unknown_year:
        song['date'] = metadata.year
    
    return metadata, song

@noInterrupt
def copy_song(src, dest, overwrite=False, mutagen_file:mutagen.FileType=None, pbar=None, save=None):
   
    if not overwrite and os.path.exists(dest):
        basename = os.path.basename(dest)
        logger.warning(f'File "{basename}" already exists. Skipping copy of "{src}".')
        save_mark_done(src, save=save)
        return
    
     # Ensure the destination directory exists
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    shutil.copy2(src, dest)
    if mutagen_file is not None:
        mutagen_file.save(dest)
    logger.info(f"Copied \"{src}\" to \"{dest}\"")
    if pbar is not None:
        pbar.set_description(f'\nCopied "{src}" to "{dest}".')
        print(end='') # Prevent tqdm from adding a new line
    
    save_mark_done(src, save=save)
    
def count_total_files(src):
    total_files = 0
    for root, dirs, files in os.walk(src):
        total_files += len(files)
    return total_files


def check_is_music_file(file):
    # use mutagen 
    try:
        song = mutagen.File(file, easy=True)
        if song is not None:
            return True
        else:
            return False
    except Exception as e:
        return False

def save_mark_done(src, save:SaveState=None):
    if save is not None:
        save.update_path(src, mark_done=True)
        
def message_skip_procesed(src):
    logger.info(f'Skipping already processed source path: {src}')
        
def save_check_done(src, save:SaveState=None)->bool:
    if save is not None:
        return save.get_state(src)
    return False

def process_song_directory(src, dst, overwrite=False, acoustid_api_key=None, total_files=None, update_from_mb=True, save:SaveState=None):
    with tqdm.tqdm(total=total_files, desc="Processing files", dynamic_ncols=False) as pbar:
        for root, dirs, files in os.walk(src, topdown=False): # Walk tree from bottom up to ensure we process files in subdirectories first
            
            if save_check_done(root, save=save):
                message_skip_procesed(root)
                count_total_files(root)
                pbar.update(len(files))
                print(end='') # Prevent tqdm from adding a new line
                continue
            
            for file in files:
                
                src_filepath = os.path.join(root, file)
                
                pbar.update()
                print(end='') # Prevent tqdm from adding a new line
                
                if save_check_done(src_filepath, save=save):
                    message_skip_procesed(src_filepath)
                    continue
                
                pbar.set_description(f'\nProcessing \"{src_filepath}\".')
                
                if not check_is_music_file(src_filepath):
                    logger.info(f'Skipping non-music file: \"{src_filepath}\"')
                    continue
                
                try:
                    metadata, song = extract_and_update_metadata(src_filepath, aid_api_key=acoustid_api_key, update_from_mb=update_from_mb)
                    dest_path = os.path.join(dst, metadata.relativeFilePath)
                    
                except Exception as e:
                    logger.error(f"Error processing \"{src_filepath}\": {traceback.format_exc()}")
                    continue
                
                try:
                    copy_song(src_filepath, dest_path, overwrite=overwrite, mutagen_file=song, save=save, pbar=pbar)
                except Exception as e:
                    logger.error(f"Error copying \"{src_filepath}\" to \"{dest_path}\": {traceback.format_exc()}")
                    continue
                
            save_mark_done(root, save=save)
                
def process_paths(srcs, dst, overwrite=False, acoustid_api_key=None, update_from_mb=True, save:SaveState=None):
    
    for src in srcs:
        if save_check_done(src, save=save):
            message_skip_procesed(src)
            continue
        
        logger.warning(f"Processing source directory: {src}")
        total_files = count_total_files(src)
        process_song_directory(
            src, 
            dst, 
            overwrite=overwrite, 
            acoustid_api_key=acoustid_api_key, 
            total_files=total_files, 
            update_from_mb=update_from_mb,
            save=save)
        
        save_mark_done(src, save=save)