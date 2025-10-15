# This file is part of my PythonMusicOrganizer project
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

logger = logging.getLogger(__name__)

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
    
    processed_daata:dict
    
    def __init__(self, outpath, save_data=None, ):
        
        if save_data is None:
            self.processed_daata = self.init_child(self.rootstring)
        else:
            self.processed_daata = save_data
            
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
    
    def save_state(self):
        with open(self.outpath, 'w') as f:
            json.dump(self.processed_daata, f, indent=4)
        
    def init_child(self, name, mark_done=False):
        return {self.namestring(): name, self.childstring(): {}, self.donestring(): mark_done}
    
    def update_path(self, path, mark_done=False)->None:
        split = path.split(os.sep)
        end = split[-1]
        root = self.processed_daata
        
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
        root = self.processed_daata
    
        for piece in split:
            if root[self.donestring()]:
                return True # If parent is done, assume all children are done
            
            if piece not in root[self.childstring()]:
                return False
            root = root[self.childstring()][piece]
            if root[self.namestring()] == end:
                return root[self.donestring()]
        return False
    
    

def start_musicbrainz(app_name, app_version, contact):
    
    musicbrainzngs.set_useragent(app_name, app_version, contact)
    musicbrainzngs.set_rate_limit(1.0, new_requests=1)

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
            return self.defaultYear
        return self._year
    @year.setter
    def year(self, value):
        if value is None:
            value = self.defaultYear
        self._year = str(value)
    
    @property
    def decade(self)->str:
        if self.year != self.defaultYear:
            year_int = int(self.year)
            decade_start = (year_int // 10) * 10
            return f"{decade_start}s"
        
    @property 
    def artists(self)->str:
        return self._artists
    @artists.setter
    def artists(self, value):
        if value is None:
            value = self.defaultArtist
        self._artists = value
    
    @property
    def album(self)->str:
        return self._album
    @album.setter
    def album(self, value):
        self._album = str(value) if value else self.defaultAlbum
    
    @property
    def title(self)->str:
        return self._title
    @title.setter
    def title(self, value):
        if value is None:
            raise ValueError("Title cannot be empty")
        self._title = value
        
    @property 
    def defaultArtist(self)->str:
        return 'Unknown Artist'
    
    @property
    def defaultAlbum(self)->str:
        return 'Unknown Album'
    
    @property
    def defaultYear(self)->str:
        return 'Unknown Year'
    
    @property 
    def manyArtists(self)->str:
        return 'Various Artists'
    
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
        return clean_filename(self.artists )
    
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
    
    
class AIDMatchError(Exception):
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
    
    return ((intersection_size / target_size) + edit_score) / 2.0

def aidmatch(filename, AID_API_KEY, og_artist=None, og_title=None):
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

    first = True
    aid_score, rid, title, artist = 0.0, None, None, None
    artist_score = 0.0
    title_score = 0.0
    combined_score = 0.0
    
    for cand_aid_score, cand_rid, cand_title, cand_artist in results:
        
        cand_artist_score = 0.0
        if og_artist is not None:
            cand_artist_score = score_string_match(cand_artist, og_artist)
        cand_title_score = 0.0
        if og_title is not None:
            cand_title_score = score_string_match(cand_title, og_title)
        cand_combined_score = cand_aid_score + 2*cand_artist_score + 3*cand_title_score
        
        if cand_combined_score > combined_score:
            rid = cand_rid
            aid_score = cand_aid_score
            title_score = cand_title_score
            artist_score = cand_artist_score
            
            title = cand_title
            artist = cand_artist
            combined_score = cand_combined_score
            
    if rid is None:
        raise AIDMatchError(f"No AcoustID match found for {filename}")    
    
    logger.info('------')
    logger.info('AcoustID Match Found:')
    logger.info('Recording ID: %s' % rid)
    logger.info('Found Title: %s' % title)
    logger.info('Found Artist: %s' % artist)
    
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
    return rid

def process_musicbrainz_metadata(mb_result, filetype) -> MetaData:
    recording = mb_result['recording']
    title = recording['title']
    
    if len(recording['release-list']) == 0:
        album = None
        artists = None
        year = None
    
    else:
        first_release = recording['release-list'][0]
        album = first_release['title']
        
        # Get artists
        artists = first_release['artist-credit-phrase']
        if len(artists) == 0:
            artists = None 
        
        # Get year from first release date
        year = None
        if 'date' in first_release:
            date = dateutil.parser.parse(first_release['date'])
            year = date.year 
    
    processed = MetaData(year=year, artists=artists, album=album, title=title, filetype=filetype)
    return processed

def musicbrainz_get_metadata(acoustid_id)->typing.Tuple[MetaData, dict]:
    try:
        result = musicbrainzngs.get_recording_by_id(acoustid_id, includes=["artists", "releases"])
        metadata = process_musicbrainz_metadata(result, filetype='mp3')
        return metadata, result
    except musicbrainzngs.WebServiceError as e:
        logger.error("Something went wrong with the request: %s", traceback.format_exc())
        raise e
    except Exception as e: # TODO: Print stacktrace
        logger.error(f"Error processing MusicBrainz metadata: {traceback.format_exc()}")
        raise e

def extract_and_update_metadata(file, aid_api_key=None, update_from_mb=False)->typing.Union[MetaData, mutagen.FileType]:
    song = mutagen.File(file, easy=True)
    basename = os.path.basename(file)
    filename, filetype = os.path.splitext(basename)
    filetype = filetype.lstrip('.')
    if song is None: 
        raise ValueError(f"Cannot read metadata for {file}")
    
    # TODO: Use metadata object
    unknown_artist = False 
    if 'artist' in song:    
        artists = ''.join(song['artist'])
    else:
        unknown_artist = True
        artists = None
        
    unknown_album = False
    if 'album' in song:
        album = ''.join(song['album'])
    else:
        # Use parent directory as album name
        unknown_album = True
        album = os.path.basename(os.path.dirname(file))
    
    unknown_title = False 
    if 'title' in song:
        title = ''.join(song['title'])
    else:
        # Use filename as title
        unknown_title = True
        title = filename
        
        
    unknown_year = False   
    if 'date' in song:
        date = ''.join(song['date'])
        date = dateutil.parser.parse(date)
        year = date.year
    
    else:
        unknown_year = True
        year = None
        
    if artists is not None and 'unknown' in artists.lower():
        unknown_artist = True
        artists = None
    if album is not None and 'unknown' in album.lower():
        unknown_album = True
        album = None
    if title is not None and 'unknown' in title.lower():
        unknown_title = True
        title = None
    if year is not None and 'unknown' in str(year).lower():
        unknown_year = True
        year = None
        
    has_unknown = unknown_artist or unknown_album or unknown_title or unknown_year
    update_artist = unknown_artist or update_from_mb
    update_album = unknown_album or update_from_mb
    update_title = unknown_title or update_from_mb
    update_year = unknown_year or update_from_mb
    
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
            logger.info(f"Fetching metadata for \"{file}\" from MusicBrainz")
            mb_metadata, _ = musicbrainz_get_metadata(aidmatch(file, aid_api_key, og_artist=artists, og_title=title))
            # update only the unknown fields
            # Update metadata
            if update_artist:
                mb_artists = mb_metadata.artists_dir
                if mb_artists is not None:
                    logger.info(f"Updating artist for \"{file}\" w/ \"{mb_metadata.artists}\"")
                    artists = mb_artists
                    song['artist'] = artists
                    
            if update_album:
                mb_album = mb_metadata.album
                if mb_album is not None:
                    logger.info(f"Updating album for \"{file}\" w/ \"{mb_metadata.album}\"")
                    song['album'] = mb_album
                    album = mb_album
            
            if update_title:
                mb_title = mb_metadata.title
                if mb_title is not None:
                    logger.info(f"Updating title for \"{file}\" w/ \"{mb_metadata.title}\"")
                    title = mb_title
                    song['title'] = mb_title
                
            if update_year:
                mb_year = mb_metadata.year
                if mb_year is not None:
                    logger.info(f"Updating year for \"{file}\" w/ \"{mb_metadata.year}\"")
                    year = mb_year
                    song['date'] = year
                
        except AIDMatchError as e:
            logger.warning(f"No AcoustID match found for \"{file}\". Cannot update metadata from MusicBrainz.")
        except Exception as e:
            # TODO: Print stacktrace
            logger.warning(f"Could not update metadata for \"{file}\": {traceback.format_exc()}")
    
    metadata = MetaData(year=year, artists=artists, album=album, title=title, filetype=filetype)
           
    return metadata, song

def copy_song(src, dest, overwrite=False, mutagen_file:mutagen.FileType=None, pbar=None):
    if not overwrite and os.path.exists(dest):
        basename = os.path.basename(dest)
        logger.warning(f'File "{basename}" already exists. Skipping copy of "{src}".')
        return
    
     # Ensure the destination directory exists
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    shutil.copy2(src, dest)
    if mutagen_file is not None:
        mutagen_file.save(dest)
    logger.info(f"Copied \"{src}\" to \"{dest}\"")
    if pbar is not None:
        pbar.set_description(f'\nCopied "{src}" to "{dest}".')
    
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
                continue
            
            for file in files:
                
                src_filepath = os.path.join(root, file)
                
                pbar.update()
                
                if save_check_done(src_filepath, save=save):
                    message_skip_procesed(src_filepath)
                    continue
                
                pbar.set_description(f'Processing \"{src_filepath}\".')
                
                if not check_is_music_file(src_filepath):
                    logger.info(f'Skipping non-music file: \"{src_filepath}\"')
                    continue
                
                try:
                    metadata, song = extract_and_update_metadata(src_filepath, aid_api_key=acoustid_api_key, update_from_mb=update_from_mb)
                    if metadata is not None:
                        dest_path = os.path.join(dst, metadata.relativeFilePath)
                    else:
                        # get parent directory of the directory the file is in
                        logger.warning(f'Incomplete or missing metadata for \"{src_filepath}\". Moving to Unknown folder.')
                        
                        parent_dir = os.path.dirname(file)
                        parent_parent_dir = os.path.dirname(parent_dir)
                        parent_dir = os.path.basename(parent_dir)
                        parent_parent_dir = os.path.basename(parent_parent_dir)
                        rel_dir = os.path.join(parent_parent_dir, parent_dir)
                        filename = os.path.basename(file)
                        dest_path = os.path.join(dst, '! Unknown', rel_dir, filename)
                    
                except Exception as e:
                    logger.error(f"Error processing \"{src_filepath}\": {traceback.format_exc()}")
                    continue
                
                try:
                    copy_song(src_filepath, dest_path, overwrite=overwrite, mutagen_file=song, pbar=pbar)
                    
                    # Mark done if we can successfully copy the file
                    save_mark_done(src_filepath, save=save)
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