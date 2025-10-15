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


import os 
import shutil
import logging 
import time 
import toml
import argparse 
import sys 
import tqdm
import music 
import acoustid 
import json 
import io 

# TODO:
# Get all metadata available for a piece of music
# Update missing metadata using AcoustID and MusicBrainz (1/2 done)
# Organize music into artist/album/track

# Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
music.logger.setLevel(logging.INFO)

# Set standard output handler
formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(message)s')
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(formatter)
logger.addHandler(stdout_handler)

# set rate limit for acoustid to 3 requests per second
DEFAULT_CONFIG = 'config.conf'
APP_NAME = 'PythonMusicOrganizer'
APP_DESCRIPT = 'Organize music files based on metadata'
APP_VERSION = "0.1"
CONFIG_HELP = f'Path to configuration file (default: {DEFAULT_CONFIG})'

parser = argparse.ArgumentParser(prog=APP_NAME, description=APP_DESCRIPT, )
parser.add_argument('-config', type=str, default=DEFAULT_CONFIG, help=CONFIG_HELP, nargs=1, required=True)

# Sources can be a list of directories
# parser.add_argument('-src', help="Source directory containing music files to organize.", nargs='+', required=True)
# parser.add_argument('-dst', type=str, help="Destination directory to move organized music files to.", nargs=1, required=True)

def set_logger(log_path):
    """Set up the logger to log to a file."""
    global logger
    handler = logging.FileHandler(log_path)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
def set_log_level(log_level=None):
    """Set the logging level."""
    global level, logger
    
    # Sanitize log level input
    if not log_level in [None, 'logging.CRITICAL', 'logging.ERROR', 'logging.WARNING', 'logging.INFO', 'logging.DEBUG']:
        raise ValueError(f"Invalid log level: {log_level}")
    
    level = eval(log_level)
    logger.setLevel(level)
    music.logger.setLevel(level)
    
def load_config(config_file):
    """Load configuration from a TOML file."""
    try:
        with open(config_file, 'r') as f:
            config = toml.load(f)
        return config
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        sys.exit(1)

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

def main(config):
    global AID_API_KEY, level
    
    
    # Load configuration
    srcs = config['paths']
    dest = config['dest']
    AID_API_KEY = config.get('acoustid_api_key', None) # Get API key from https://acoustid.org/api-key, need an application ID
    contact = config.get('email_contact', None) 
    update_from_mb = config['update_from_mb']
    fpcalc_path = config.get('fpcalc_path', 'fpcalc')  # Default to 'fpcalc' if not specified
    overwrite = config.get('overwrite', False)
    log_path = config.get('log_path', None)
    log_level = config.get('log_level', 'logging.INFO')
    save_file = config.get('save_file', None)
    
    save = None
    
    
    print(log_level)
    
    if save_file is not None:
        save = music.SaveState.load_save(save_file)
    
    if log_path is not None:
        set_logger(log_path)
        
    if log_level is not None:
        set_log_level(log_level)
    else:
        
        print('HERE')
        set_log_level('logging.WARNING')
        
    # Set the environment variable for fpcalc
    os.environ[acoustid.FPCALC_ENVVAR] = fpcalc_path
    music.start_musicbrainz(APP_NAME, APP_VERSION, contact)
    music.process_paths(
        srcs, 
        dest, 
        overwrite=overwrite, 
        acoustid_api_key=AID_API_KEY, 
        update_from_mb=update_from_mb,
        save=save)
        
if __name__ == '__main__':
    args = vars(parser.parse_args())
    config_file = args['config']
    config = toml.load(config_file)
    main(config)
    
    