#/usr/bin/python3

# Find MP3 files on SD Card
# Organize them somewhat

import os
import datetime
from glob import glob
import taglib

sdcard = "/media/removable/SD Card"

mp3s = {}
mp3list = []
missing = []

def add_folder(folder):
    """This one is not recursive"""
    mp3s[folder] = glob(os.path.join(sdcard, folder, "*.mp3"))


def mp3gen(path):
    """Finds all mp3 and mp4s recursively"""
    for root, dirs, files in os.walk(path):
        for filename in files:
            if os.path.splitext(filename)[1] in [".mp3", ".m4a"]:
                yield os.path.join(root, filename)

for mp3file in mp3gen(sdcard):
    mp3list.append(mp3file)

# Search strings for find_songs
pl2013 = ['Wake Me Up', 'Pompeii', 'Unbelievers', 'Close to Me', 'Purple Yellow', 'Recover', 'Instant Crush', 'Hurricane', 'Paper Doll', 'Reflektor', 'Feel Good']
pl2014 = ['Cherub', 'Big Data', 'Aviation High', 'Riptide', 'Stolen Dance']
pl2015 = ['Budapest', 'Kansas City', 'New Dorp', 'First', 'Shots', 'Wanna Fight', 'My Type', 'Standing Outside', 'Do I Wanna Know (acoustic)', 'Sedona', 'Peaches']
pl2016 = ['']


def find_songs(playlist, mp3list):
    """Search mp3list for filenames containing a list of strings"""
    global missing # get all the missing songs at once
    playlistfiles = []
    for p in playlist:
        for m in mp3list:
            if p in m:
                playlistfiles.append(m)
                break
        else:
            missing.append(p)
    return playlistfiles

# files list can go to path_swap() or straight to write_playlist()
files2013 = find_songs(pl2013, mp3list)

def find_recent(file, delta):
    """Return files from list that have been modified in #delta days"""
    filelist = []
    d = datetime.datetime.now()
    e = datetime.datetime.utcfromtimestamp(0)
    until = (d - e - datetime.timedelta(days=delta)).total_seconds()
    if os.path.getmtime(file) > until:
        filelist.append(file)
    return filelist


def find_unique(mp3list, tagname='ARTIST'):
    """Returns list of file paths that have a unique tag, ie, only song from an artist
    Second part of tuple returns songs missing the tag
    """
    missing = []
    items = []
    tags = []
    unique = []
    for m in mp3list:
        t = taglib.File(m).tags
        if tagname in t.keys():
            tags.append(t[tagname])
            items.append((t[tagname],m))
        else:
            missing.append(m)
    for tag in tags:
        if tags.count(tag) == 1:
            for i in items:
                if i[0] == tag:
                    unique.append(i)
    return unique,missing

# This is just getting the filenames and making a playlist from them
sp = [i[1] for i in find_unique(mp3list)[0]]
files2016 = [
    sp[23],
    sp[24],
    sp[25],
    sp[26],
    sp[13],
    sp[0],
    sp[17],
    sp[19],
    sp[18]
]

def trim_list(small, big):
    """Remove sub list from list"""
    for s in small:
        if s in big:
            big.remove(s)
    return big

def write_playlist(playlist, filename):
    with open(os.path.join(sdcard, 'Music.0', filename + '.m3u'), 'w') as f:
        data = '\n'.join(playlist)
        f.write(data)
        print(data)

def open_playlist(playlist):
    with open(playlist, 'r') as f:
        return f.readlines()

def swap_paths(playlist):
    a = '/media/removable/SD Card' # ubuntu
    b = '/storage/extSdCard'       # android
    for i in range(0,2):
        if a in playlist[0]:
            new, old = b, a
            break
        elif b in playlist[0]:
            new, old = a, b
            break
    else:
        print('path not found')
        return
    for i in playlist:
        yield i.replace(old,new)

def pl(l):
    """Quick list printer for testing"""
    [print(i) for i in l]