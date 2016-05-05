# vim: fileencoding=utf-8

import datetime
import hashlib
import itertools
import pickle
import json
import time
import os


CACHE_FILE_NAME = 'copykill2.cache'


class FileData:
    """Store data about files here
    """

    def __init__(self, *, path=None, name=None, stat=None):
        assert path
        assert name
        assert stat

        self.path = path
        self.name = name
        self.stat = stat

        self._sha256sum = None

    def __repr__(self):
        return '{} {}/{}'.format(self.size, self.path, self.name)

    def __str__(self):
        return '{}'.format(repr(self))

    @property
    def sha256sum(self):
        return self.calc_hash()

    @property
    def mtime(self):
        """mtime in nanosecs
        """

        # d = time.mktime(time.gmtime(self.stat.st_mtime_ns))
        d = time.mktime(time.gmtime(self.stat.st_mtime))
        d = datetime.datetime.fromtimestamp(d)

        return d.strftime('%Y-%m-%dT%H:%M:%S.%f')

    @property
    def filepath(self):
        return os.path.join(self.path, self.name)

    @property
    def size(self):
        """stat size, bytes
        """

        return self.stat.st_size

    def as_dict(self):
        """Get a dictionary of us
        """

        return {
            'path': self.path,
            'name': self.name,
            'mtime': self.mtime,
            'size': self.size,
            'sha256sum': self.sha256sum,
        }

    def calc_hash(self, force=False):
        """If hash is calced and force is False, return what we have
        """

        if self._sha256sum is not None and not force:
            return self._sha256sum

        with open(self.filepath, 'rb') as f:
            self._sha256sum = hashlib.sha256(f.read()).hexdigest()

        return self._sha256sum


def file_datas_for(path, refresh=True):
    """Create a cache to make development faster
    """

    cache_path = os.path.join(path, CACHE_FILE_NAME)

    if os.path.exists(cache_path) and not refresh:
        with open(cache_path, 'rb') as cache_file:
            try:
                return pickle.load(cache_file)
            except EOFError:
                pass

    path = os.path.realpath(path)

    files = {}

    for dirpath, dirnames, filenames in os.walk(path):
        for name in filenames:
            file_path = os.path.join(dirpath, name)
            if os.path.isfile(file_path):
                stat = os.stat(file_path)

                filedata = FileData(path=dirpath, name=name, stat=stat)

                # files[filedata.sha256sum] = filedata
                files.setdefault(filedata.size, []).append(filedata)

    with open(cache_path, 'wb') as cache_file:
        pickle.dump(files, cache_file)

    return files


def check_duplicates(files):
    dups = []

    for size, filelist in files.items():
        if len(filelist) > 1:
            for hash, filelist in itertools.groupby(sorted(filelist, key=lambda f: f.sha256sum), lambda f: f.sha256sum):
                filelist = list(filelist)
                if len(filelist) > 1:
                    dups.append(filelist)

    return dups


def print_duplicates(dups):
    for duplist in dups:
        for dup in duplist:
            print(dup.filepath)
        print('')


def cleanup(preserve_dir, dups):
    assert os.path.isdir(preserve_dir)

    report = {
        'by_hash': {
            # `hash`: {
            #    'killed': []
            #    'preserved': ''
            # }
        }
    }

    report_name = 'copykill2.report.{}'.format(datetime.datetime.now().date().isoformat())
    report_path = os.path.join(preserve_dir, report_name)
    if os.path.exists(report_path):
        i = 0
        report_name_raw = report_name + '.{}'
        report_name = report_name_raw.format(i)
        while os.path.exists(report_path):
            i += 1
            report_name = report_name_raw.format(i)
            report_path = os.path.join(preserve_dir, report_name)

    for duplist in dups:
        to_preserve = [d for d in duplist if not d.path.startswith(preserve_dir)]
        if len(to_preserve) > 1:
            to_preserve = sorted(to_preserve, key=lambda f: f.mtime)[0]
        else:
            to_preserve = to_preserve[0]

        to_kill = [d for d in duplist if not d.path.startswith(preserve_dir)]

        hashdict = {
            to_preserve.sha256sum: {
                'preserved': to_preserve.as_dict(),
                'killed': [f.as_dict() for f in to_kill],
            }
        }

        report['by_hash'].update(hashdict)

    with open(report_path, 'wb') as report_file:
        report_json = json.dumps(report, indent=4)
        report_file.write(bytes(report_json, encoding='utf-8'))
        report_file.write(b'\n')

