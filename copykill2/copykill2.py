# vim: fileencoding=utf-8

import concurrent.futures

import datetime
import hashlib
import itertools
import pickle
import json
import time
import os


CACHE_FILE_NAME = 'copykill2.cache'
CPU_COUNT = os.sysconf('SC_NPROCESSORS_ONLN')
MAX_WORKERS = CPU_COUNT - 2


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

    def exists(self):
        return os.path.exists(self.filepath)

    def calc_hash(self, force=False):
        """If hash is calced and force is False, return what we have
        """

        if self._sha256sum is not None and not force:
            return self._sha256sum

        # Something may have removed this file
        if not self.exists():
            return None

        # print('[{}] Calculating hash for {}'.format(os.getpid(), self.filepath), flush=True)
        with open(self.filepath, 'rb') as f:
            hash_ = hashlib.sha256()
            while True:
                chunk = f.read(128 * 1024 * 1024)
                if not chunk:
                    break
                hash_.update(chunk)

        self._sha256sum = hash_.hexdigest()
        # print('[{}] \t Done {}'.format(os.getpid(), self.filepath), flush=True)

        return self._sha256sum


def file_datas_for(path, refresh=False):
    """Create a cache to make development faster
    """

    cache_path = os.path.join(path, CACHE_FILE_NAME)

    if os.path.exists(cache_path) and not refresh:
        print('Loading cache')
        with open(cache_path, 'rb') as cache_file:
            try:
                files = pickle.load(cache_file)
                print('Loaded cache {}'.format(cache_file))
                return files
            except EOFError as e:
                print('Caught and ignored {}'.format(e))

    path = os.path.realpath(path)

    files = {}

    i = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for name in filenames:
            file_path = os.path.join(dirpath, name)
            if os.path.isfile(file_path):
                i += 1
                stat = os.stat(file_path)

                filedata = FileData(path=dirpath, name=name, stat=stat)

                # files[filedata.sha256sum] = filedata
                files.setdefault(filedata.size, []).append(filedata)

                if i % 100 == 0:
                    print(end=".", flush=True)
    try:
        with open(cache_path, 'wb') as cache_file:
            pickle.dump(files, cache_file)
    except PermissionError:
        print('Creating cache failed, whatever')

    print('Got {} file sizes'.format(len(files.keys())))
    return files


def sort_filedata(f):
    return f.sha256sum or ''


def check_duplicate_filelist(filelist):
    print('[{}] Looking at {} files'.format(os.getpid(), len(filelist)), flush=True)
    for hash, dup_filelist in itertools.groupby(sorted(filelist, key=sort_filedata), sort_filedata):
        print('[{}] {}'.format(os.getpid(), hash), end=' ', flush=True)
        dup_filelist = list(dup_filelist)
        if len(dup_filelist) > 1:
            print('\t{} duplicates'.format(len(dup_filelist)), flush=True)
        else:
            print('\tonly one', flush=True)

    return dup_filelist


def check_duplicates(files):
    dups = []
    futures = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for size, filelist in files.items():
            if len(filelist) > 1:
                future = executor.submit(
                    check_duplicate_filelist,
                    filelist,
                )
                future.add_done_callback(lambda fut: dups.append(fut.result()))
                futures.append(future)

        print('[{}] Waiting on {} futures'.format(os.getpid(), len(futures)))
        print(concurrent.futures.wait(futures))
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
        },
        'preserved_count': 0,
        'killed_count': 0,
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

    len_dups = len(dups)
    print('Going through {} duplicates'.format(len(dups)))
    for i, duplist in enumerate(dups):
        to_preserve = [d for d in duplist if not d.path.startswith(preserve_dir)]
        if len(to_preserve) > 1:
            to_preserve = sorted(to_preserve, key=lambda f: f.mtime)[0]
        else:
            to_preserve = to_preserve[0]

        to_kill = [d for d in duplist if not d.path.startswith(preserve_dir) and d.exists()]

        print('[{}/{}] {} {} to kill'.format(i + 1, len_dups, to_preserve.sha256sum, len(duplist)))

        hashdict = {
            to_preserve.sha256sum: {
                'preserved': to_preserve.as_dict(),
                'killed': [f.as_dict() for f in to_kill],
            }
        }

        report['by_hash'].update(hashdict)
        report['preserved_count'] += 1
        report['killed_count'] += len(to_kill)

    print('Writing report to {}'.format(report_path))
    with open(report_path, 'wb') as report_file:
        report_json = json.dumps(report, indent=4)
        report_file.write(bytes(report_json, encoding='utf-8'))
        report_file.write(b'\n')

