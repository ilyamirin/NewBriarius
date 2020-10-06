#!/usr/bin/env python

import argparse
import sys
import asyncio
import json
from motor.motor_asyncio import AsyncIOMotorClient
import hashlib
from pathlib import Path
from tqdm.asyncio import tqdm
import shutil

#TODO: Повесить глобальный обработчик ошибок

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

class NotificationError(Exception):
    pass

class LazyDict(dict):
    def get(self, key, thunk=None):
        return (self[key] if key in self else
                thunk() if callable(thunk) else
                thunk)

    def setdefault(self, key, thunk=None):
        return (self[key] if key in self else
                dict.setdefault(self, key,
                                thunk() if callable(thunk) else
                                thunk))

class FileHelper:
    def __init__(self, buffer_size):
        self.handlers = LazyDict()
        self.buffer_size = buffer_size

    class FileGetter: 
        def __init__(self, handlers, buffer_size): 
            self.handlers = handlers
            self.buffer_size = buffer_size

        def get_file(self, file_name): 
            create_file = lambda: open(file_name, 'a', self.buffer_size, newline='', encoding="utf-8")
            return self.handlers.setdefault(file_name, create_file)

    def __enter__(self):
        return self.FileGetter(self.handlers, self.buffer_size)

    def __exit__(self, type, value, traceback):
        for fd in self.handlers.values():
            fd.close()

class Commands:
    def __init__(self, config_dic):
        for key, value in config_dic.items():
            setattr(self, key, value)

        self.hash_func = self.__hashing_field(self.HASH_FUNC)
        self.hash_dir = Path(self.HASH_DIR)

    def __hashing_field(self, algorithm):
        h = hashlib.new(algorithm)

        def hashing_string(string):
            htmp = h.copy()
            htmp.update(string.encode())
            return htmp.hexdigest()

        return hashing_string

    def __save_csv(self, file_getter, hashed_field, data_dic):
        file_name = f'{hashed_field[:self.PREFIX_SIZE]}.csv'
        path = self.hash_dir / file_name

        out = file_getter.get_file(path)

        # TODO: Добавить экранирование
        text = f'{data_dic[self.HASHABLE_FIELD]}{self.CSV_DELIMITER}{data_dic[self.DISPLAY_FIELD]}\n'
        out.write(text)

    def __load_csv(self, hashed_field, field):
        file_name = f'{hashed_field[:self.PREFIX_SIZE]}.csv'
        path = self.hash_dir / file_name

        if not path.exists():
            return None

        with path.open('r', self.FILE_BUFFE_SIZE, newline='', encoding="utf-8") as f:
            for line in f:
                line_arr = line.rstrip('\n').split(self.CSV_DELIMITER)
                #TODO: Тут должна быть запись в лог
                if len(line_arr) != 2: continue
                if line_arr[0] == field:
                    yield line_arr[1]

    async def __fill_dir(self, client, file_getter):
        db = client[self.DATABASE]
        collection = db[self.COLLECTION]
        cursor = collection.find()

        with tqdm(desc='Loading', unit=' records') as progress:
            docs = await cursor.to_list(length=self.CHUNK_SIZE)
            while docs:
                for doc in docs:
                    if self.HASHABLE_FIELD not in doc: continue
                    if self.DISPLAY_FIELD not in doc: continue
                    field = doc[self.HASHABLE_FIELD]
                    hashed_field = self.hash_func(field)
                    self.__save_csv(file_getter, hashed_field, doc)
                    #TODO: Возможно, нужно увеличивать в начале цикла
                    progress.update()
                docs = await cursor.to_list(length=self.CHUNK_SIZE)

    def download_elements(self):
        if not self.hash_dir.exists():
            self.hash_dir.mkdir(parents=True, exist_ok=True)

        with FileHelper(self.FILE_BUFFE_SIZE) as file_getter:
            client = AsyncIOMotorClient(self.CONNECTION)
            loop = asyncio.get_event_loop()
            try:
                loop.run_until_complete(self.__fill_dir(client, file_getter))
            finally:
                loop.close()
                client.close()

        print('Download completed successfully')

    def search_elements(self, get_first=False):
        if not self.hash_dir.exists():
            raise NotificationError('The folder does not exist. There is nothing to look for')

        print(f'Enter {self.HASHABLE_FIELD} for search {self.DISPLAY_FIELD}. Press Cntrl-C to exit')

        for line in sys.stdin:
            search_line = line.rstrip('\n')
            hashed_field = self.hash_func(search_line)
            element_founder = self.__load_csv(hashed_field, search_line)

            found_el = set()
            for element in element_founder:
                found_el.add(element)
                if get_first: break

            res = list(found_el)

            data = {
                self.HASHABLE_FIELD: search_line,
            }

            if len(res) > 0:
                if get_first:
                    res = next(iter(res), None)
                data[self.DISPLAY_FIELD] = res

            print(json.dumps(data, indent=4, ensure_ascii=False))

    def optimize_archives(self):
        if not self.hash_dir.exists():
            raise NotificationError('The folder does not exist. There is nothing to optimize')

        files = list(self.hash_dir.glob('*.csv'))

        for file in tqdm(files, desc='Optimization', unit=' files'):
            completed_lines_hash = set()
            #TODO: Удалять временный файл при ошибке
            tmp_file = self.hash_dir / f'{file.name}.tmp'
            with open(tmp_file, 'w', self.FILE_BUFFE_SIZE, newline='', encoding="utf-8") as output_file,\
                open(file, 'r', self.FILE_BUFFE_SIZE, newline='', encoding="utf-8") as input_file: 
                for line in input_file:
                    hashed_line = self.hash_func(line)
                    if hashed_line not in completed_lines_hash:
                        output_file.write(line)
                        completed_lines_hash.add(hashed_line)

            shutil.move(tmp_file, file)

        print('Optimization completed successfully')

def merge_config(db_config, json_config_file):
    path = Path(json_config_file)

    if path.exists():
        with path.open('r') as f:
            json_config = json.loads(f.read())
            db_config = {**db_config, **json_config}
    else:
        with path.open('w') as f:
            json.dump(db_config, f, indent=4)

    #TODO: Добавить валидацию других полей
    if db_config['PREFIX_SIZE'] not in (1, 2):
        raise NotificationError('Parameter "PREFIX_SIZE" must be one or two')

    if db_config['HASHABLE_FIELD'] == db_config['DISPLAY_FIELD']:
        raise NotificationError('Parameters "HASHABLE_FIELD" and "DISPLAY_FIELD" must be different')

    return db_config

def main(db_config):
    parser = argparse.ArgumentParser(description=
        'A utility for downloading a large number of records from the MongoDB collection\
        to the hard disk into *.csv files. Allows you to search by them.\
        Csv files save two fields: by which to search and the desired one.'
    )
    #TODO: Заюзать os.getenv
    parser.add_argument('-c', '--config', help='path to configuration file', default='hasher-config.json')
    parser.add_argument('cmd', 
        help='command to execute', 
        choices=['download', 'optimize', 'search-all', 'search-one']
    )
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    args = parser.parse_args()

    db_config = merge_config(db_config, args.config)

    cmd = Commands(db_config)
    commands = {
        'download': lambda cmd: cmd.download_elements(),
        'optimize': lambda cmd: cmd.optimize_archives(),
        'search-all': lambda cmd: cmd.search_elements(),
        'search-one': lambda cmd: cmd.search_elements(get_first=True),
    }[args.cmd](cmd)

if __name__ == '__main__':
    db_config = {
        'CONNECTION': 'mongodb://localhost:27017/',
        'DATABASE': 'test_db',
        'COLLECTION': 'users_coll',
        'HASHABLE_FIELD': 'email',
        'DISPLAY_FIELD': 'password',
        'HASH_DIR': './email-hashs/',
        'HASH_FUNC': 'md5',
        'PREFIX_SIZE': 2,
        'CHUNK_SIZE': 10000,
        'FILE_BUFFE_SIZE': 1048576,
        'CSV_DELIMITER': ';',
    }

    try:
        main(db_config)
    except KeyboardInterrupt:
        print('Canceled by user')
        pass
    except NotificationError as msg:
        # TODO: Добавить логирование
        eprint(f'ERROR: {msg}')
        sys.exit(1)