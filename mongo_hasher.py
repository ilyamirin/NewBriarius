import sys
from pymongo import MongoClient
import json
import hashlib
from pathlib import Path
from bson import json_util
from tqdm import tqdm


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def hashs_exists():
    path = Path(HASH_DIR)

    if not path.exists():
        return False

    if not any(path.iterdir()):
        return False

    return True


def save_json(hashed_field, data_dic):
    file_name = f'{hashed_field[:PREFIX_SIZE]}.{FILE_EXTENSION}'
    path = Path(HASH_DIR)

    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)

    path = path / file_name
    print(path)

    data_dic = [data_dic]
    if path.exists():
        with path.open("r") as f:
            data = f.read()
            file_data = json_util.loads(data)
            data_dic = data_dic + file_data

    with path.open("w") as f:
        json.dump(data_dic, f, default=json_util.default, indent=JSON_INDENT)


def load_json(hashed_field, field):
    file_name = f'{hashed_field[:PREFIX_SIZE]}.{FILE_EXTENSION}'
    path = Path(HASH_DIR, file_name)

    if not path.exists():
        return []

    with path.open("r") as f:
        data = f.read()
        file_data = json_util.loads(data)

    res = []

    for item in file_data:
        if HASHABLE_FIELD not in item: continue
        if item[HASHABLE_FIELD] != field: continue
        res.append(item)

    return res


class TqdmUpTo(tqdm):
    def update_to(self, collection, filter):
        self.total = collection.count_documents(filter)
        return True


def hash_elements_to_dir(hashable_func):
    with MongoClient(CONNECTION) as client:
        db = client[DATABASE]
        collection = db[COLLECTION]
        filter = {HASHABLE_FIELD:{'$ne':None}}
        items = collection.find(filter)

        with TqdmUpTo(collection) as progress:
            progress.update_to(collection, filter)
            for item in items:
                field = item[HASHABLE_FIELD]
                hashed_field = hashable_func(field)
                save_json(hashed_field, item)
                progress.update()


def hashing_field(algorithm):
    h = hashlib.new(algorithm)

    def hashing_string(string):
        htmp = h.copy()
        htmp.update(string.encode())
        res = htmp.hexdigest()
        return res

    return hashing_string


def found_elements(field, hashable_func):
    hashed_field = hashable_func(field)
    elements = load_json(hashed_field, field)

    res = set()

    for element in elements:
        if DISPLAY_FIELD not in element: continue
        res.add(element[DISPLAY_FIELD])

    res = list(res)

    return res


def merge_config(db_config, json_config_file):
    path = Path(json_config_file)

    if path.exists():
        with path.open("r") as f:
            data = f.read()
            json_config = json.loads(data)
            db_config = {**db_config, **json_config}
    else:
        with path.open("w") as f:
            json.dump(db_config, f, indent=db_config['JSON_INDENT'])

    return db_config


def main(db_config):
    for key, value in db_config.items():
        globals()[key] = value

    hash_func = hashing_field(HASH_FUNC)

    if not hashs_exists():
        hash_elements_to_dir(hash_func)

    print(f'Enter {HASHABLE_FIELD} for search {DISPLAY_FIELD}. Press Cntrl+C to exit')

    for line in sys.stdin:
        my_line = line.rstrip('\n')
        element_founder = found_elements(my_line, hash_func)
        print(element_founder)


if __name__ == '__main__':
    config_file = './hasher-config.json'
    db_config = {
        'CONNECTION': 'mongodb://localhost:27017/',
        'DATABASE': 'test_db',
        'COLLECTION': 'users_coll',
        'HASHABLE_FIELD': 'email',
        'DISPLAY_FIELD': 'password',
        'HASH_DIR': './email-hashs/',
        'HASH_FUNC': 'md5',
        'PREFIX_SIZE': 8,
        'FILE_EXTENSION': 'json',
        'JSON_INDENT': 4,
    }

    try:
        arguments = len(sys.argv)

        if arguments == 2:
            config_file = sys.argv[1]
        elif arguments > 2:
            raise ValueError('There should be only one argument: path to the configuration file')

        db_config = merge_config(db_config, config_file)
        main(db_config)
    except KeyboardInterrupt:
        pass
    except Exception as msg:
        eprint(msg)
        sys.exit(1)
