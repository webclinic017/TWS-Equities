import json
from os import listdir
from os.path import dirname
from os.path import join


_POSITIVE_INPUT_PATH = join(dirname(__file__), 'positive')
_NEGATIVE_INPUT_PATH = join(dirname(__file__), 'negative')


def load_json_file(file_path):
    with open(file_path, 'r') as f:
        return json.loads(f.read())


def get_input(target_directory):
    input_data = {}
    target_files = list(filter(lambda x: x.endswith('.json'), listdir(target_directory)))
    for file in target_files:
        ticker_id = int(file.split('.')[0])
        input_data[ticker_id] = load_json_file(join(target_directory, file))
    return input_data


def get_positive_input():
    return get_input(_POSITIVE_INPUT_PATH)


def _test():
    positive_data = get_input('positive')
    print(positive_data)
    negative_data = get_input('negative')
    print(negative_data)


if __name__ == '__main__':
    _test()
