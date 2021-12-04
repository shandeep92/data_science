import datetime
import os
import fake_useragent
import re
import unidecode


def get_datetime_str(up_to='second') -> str:
    """
    up_to: second, minute, hour, day
    """
    if   up_to == 'second':
        s = str(datetime.datetime.now())[0:19]
    elif up_to == 'minute':
        s = str(datetime.datetime.now())[0:16]
    elif up_to == 'hour':
        s = str(datetime.datetime.now())[0:13]
    elif up_to == 'day':
        s = str(datetime.datetime.now())[0:10]
    else:
        raise Exception('no valid value')
    s = s.replace('-', '').replace(' ', '_').replace(':', '')
    return s


def get_user_agent():
    user_agent = fake_useragent.UserAgent(use_cache_server=False)
    return user_agent.ie


def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def is_notebook():
    "check if running inside a notebook"
    # try:
    #     from google import colab
    #     return True
    # except:
    #     pass
    try:
        shell = get_ipython().__class__.__name__
        if   shell == 'ZMQInteractiveShell':
            return True   # Jupyter notebook, Spyder or qtconsole
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


def convert_bytes(num) -> str:
    "return human-readable string with MB, GB, etc"
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.2f %s" % (num, x)
        num /= 1024.0


def get_file_size(file_path) -> str:
    "return the size of a local file as formatted string"
    if os.path.isfile(file_path):
        file_info = os.stat(file_path)
        return convert_bytes(file_info.st_size)


def replace_unnecessary_characters(name: str) -> str:
    normalize_pattern = re.compile('[^a-zA-Z0-9. ]', re.UNICODE)

    replace_to_empty = ['-', ',']

    if name is None:
        return None

    else:
        # lower case
        name = name.lower()

        # replace characters/words with empty string
        for replace in replace_to_empty:
            name = name.replace(replace, '')

        # removes accents and special letters like ü ö ä ß é à
        name = unidecode.unidecode(name)

        # remove non-alphanumerical characters
        name = normalize_pattern.sub('', name)

        # remove duplicate words, sort words
        name = " ".join(sorted(list(set((name.split(" "))))))

        # remove leading and trailing whitespaces
        name = name.strip()

        if name.endswith(' x'):
            name = name[:-2]

        return name


def convert_bytes(num) -> str:
    "return human-readable string with MB, GB, etc"
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.2f %s" % (num, x)
        num /= 1024.0
