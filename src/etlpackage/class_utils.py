import os
import re


def db_list_files(file_path, file_prefix, dbutils=None):
    file_list = [file.path for file in dbutils.fs.ls(file_path) if re.search(file_prefix, os.path.basename(file.path))]
    return file_list


def file_exists(path, dbutils):
    try:
        dbutils.fs.ls(path)
        print("The path ", path, " exists")
        pass
        return True
    except:
        print("The path ", path, " does not exist")
        return False
