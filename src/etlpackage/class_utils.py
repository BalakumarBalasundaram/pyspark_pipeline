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


def create_partitions_from_file(base_dir, part_keys, current_year=None, current_month=None, current_day=None):
    # part_keys="ing_year=YYYY:ing_month=MM:ing_day=DD"
    partition_values = {}
    partitons = part_keys.split(":")
    # len=len(xx)
    partition_k = base_dir
    for i in partitons:
        # print(i)
        if i == 'ing_year=YYYY':
            partition_k = partition_k + '/ing_year=' + current_year
            partition_values['ing_year'] = current_year
            if not file_exists(partition_k):
                print("Creating directory ", partition_k, "now")
                # dbutils.fs.mkdirs(partition_k)
        elif i == 'ing_month=MM':
            partition_k = partition_k + '/ing_month=' + current_month
            partition_values['ing_month'] = current_month
            if not file_exists(partition_k):
                print("Creating directory ", partition_k, "now")
                # dbutils.fs.mkdirs(partition_k)
        elif i == 'ing_day=DD':
            partition_k = partition_k + '/ing_day=' + current_day
            partition_values['ing_day'] = current_day
            if not file_exists(partition_k):
                print("Creating directory ", partition_k, "now")
                # dbutils.fs.mkdirs(partition_k)
    print(partition_k)
    print(partition_values)
    return partition_k, partition_values


def create_partions_with_current_date(base_dir, part_keys, current_year=None, current_month=None, current_day=None):
    # part_keys="ing_year=YYYY:ing_month=MM:ing_day=DD"
    partition_values = {}
    partitions = part_keys.split(":")
    # len=len(xx)
    partition_k = base_dir
    for i in partitions:
        # print(i)
        if i == 'ing_year=YYYY':
            partition_k = partition_k + '/ing_year=' + current_year
            partition_values['ing_year'] = current_year
            if not file_exists(partition_k):
                print("Creating directory ", partition_k, "now")
                # dbutils.fs.mkdirs(partition_k)
        elif i == 'ing_month=MM':
            partition_k = partition_k + '/ing_month=' + current_month
            partition_values['ing_month'] = current_month
            if not file_exists(partition_k):
                print("Creating directory ", partition_k, "now")
                # dbutils.fs.mkdirs(partition_k)
        elif i == 'ing_day=DD':
            partition_k = partition_k + '/ing_day=' + current_day
            partition_values['ing_day'] = current_day
            if not file_exists(partition_k):
                print("Creating directory ", partition_k, "now")
                # dbutils.fs.mkdirs(partition_k)
    print(partition_k)
    print(partition_values)
    return partition_k, partition_values
