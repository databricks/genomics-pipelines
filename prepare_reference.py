#!/usr/bin/env python

from __future__ import print_function
import argparse
import os.path as p
import os
import subprocess
import sys

BWA_INDEX_EXTENSIONS = ['.amb', '.ann', '.bwt', '.pac', '.sa']

BWA_INDEX_IMAGE_FILE = '.img'

FASTA_INDEX_FILE = '.fai'

DICT_FILE = '.dict'

FASTA_EXTENSIONS = ['.fa', '.fasta']

REFERENCE_DIRECTORY = '/mnt/dbnucleus/dbgenomics/reference-genome'


def dict_path_from_fasta(fasta_path):
    root, ext = p.splitext(fasta_path)
    return root + DICT_FILE


def check_input_path(fasta_path):
    assert p.isfile(fasta_path), 'Reference genome file does not exist'
    is_fasta = any(fasta_path.endswith(ext) for ext in FASTA_EXTENSIONS)
    assert is_fasta, 'Reference genome file does not have a valid fasta extension: %s' % FASTA_EXTENSIONS


def check_dict_file(fasta_path):
    dict_exists = p.isfile(dict_path_from_fasta(fasta_path))
    assert dict_exists, 'Missing .dict file'


def check_fasta_index(fasta_path):
    fai_exists = p.isfile(fasta_path + FASTA_INDEX_FILE)
    assert fai_exists, 'Missing .fai index file'


def bwa_img_exists(fasta_path):
    return p.isfile(fasta_path + BWA_INDEX_IMAGE_FILE)


def check_bwa_index(fasta_path):
    if bwa_img_exists(fasta_path):
        return  # all good
    elif all(p.isfile(fasta_path + ext) for ext in BWA_INDEX_EXTENSIONS):
        return  # all good
    else:
        raise ValueError('Missing BWA-MEM index files. Either a .img index or %s must be present' %
                         BWA_INDEX_EXTENSIONS)


def prepare_bwa_reference(fasta_path):
    if p.isfile(fasta_path + BWA_INDEX_IMAGE_FILE):
        return  # already have an image file

    subprocess.check_call([
        'java', '-cp', '/databricks/jars/*',
        'com.databricks.hls.pipeline.dnaseq.alignment.GenerateBwaIndexImage', fasta_path,
        fasta_path + BWA_INDEX_IMAGE_FILE
    ])


def copy_reference_files(fasta_path, dest_dir):
    subprocess.check_call('cp %s %s' % (fasta_path + '*', dest_dir), shell=True)
    subprocess.check_call('cp %s %s' % (dict_path_from_fasta(fasta_path), dest_dir), shell=True)


def main():
    parser = argparse.ArgumentParser(description='Set up a custom reference genome')
    parser.add_argument('fasta_path', help='The DBFS path for the reference genome fasta file')
    args = parser.parse_args()
    dbfs_fasta_path = '/dbfs' + args.fasta_path
    print('Setting up reference genome from fasta file %s' % dbfs_fasta_path, file=sys.stderr)
    fasta_file_name = p.split(args.fasta_path)[1]
    check_input_path(dbfs_fasta_path)
    check_dict_file(dbfs_fasta_path)
    check_fasta_index(dbfs_fasta_path)
    check_bwa_index(dbfs_fasta_path)

    print('Copying reference genome files to local filesystem...', file=sys.stderr)
    copy_reference_files(dbfs_fasta_path, REFERENCE_DIRECTORY)
    print('Creating index image file if necessary...', file=sys.stderr)
    prepare_bwa_reference(p.join(REFERENCE_DIRECTORY, fasta_file_name))


if __name__ == '__main__':
    main()
