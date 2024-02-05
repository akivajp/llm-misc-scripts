#!/usr/bin/env python3

import csv
import json

from logzero import logger
from tqdm.auto import tqdm

ENCODING_OPTIONS = [
    'utf-8',
    'cp932',
]

EXCLUDE_COLUMNS = [
    'ランダムに並べ替え', # A
    'total_NO', # B
    'data', # C
    'NO', # D
    'len_Q', # H
    'flg_Q', # I
    'comment_Q', # J
    'コメント', # Q
    'len_A', # S
    'flg_A', # T
    'comment_A', # U
    'chat_GPT', # V
    '参考サイト【質問】', # W
    '参考サイト【回答】', # X
    '備考', # Y
    'response', # Z
    'EOLEOLEOLEOL', #AA
    '', # AB?
]

EXCLUDE_FLAGS = [
    '2', '3' , '9', 'R',
]

def convert_tsv2jsonl(
    input_file,
    jsonl_file,
    encoding = 'utf-8',
    tsv = False,
):
    #with open(tsv_file, 'r') as tsvfile:
    if tsv:
        delimiter = '\t'
    else:
        delimiter = ','
    with open(input_file, 'r', encoding=encoding) as tsvfile:
        #reader = csv.DictReader(tsvfile, delimiter='\t')
        reader = csv.DictReader(tsvfile, delimiter=delimiter)
        with open(jsonl_file, 'w') as jsonlfile:
            for i, row in enumerate(
                tqdm(reader),
            ):
                if i == 0:
                    #logger.debug('row: %s', row)
                    logger.debug('row.keys(): %s', row.keys())
                #if i > 1:
                #    break
                flg_q = row['flg_Q']
                flg_a = row['flg_A']
                q = row['書き換えた質問文']
                a = row['作成した回答']
                if not q or not a:
                    continue
                if flg_q in EXCLUDE_FLAGS:
                    continue
                if flg_a in EXCLUDE_FLAGS:
                    continue
                for exclude_column in EXCLUDE_COLUMNS:
                    if exclude_column in row:
                        del row[exclude_column]
                if i == 0:
                    logger.debug('row: %s', row)
                    logger.debug('row.keys(): %s', row.keys())
                #jsonlfile.write(json.dumps(row) + '\n')
                jsonlfile.write(json.dumps(row, ensure_ascii=False) + '\n')

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='Input CSV/TSV file')
    parser.add_argument('jsonl_file', help='Output JSONL file')
    parser.add_argument('--encoding', '-E', default='utf-8', help='Encoding of the TSV file')
    parser.add_argument('--tsv', '-T', action='store_true', help='Input TSV file')
    args = parser.parse_args()
    convert_tsv2jsonl(
        args.input_file,
        args.jsonl_file,
        encoding=args.encoding,
        tsv=args.tsv,
    )
