#!/usr/bin/env python3

#import csv
import json
import os.path
import unicodedata

import pandas as pd

from logzero import logger
from tqdm.auto import tqdm

EXCLUDE_FLAGS = [
    '2', '3' , '9', 'R',
]

def normalize(text):
    if type(text) == str:
        return unicodedata.normalize('NFKC', text)
    return text

map_id_pair_to_row = dict()

def convert_excel2jsonl(
    input_file,
    #jsonl_file,
    id_prefix,
    index_length,
):
    df = pd.read_excel(input_file)
    rows = []
    jsonl_file = os.path.splitext(input_file)[0] + '.jsonl'
    #logger.debug('# df: %s', df)
    #id_pairs = []
    #map_id_pair_to_row = dict()
    with open(jsonl_file, 'w', encoding='utf-8') as jsonlfile:
        for i, row in tqdm(
            df.iterrows(),
            total=len(df),
            desc=f'Converting {input_file} to {jsonl_file}'
        ):
            #if i == 0:
            #    logger.debug('row: %s', row)
            #    logger.debug('row.keys(): %s', row.keys())
            #if i >= 1:
            #    break
            #q_id = row['QuestionID']
            #q_id = row['1.4ランダムに並べ替え']
            if 'ランダムに並べ替え' in row:
                q_id = row['ランダムに並べ替え']
            elif '1.4ランダムに並べ替え' in row:
                q_id = row['1.4ランダムに並べ替え']
            elif 'ランダムに並べ替え2.6' in row:
                q_id = row['ランダムに並べ替え2.6']
            else:
                logger.error('row:\n%s', row)
                raise ValueError('No Question ID')
            #elif 'total_NO' in row:
            #    q_id = row['total_NO']
            #q_id = row['NO']
            a_id = 1
            #a_id = row['AnswerID']
            try:
                q = row['書き換えた質問文']
                a = row['作成した回答']
                flg_q = row['flg_Q']
                flg_a = row['flg_A']
                task = row['操作']
                prospective = row['主観']
                time_dependency = row['時間依存']
                domain = row['分野']
                #source_to_answer = row['対象']
                if '対象' in row:
                    source_to_answer = row['対象']
                elif '能力' in row:
                    source_to_answer = row['能力']
                else:
                    raise ValueError('No 対象 or 能力')
                output_type = row['回答タイプ']
                text_producer = row['質問作成者']
                output_producer = row['回答作成者']
                if '参考サイト【回答】' in row:
                    answer_reference = row['参考サイト【回答】']
                elif '参照サイト（回答）' in row:
                    answer_reference = row['参照サイト（回答）']
                else:
                    raise ValueError('No 参考サイト【回答】 or 参照サイト（回答）')
                if type(answer_reference) == str:
                    answer_reference = answer_reference.strip().split('\n')
                    answer_reference = [x for x in answer_reference if x]
            except KeyError as e:
                logger.error('row:\n%s', row)
                raise e
            if not q or not a:
                continue
            if flg_q in EXCLUDE_FLAGS:
                continue
            if flg_a in EXCLUDE_FLAGS:
                continue
            #id = 'answercarefully-instruction-000-001-0000001-001'
            #id = f'answercarefully-instruction-000-001-0000001-{q_id:03d}'
            d = dict(
                ID=q_id,
                text=q,
                output=a,
                meta = {
                    'task': task,
                    'prospective': prospective,
                    'time-dependency': time_dependency,
                    'domain': domain,
                    'source-to-answer': source_to_answer,
                    'output-type': output_type,
                    'text-producer': text_producer,
                    'output-producer': output_producer,
                    'output-reference': answer_reference,
                },
            )
            #id_pairs.append((q_id, a_id))
            #logger.debug('d: %s', d)
            #jsonlfile.write(json.dumps(d, ensure_ascii=False) + '\n')
            rows.append(d)
            if (q_id, a_id) in map_id_pair_to_row:
                logger.error('duplicated: %s', map_id_pair_to_row[(q_id, a_id)])
                raise ValueError(f'Duplicate ID: {(q_id, a_id)}')
            map_id_pair_to_row[(q_id, a_id)] = d
        max_index_length = len(str(len(rows)))
        if index_length < max_index_length:
            logger.error('index_length must be greater than or equal to %d', max_index_length)
            return
        for i, row in enumerate(
            tqdm(rows),
        ):
            str_index = str(int(row['ID'])).zfill(index_length)
            id = f'{id_prefix}-{str_index}-001'
            row['ID'] = id
            jsonlfile.write(json.dumps(row, ensure_ascii=False) + '\n')
        #id_pairs.sort()
        #logger.debug('id_pairs: %s', id_pairs)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    #parser.add_argument('input_file', help='Input Excel file')
    parser.add_argument('input_files', nargs='+', help='Input Excel files')
    #parser.add_argument('jsonl_file', help='Output JSONL file')
    parser.add_argument('--id-prefix', '-P', required=True, help='ID prefix')
    #parser.add_argument('--index-length', '-L', type=int, required=True, help='Index Length')
    parser.add_argument('--index-length', '-L', type=int, default=7, help='Index Length')
    args = parser.parse_args()
    logger.debug('args: %s', args)
    for input_file in args.input_files:
        convert_excel2jsonl(
            input_file,
            #args.jsonl_file,
            id_prefix=args.id_prefix,
            index_length=args.index_length,
        )
