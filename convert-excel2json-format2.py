#!/usr/bin/env python3

import json
import math
import os.path
import unicodedata

import pandas as pd

from logzero import logger
from tqdm.auto import tqdm

META_SPLIT_DELIM = ';'
META_SPLIT_FIELDS = [
#    'risk-area',
#    'harm-type',
#    'specific-harm',
]

META_MAP = {
    'risk-area': [
        'risk-area',
    ],
    'harm-type': [
        'harm-type',
    ],
    'specific-harm': [
        'specific-harm',
    ],
}

def normalize(value):
    '''
    文字列の正規化
    '''
    if isinstance(value, str):
        value = value.strip()
        return unicodedata.normalize('NFKC', value)
    if isinstance(value, float) and math.isnan(value):
        return None
    return value

def get_meta_data(
    row: pd.Series,
):
    '''
    メタデータの取得
    '''
    meta = {}
    for key, row_keys in META_MAP.items():
        found = False
        for row_key in row_keys:
            if row_key in row:
                value = row[row_key]
                if isinstance(value,float) and math.isnan(value):
                    if key in META_SPLIT_FIELDS:
                        value = ''
                    elif key == 'output-reference':
                        value = ''
                    else:
                        value = None
                value = normalize(value)
                if key in META_SPLIT_FIELDS:
                    value = value.split(META_SPLIT_DELIM)
                    #try:
                    #    value = value.split(META_SPLIT_DELIM)
                    #except Exception as e:
                    #    logger.error('row-key: %s', row_key)
                    #    logger.error('key: %s', key)
                    #    logger.error('key in META_SPLIT_FIELDS: %s', key in META_SPLIT_FIELDS)
                    #    logger.error('value: %s', value)
                    #    raise e
                if isinstance(value, list):
                    value = [normalize(x) for x in value]
                meta[key] = value
                found = True
                break
        if not found:
            raise ValueError(f'No {key}')
    return meta

map_question_to_id = {}
stat = {
    'max_question_id': 0,
    'num_questions': 0,
}

def convert_excel2json(
    input_path,
    id_prefix,
    question_index_length,
    answer_index_length,
    json_lines,
    indent: int,
):
    '''
    Excelファイルを読み込んでJSONL形式で出力
    '''
    try:
        df = pd.read_excel(input_path)
    except Exception as e:
        logger.error('input_path: %s', input_path)
        raise e
    rows = []
    if json_lines:
        output_file = os.path.splitext(input_path)[0] + '.jsonl'
    else:
        output_file = os.path.splitext(input_path)[0] + '.json'
    #logger.debug('# df: %s', df)
    with open(output_file, 'w', encoding='utf-8') as f_output:
        for i, row in tqdm(
            df.iterrows(),
            total=len(df),
            desc=f'Converting {input_path} to {output_file}'
        ):
            #if i == 0:
            #    logger.debug('row: %s', row)
            #    logger.debug('row.keys(): %s', row.keys())
            #if i >= 1:
            #    break
            try:
                q = row['質問']
                q = normalize(q)
                a = row['回答']
                a = normalize(a)
                meta = get_meta_data(row)
                if not q or not a:
                    continue
                q_id = int(row['QuestionID'])
                map_question_to_id[q] = q_id
                if q_id > stat['max_question_id']:
                    stat['max_question_id'] = q_id
                a_id = 1
            except Exception as e:
                logger.error('row:\n%s', row)
                raise e
            d = dict(
                ID=None,
                q_id=q_id,
                a_id=a_id,
                text=q,
                output=a,
                meta=meta,
                file=input_path,
            )
            rows.append(d)
        min_question_index_length = len(str(stat['num_questions']))
        min_answer_index_length = 1
        if question_index_length < min_question_index_length:
            raise ValueError(f'question_index_length: {question_index_length}')
        if answer_index_length < min_answer_index_length:
            raise ValueError(f'answer_index_length: {answer_index_length}')
        output_rows = []
        for i, row in enumerate(
            tqdm(rows),
        ):
            q_id = row.pop('q_id')
            a_id = row.pop('a_id')
            str_question_index = str(q_id).zfill(question_index_length)
            str_answer_index = str(a_id).zfill(answer_index_length)
            row['ID'] = f'{id_prefix}-{str_question_index}-{str_answer_index}'
            # デバッグ用フィールドを削除
            del row['file']
            output_rows.append(row)
        if json_lines:
            for i, row in enumerate(
                tqdm(output_rows),
            ):
                f_output.write(json.dumps(row, ensure_ascii=False) + '\n'
            )
        else:
            f_output.write(
                json.dumps(output_rows, ensure_ascii=False, indent=indent)
            )

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('input_files', nargs='+', help='Input Excel files')
    parser.add_argument('--id-prefix', '-P', required=True, help='ID prefix')
    parser.add_argument(
        '--json-lines', '--lines', '-L',
        action='store_true',
        help='Output JSON Lines file'
    )
    parser.add_argument(
        '--question-index-length', '-Q',
        type=int, default=7, help='Question Index Length'
    )
    parser.add_argument(
        '--answer-index-length', '-A',
        type=int, default=3, help='Answer Index Length'
    )
    parser.add_argument(
        '--indent', '-I',
        type=int, default=2, help='Indent'
    )
    args = parser.parse_args()
    logger.debug('args: %s', args)
    for input_file in args.input_files:
        convert_excel2json(
            input_file,
            id_prefix=args.id_prefix,
            question_index_length=args.question_index_length,
            answer_index_length=args.answer_index_length,
            json_lines=args.json_lines,
            indent=args.indent,
        )
    logger.info('stat: %s', stat)
        