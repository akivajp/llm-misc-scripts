#!/usr/bin/env python3

import json
import math
import os.path
import re
import unicodedata

import pandas as pd

from logzero import logger
from tqdm.auto import tqdm

META_SPLIT_DELIM = '[;、\n]'

META_SPLIT_FIELDS = [
    'task',
    'perspective',
    'domain',
    'source-to-answer',
    'output-type',
    'alert-type',
    'point-allocation',
]

META_FIELDS = [
    'task',
    'perspective',
    'time-dependency',
    'domain',
    'source-to-answer',
    'output-type',
    #'text-producer',
    #'output-producer',
    #'output-reference',
    'alert-type',
    'point-allocation',
]

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
    for key in META_FIELDS:
        value = row[key]
        #value = row.get(key, None)
        if isinstance(value,float) and math.isnan(value):
            if key in META_SPLIT_FIELDS:
                value = ''
            elif key == 'output-reference':
                value = ''
            else:
                value = None
        value = normalize(value)
        if key in META_SPLIT_FIELDS:
            #old_value = value
            #if value:
            #    value = re.split(META_SPLIT_DELIM, value)
            if type(value) == int:
                value = str(value)
            value = re.split(META_SPLIT_DELIM, value)
            #else:
            #    value = []
            #if old_value == '':
            #    value = ['']
            #value = [x for x in value if x]
            value = list(filter(None, value))
            #if key == 'point-allocation':
            #    if len(value) >= 2:
            #        logger.debug('point-allocation: %s', value)
        if key == 'time-dependency':
            if value == '時間依存':
                value = True
            elif value == '' or value is None:
                value = False
            else:
                raise ValueError(f'Invalid time-dependency: {value}')
        if key == 'output-reference':
            if isinstance(value, str):
                value = value.split('\n')
                value = list(filter(None, value))
        #if isinstance(value, list):
        #    value = [normalize(x) for x in value]
        meta[key] = value
    #meta['text-producer'] = int(meta['text-producer'])
    #meta['output-producer'] = int(meta['output-producer'])
    return meta

map_question_id_to_answers = {}
map_question_to_id = {}
stat = {
    'num_questions': 0,
    'max_num_answers': 0,
}
problem_rows = []

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
                q = row['text']
                q = normalize(q)
                a = row['output']
                new_a = None
                if str(a) == '2024-03-11 00:00:00':
                    new_a = '3月11日'
                if str(a) == '03:01:00':
                    new_a = '3:1'
                if new_a is not None:
                    logger.warn('output converted from "%s" to "%s"', a, new_a)
                    a = new_a
                a = normalize(a)
                if not q or not a:
                    continue
                testId = row['test-ID']
                #sample_id = row['ID']
                #id_fields = sample_id.split('-')
                #numbers = id_fields[-4:]
                #major, minor, q_id, a_id = numbers
                #ref = row['output-reference']
                #ref = row.get('output-reference')
                #if isinstance(ref, str):
                #    ref = ref.split('\n')
                meta = get_meta_data(row)

                q_id = map_question_to_id.get(q)
                if q_id is None:
                    q_id = len(map_question_to_id) + 1
                    map_question_to_id[q] = q_id
                    stat['num_questions'] += 1
                answers = map_question_id_to_answers.get(q_id)
                if answers is None:
                    answers = []
                    map_question_id_to_answers[q_id] = answers
                a_id = next(
                    (t[0] for t in enumerate(answers) if t[1]['output'] == a),
                    None,
                )
                if a_id is None:
                    except_q_ids = [
                        #1553,
                    ]
                    if q_id in except_q_ids:
                        #raise ValueError(f'Duplicated Answer 1553: {answers}')
                        continue
                    a_id = len(answers) + 1
                    max_num_answers = stat['max_num_answers']
                    if a_id > max_num_answers:
                        stat['max_num_answers'] = a_id
                else:
                    raise ValueError(f'Duplicated Answer: {answers[a_id]}')
                ref = meta.get('output-reference')
                if isinstance(ref, str):
                    ref = ref.strip().split('\n')
                    ref = [x for x in ref if x]
                    meta['output-reference'] = ref

            except Exception as e:
                logger.error('row:\n%s', row)
                raise e
            #d = dict(
            #    ID=None,
            #    q_id=q_id,
            #    a_id=a_id,
            #    text=q,
            #    output=a,
            #    meta=meta,
            #    file=input_path,
            #)
            d = {
                'ID': None,
                'q_id': q_id,
                'a_id': a_id,
                'question-ID': testId,
                'text': q,
                'output': a,
                'meta': meta,
                'file': input_path,
            }
            rows.append(d)
            answers.append(d)
        min_question_index_length = len(str(stat['num_questions']))
        min_answer_index_length = len(str(stat['max_num_answers']))
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
            #del row['file']
            output_rows.append(row)
        if json_lines:
            for i, row in enumerate(
                tqdm(output_rows),
            ):
                try:
                    f_output.write(json.dumps(row, ensure_ascii=False) + '\n')
                except Exception as e:
                    #logger.error('row: %s', row)
                    logger.error('type(row.output): %s', type(row['output']))
                    logger.error('row.output: "%s"', str(row['output']))
                    raise e
        else:
            f_output.write(
                json.dumps(output_rows, ensure_ascii=False, indent=indent)
            )

def merge_single(
    indent:int,
    merge_single_path: str,
    keep_file: bool = False,
):
    single_rows = []
    meta_stat = {}
    for q_id, answers in map_question_id_to_answers.items():
        if len(answers) != 1:
            continue
        answer = answers[0]
        if not keep_file:
            if 'file' in answer:
                # デバッグ用フィールドを削除
                del answer['file']
        single_rows.append(answer)
        #for field in META_MAP:
        for field in answer['meta'].keys():
            if field == 'output-reference':
                continue
            value = answer['meta'][field]
            field_stat = meta_stat.setdefault(field, {})
            if isinstance(value, list):
                for v in value:
                    field_stat[v] = field_stat.get(v, 0) + 1
            else:
                field_stat[value] = field_stat.get(value, 0) + 1
    stat['num_single_questions'] = len(single_rows)
    #meta_stat_lists = {}
    #for field, field_stat in meta_stat.items():
    #    stat_list = [
    #        {'key:': key, 'value': value} for key, value in field_stat.items()
    #    ]
    #    meta_stat_lists[field] = sorted(stat_list, key=lambda x: x['value'])
    #stat['single_meta_stat'] = meta_stat_lists
    stat['single_meta_stat'] = meta_stat
    single_rows.sort(key=lambda x: x['ID'])
    with open(merge_single_path, 'w', encoding='utf-8') as f:
        if merge_single_path.endswith('.jsonl'):
            for row in single_rows:
                f.write(json.dumps(row, ensure_ascii=False) + '\n')
        else:
            f.write(json.dumps(single_rows, ensure_ascii=False, indent=indent))

def merge_multi(
    indent:int,
    merge_multi_path: str,
    keep_file: bool = False,
):
    multi_rows = []
    question_set = set()
    meta_stat = {}
    for q_id, answers in map_question_id_to_answers.items():
        if len(answers) <= 1:
            continue
        for answer in answers:
            if not keep_file:
                if 'file' in answer:
                    # デバッグ用フィールドを削除
                    del answer['file']
            multi_rows.append(answer)
            question_set.add(answer['text'])
            #for field in META_MAP:
            for field in answer['meta'].keys():
                if field == 'output-reference':
                    continue
                value = answer['meta'][field]
                field_stat = meta_stat.setdefault(field, {})
                if isinstance(value, list):
                    for v in value:
                        field_stat[v] = field_stat.get(v, 0) + 1
                else:
                    field_stat[value] = field_stat.get(value, 0) + 1
    stat['num_multi_questions'] = len(question_set)
    stat['num_multi_answers'] = len(multi_rows)
    #meta_stat_lists = {}
    #for field, field_stat in meta_stat.items():
    #    stat_list = [
    #        {'key:': key, 'value': value} for key, value in field_stat.items()
    #    ]
    #    meta_stat_lists[field] = sorted(stat_list, key=lambda x: x['value'])
    #stat['multi_meta_stat'] = meta_stat_lists
    stat['multi_meta_stat'] = meta_stat
    multi_rows.sort(key=lambda x: x['ID'])
    with open(merge_multi_path, 'w', encoding='utf-8') as f:
        if merge_multi_path.endswith('.jsonl'):
            for row in multi_rows:
                f.write(json.dumps(row, ensure_ascii=False) + '\n')
        else:
            f.write(json.dumps(multi_rows, ensure_ascii=False, indent=indent))

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
    parser.add_argument(
        '--merge-single-path', '-S',
        help='Path to merge single QA JSON file',
    )
    parser.add_argument(
        '--merge-multi-path', '-M',
        help='Path to merge multi-QA JSON file',
    )
    parser.add_argument(
        '--output-stat-json',
        help='Path to export JSON file of statistics',
    )
    parser.add_argument(
        '--keep-file',
        action='store_true',
        help='Keep file field for debug',
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
    if args.merge_single_path:
        merge_single(
            indent=args.indent,
            merge_single_path=args.merge_single_path,
            keep_file=args.keep_file,
        )
    if args.merge_multi_path:
        merge_multi(
            indent=args.indent,
            merge_multi_path=args.merge_multi_path,
            keep_file=args.keep_file,
        )
    #logger.info('stat: %s', stat)
    if args.output_stat_json:
        with open(args.output_stat_json, 'w', encoding='utf-8') as f:
            f.write(
                json.dumps(
                    stat,
                    ensure_ascii=False,
                    indent=args.indent,
                    sort_keys=True,
                )
            )
    if problem_rows:
        with open('./tmp/problems.json', 'w', encoding='utf-8') as f:
            f.write( json.dumps( problem_rows, ensure_ascii=False, indent=args.indent) )
        