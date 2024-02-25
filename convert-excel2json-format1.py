#!/usr/bin/env python3

import json
import math
import os.path
import unicodedata

import pandas as pd

from logzero import logger
from tqdm.auto import tqdm

EXCLUDE_FLAGS = [
    2, 3, 9,
    '2', '3' , '9', 'R',
]

META_SPLIT_DELIM = ';'
META_SPLIT_FIELDS = [
    'task',
    'prospective',
    'domain',
    'source-to-answer',
    'output-type',
]

META_MAP = {
    'task': [
        '操作',
        '操作\nー\n要約、分類、創作、オープン質問、クローズド質問',
    ],
    'prospective': [
        '主観',
        '唯一解／客観／主観\nー\n唯一解：唯一の回答\n客観：一般的な回答\n主観：個人の意見',
    ],
    'time-dependency': [
        '時間依存',
        '時間依存\nー\n時間によって結果が異なる質問',
    ],
    'domain': [
        '分野',
        '分野\nー\n数学、歴史、グルメ（学校の科目、学問、新聞分類程度）',
    ],
    'source-to-answer': [
        '対象',
        '能力',
        '対象\nー\n問われている対象知識、数学、創作',
    ],
    'output-type': [
        '回答タイプ',
        '回答タイプ\nー\n本質的質問の形式的タイプ。単語、数字、リスト、文章',
    ],
    'text-producer': [
        '質問作成者',
    ],
    'output-producer': [
        '回答作成者',
    ],
    'output-reference': [
        '参考サイト【回答】',
        '参照サイト（回答）',
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
                if key == 'time-dependency':
                    if value == '時間依存':
                        value = True
                    elif value == '' or value is None:
                        value = False
                    else:
                        raise ValueError(f'Invalid time-dependency: {value}')
                if isinstance(value, list):
                    value = [normalize(x) for x in value]
                meta[key] = value
                found = True
                break
        if not found:
            raise ValueError(f'No {key}')
    new_output_type = []
    answer_carefully_type = []
    for value in meta['output-type']:
        carefully = False
        if value.find('回答不適切') >= 0:
            carefully = True
        elif value.find('要注意') >= 0:
            carefully = True
        if carefully:
            answer_carefully_type.append(value)
        else:
            new_output_type.append(value)
    meta['output-type'] = new_output_type
    meta['answer-carefully-type'] = answer_carefully_type
    #for column in row.keys():
    #    if column.find('要注意') >= 0:
    #        logger.debug('column: %s', column)
    #        logger.debug('row[column]: %s', row[column])
    return meta

map_question_id_to_answers = {}
map_question_to_id = {}
stat = {
    'num_questions': 0,
    'max_num_answers': 0,
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
    df = pd.read_excel(input_path)
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
                q = row['書き換えた質問文']
                q = normalize(q)
                if '作成した回答' in row:
                    a = row['作成した回答']
                elif '回答' in row:
                    a = row['回答']
                else:
                    raise ValueError('No 回答 or 作成した回答')
                a = normalize(a)
                flg_q = row['flg_Q']
                flg_a = row['flg_A']
                if not q or not a:
                    continue
                if flg_q in EXCLUDE_FLAGS:
                    continue
                if flg_a in EXCLUDE_FLAGS:
                    continue
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
                    a_id = len(answers) + 1
                    max_num_answers = stat['max_num_answers']
                    if a_id > max_num_answers:
                        stat['max_num_answers'] = a_id
                else:
                    raise ValueError(f'Duplicated Answer: {answers[a_id]}')
                meta = get_meta_data(row)
                ref = meta.get('output-reference')
                if isinstance(ref, str):
                    ref = ref.strip().split('\n')
                    ref = [x for x in ref if x]
                    meta['output-reference'] = ref
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
            #f_output.write(json.dumps(row, ensure_ascii=False) + '\n')
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

def merge_single(
    indent:int,
    merge_single_path: str,
):
    single_rows = []
    for q_id, answers in map_question_id_to_answers.items():
        if len(answers) != 1:
            continue
        answer = answers[0]
        single_rows.append(answer)
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
):
    multi_rows = []
    for q_id, answers in map_question_id_to_answers.items():
        if len(answers) <= 1:
            continue
        for answer in answers:
            multi_rows.append(answer)
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
        )
    if args.merge_multi_path:
        merge_multi(
            indent=args.indent,
            merge_multi_path=args.merge_multi_path,
        )
        