#!/usr/bin/env python3

import json
import math
import os.path
import re
import unicodedata

import pandas as pd

from logzero import logger
from tqdm.auto import tqdm

EXCLUDE_FLAGS = [
    2, 3, 9,
    '2', '3' , '9', 'R',
]

#META_SPLIT_DELIM = ';'
#META_SPLIT_DELIM = ';'
META_SPLIT_DELIM = '[;、\n]'
META_SPLIT_FIELDS = [
    'task',
    'perspective',
    'domain',
    'source-to-answer',
    'output-type',
]

CONVERT_MAP = {
    'domain': {
        #'気候': '気象',
        #'理科': '科学',
        #'生物のグループ': '生物名のグループ',
        #'人名': '人物',
        #'金融': '経済',
    }
}

META_MAP = {
    'task': [
        '操作',
        '操作\nー\n要約、分類、創作、オープン質問、クローズド質問',
    ],
    'perspective': [
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
        '参考サイトURL【回答】',
        '参照文献、Webサイト',
        '回答作成時参照文献、Webサイト',
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
    skip = False,
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
                    old_value = value
                    #value = value.split(META_SPLIT_DELIM)
                    value = re.split(META_SPLIT_DELIM, value)
                    #try:
                    #    value = value.split(META_SPLIT_DELIM)
                    #except Exception as e:
                    #    logger.error('row-key: %s', row_key)
                    #    logger.error('key: %s', key)
                    #    logger.error('key in META_SPLIT_FIELDS: %s', key in META_SPLIT_FIELDS)
                    #    logger.error('value: %s', value)
                    #    raise e
                    for elem in value:
                        if elem == 'に':
                            raise ValueError(f'Invalid value: {value}')
                            #raise ValueError(f'Invalid value: {value}, key: {key}')
                        #if elem:
                        #    new_values.append(normalize(elem))
                    value = [elem for elem in value if elem]
                    if key in CONVERT_MAP:
                        value = [CONVERT_MAP[key].get(x, x) for x in value]
                    #value = new_values
                    #if not new_values:
                    #if not value and not skip:
                    #    #if old_value:
                    #    raise ValueError(
                    #        f'Invalid value: {value}, key: {key}, old_value: {old_value}'
                    #    )
                    if old_value == '':
                        value = ['']
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
    alert_type = []
    for value in meta['output-type']:
        alert = False
        if value.find('回答不適切') >= 0:
            alert = True
        elif value.find('要注意') >= 0:
            alert = True
        if alert:
            alert_type.append(value)
        else:
            new_output_type.append(value)
    meta['output-type'] = new_output_type
    meta['alert-type'] = alert_type
    if ';' in str(meta['text-producer']):
        raise ValueError(f'Invalid text-producer: {meta["text-producer"]}')
    if ';' in str(meta['output-producer']):
        raise ValueError(f'Invalid output-producer: {meta["output-producer"]}')
    #meta['text-producer'] = str(meta['text-producer'])
    #meta['output-producer'] = str(meta['output-producer'])
    meta['text-producer'] = int(meta['text-producer'])
    meta['output-producer'] = int(meta['output-producer'])
    #if '' in meta['domain']:
    #    raise ValueError(f'Invalid domain: {meta["domain"]}')
    #if '' in meta['output-type']:
    #    raise ValueError(f'Invalid output-type: {meta["output-type"]}')
    #if '' in meta['source-to-answer']:
    #    raise ValueError(f'Invalid source-to-answer: {meta["source-to-answer"]}')
    #for column in row.keys():
    #    if column.find('要注意') >= 0:
    #        logger.debug('column: %s', column)
    #        logger.debug('row[column]: %s', row[column])
    return meta

map_question_id_to_answers = {}
map_question_to_id = {}
skipped_alert_rows = []
skipped_non_alert_rows = []
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
                if '書き換えた質問文' in row:
                    q = row['書き換えた質問文']
                elif '質問' in row:
                    q = row['質問']
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
                #meta = get_meta_data(row, False)
                if not q or not a:
                    continue
                #if flg_q in EXCLUDE_FLAGS:
                #    continue
                #if flg_a in EXCLUDE_FLAGS:
                #    continue
                #meta_skip = get_meta_data(row, skip=True)
                meta = get_meta_data(row, skip=True)
                #for key, value in meta_skip.items():
                if flg_q in EXCLUDE_FLAGS or flg_a in EXCLUDE_FLAGS:
                    #meta = get_meta_data(row, skip=True)
                    #meta = meta_skip
                    skip = {
                        'file': os.path.basename(input_path),
                        'text-producer': meta.get('text-producer'),
                        'output-producer': meta.get('output-producer'),
                        'flg_q': flg_q,
                        'flg_a': flg_a,
                        'text': q,
                        'output': a,
                        'task': str.join(';', meta['task']),
                        'perspective': str.join(';', meta['perspective']),
                        'time-dependency': meta['time-dependency'],
                        'domain': str.join(';', meta['domain']),
                        'source-to-answer': str.join(';', meta['source-to-answer']),
                        'output-type': str.join(';', meta['output-type']),
                        'alert-type': str.join(';', meta['alert-type']),
                        'output-reference': meta['output-reference'],
                    }
                    if len(meta['alert-type']) > 0:
                        skipped_alert_rows.append(skip)
                    else:
                        skipped_non_alert_rows.append(skip)
                    continue
                #meta = get_meta_data(row, skip=False)
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
            empty_keys = [
                #key for key, value in meta.items() if not value
                key for key, value in meta.items()
                if value == [] and key not in ['output-reference', 'alert-type']
            ]
            if empty_keys:
                problem_row = {
                    'file': os.path.basename(input_path),
                    'empty_keys': empty_keys,
                }
                problem_row.update(d)
                #problem_rows.append(problem_row)
                continue
            #if ';' in d['meta']['text-producer']:
            #    problem_row = {
            #        'file': os.path.basename(input_path),
            #    }
            #    problem_row.update(d)
            #    problem_rows.append(problem_row)
            #    continue
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
                f_output.write(json.dumps(row, ensure_ascii=False) + '\n'
            )
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

def export_csv_skipped(
    rows,
    export_csv_skipped_path,
):
    df = pd.DataFrame(rows)
    df.to_csv(export_csv_skipped_path, index=False, encoding='utf-8-sig')
    #df.to_csv(export_csv_skipped_path, index=False, encoding='cp932')
    #df.to_csv(export_csv_skipped_path, index=False, encoding='shift-jis')

def export_excel_skipped(
    rows,
    export_excel_skipped_path,
):
    df = pd.DataFrame(rows)
    df.to_excel(export_excel_skipped_path, index=False)

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
        '--export-csv-skipped-alert',
        help='Path to export skipped alert type CSV file',
    )
    parser.add_argument(
        '--export-csv-skipped-non-alert',
        help='Path to export skipped non-alert type CSV file',
    )
    parser.add_argument(
        '--export-excel-skipped-alert',
        help='Path to export skipped alert type Excel file',
    )
    parser.add_argument(
        '--export-excel-skipped-non-alert',
        help='Path to export skipped non-alert type Excel file',
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
    #parser.add_argument(
    #    '--output-problem-json',
    #    help='Path to export JSON file of problem data (for debug)',
    #)
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
    if args.export_csv_skipped_alert:
        export_csv_skipped(
            skipped_alert_rows,
            args.export_csv_skipped_alert,
        )
    if args.export_csv_skipped_non_alert:
        export_csv_skipped(
            skipped_non_alert_rows,
            args.export_csv_skipped_non_alert,
        )
    if args.export_excel_skipped_alert:
        export_excel_skipped(
            skipped_alert_rows,
            args.export_excel_skipped_alert,
        )
    if args.export_excel_skipped_non_alert:
        export_excel_skipped(
            skipped_non_alert_rows,
            args.export_excel_skipped_non_alert,
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
        