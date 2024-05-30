#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import sys

#from . common.json_functions import load_json_array
from common.json_functions import load_json_array

from logzero import logger

#from visedit import StringEdit

map_question_to_id = {}
map_question_id_to_answers = {}
map_question_id_to_target_answers = {}
map_id_to_question = {}
map_id_to_target_question = {}

FILL_TARGETS = [
    'task',
    'perspective',
    'domain',
    'source-to-answer',
    'output-type',
]

#map_target_question_to_id = {}
#max_question_id = 0
stat = {
    'num_questions': 0,
    'num_target_questions': 0,
    'num_modified_questions': 0,
    'num_new_questions': 0,
    'max_question_id': 0,
    #'max_loaded_question_id': 0,
    'max_answers': 0,
    #'max_loaded_answers': 0,
    #'max_loaded_answers': 0,
    'max_target_answers': 0,
    'num_modified_questions': 0,
    'num_modified_answers': 0,
}

#new_question_ids = []
#map_question_id_to_new_answer_ids = []

def load_previous_json(
    path: str,
):
    '''
    既存のJSONファイルからデータを取得
    '''
    logger.debug(f'Loading previous JSON file: {path}')
    rows = load_json_array(path)
    for row in rows:
        for key in FILL_TARGETS:
            val = row['meta'][key]
            if not val:
                str_id = row['ID']
                logger.debug(f'id:{str_id}')
                logger.debug(f'key:{key}')
                logger.debug(f'val:{val}')
                raise ValueError(f'{str_id}, Empty value: {key}')
        id_fields = row['ID'].split('-')
        question_id = int(id_fields[-2])
        answer_id = int(id_fields[-1])
        question = row['text']
        answer = row['output']
        if question not in map_question_to_id:
            map_question_to_id[question] = question_id
            map_id_to_question[question_id] = question
            answers = []
            map_question_id_to_answers[question_id] = answers
            if question_id > stat['max_question_id']:
                stat['max_question_id'] = question_id
            #stat['num_loaded_questions'] += 1
            stat['num_questions'] += 1
        else:
            answers = map_question_id_to_answers[question_id]
        #if answer not in map_question_id_to_answers[question_id]:
        #    map_question_id_to_answers[question_id].append(answer)
        #    answers = map_question_id_to_answers[question_id]
        #if answer not in answers:
        if next((a for a in answers if a['output'] == answer), None) is None:
            #answers.append(answer)
            answers.append(row)
            #if len(answers) > stat['max_loaded_answers']:
            #    stat['max_loaded_answers'] = len(answers)
            if len(answers) > stat['max_answers']:
                stat['max_answers'] = len(answers)
        else:
            print(f'Duplicate answer: {answer[:100]}')
        #if len(map_question_id_to_answers[question_id]) > stat['max_answers']:
        #    stat['max_answers'] = len(map_question_id_to_answers[question_id])
        #if question_id > stat['max_question_id']:
        #    stat['max_question_id'] = question_id
        #stat['num_questions'] += 1

def merge_json_files(
    json_paths,
    prefix='',
    question_index_length=7,
    answer_index_length=3,
    allow_duplicate_question=True,
    check_loaded=False,
):
    rows = []
    #if check_loaded:
    #    allow_duplicate_question = True
    for json_path in json_paths:
        logger.debug(f'Processing: {json_path}')
        file_rows = load_json_array(json_path)
        for i, row in enumerate(file_rows):
            for key in FILL_TARGETS:
                val = row['meta'][key]
                #if not val:
                #    str_id = row['ID']
                #    logger.debug(f'id:{str_id}')
                #    logger.debug(f'key:{key}')
                #    logger.debug(f'val:{val}')
                #    raise ValueError(f'{str_id}, Empty value: {key}')
                error = False
                if isinstance(val, list):
                    for v in val:
                        if error:
                            break
                        if not v:
                            logger.debug(f'row: {row}')
                            logger.debug(f'line:{i}, id:${row["ID"]}, key:{key}, val:{v}')
                            #row['meta'][key].append(v)
                            error = True
                            break
                if error:
                    break

            question = row['text']
            #answers = []
            if question not in map_question_to_id:
                if check_loaded:
                    id_fields = row['ID'].split('-')
                    question_id = int(id_fields[-2])
                    #except_ids = [
                    #    4735,
                    #    4757,
                    #    4772,
                    #    4950,
                    #    5350,
                    #    5535,
                    #    5536,
                    #    5570,
                    #    5579,
                    #    6114,
                    #]
                    #if question_id in except_ids:
                    if True:
                        # 修正されたデータのみを出力するため、質問文IDのマップを修正する
                        map_question_to_id[question] = question_id
                        map_id_to_question[question_id] = question
                        answers = map_question_id_to_answers[question_id]
                        stat['num_modified_questions'] += 1
                    else:
                        question_prev = map_id_to_question[question_id]
                        logger.debug(f'line:{i}, id:${row["ID"]}')
                        logger.debug(f'question_id: {question_id}')
                        logger.debug(f'question_old: {question_prev}')
                        logger.debug(f'question_new: {question}')
                        for j in range(0, max(len(question_prev),len(question))):
                            logger.debug(f'{j}: {question_prev[j:j+1]}, {question[j:j+1]}')
                            if question_prev[j] != question[j]:
                                logger.debug(f'j: {j}')
                                logger.debug(f'question_prev[j]: {question_prev[j:j+1]}')
                                logger.debug(f'question[j]: {question[j:j+1]}')
                                break
                        raise ValueError(f'Question not loaded: {question}')
                else:
                    question_id = stat['max_question_id'] + 1
                    stat['max_question_id'] = question_id
                    stat['num_questions'] += 1
                    stat['num_new_questions'] += 1
                    #stat['num_loaded_questions'] += 1
                    #stat['num_target_questions'] += 1
                    map_question_to_id[question] = question_id
                    map_id_to_question[question_id] = question
                    #map_id_to_target_question[question_id] = question
                    answers = []
                    #map_question_id_to_answers[question_id] = []
                    map_question_id_to_answers[question_id] = answers
            else:
                if not allow_duplicate_question:
                    raise ValueError(f'Duplicate question: {question}')
                question_id = map_question_to_id[question]
                #answers = map_question_id_to_answers[question_id]
                answers = map_question_id_to_answers[question_id]
            #if question not in map_target_question_to_id:
            #    map_target_question_to_id[question] = question_id
            #    stat['num_target_questions'] += 1
            if question_id not in map_id_to_target_question:
                map_id_to_target_question[question_id] = question
                stat['num_target_questions'] += 1
                target_answers = {}
                map_question_id_to_target_answers[question_id] = target_answers
            else:
                target_answers = map_question_id_to_target_answers[question_id]
            #answers = row['output']
            #for answer in answers:
            #    if answer not in map_question_id_to_answers[question_id]:
            #        answer_id = f'{len(map_question_id_to_answers[question_id]):0{answer_index_length}d}'
            #        map_question_id_to_answers[question_id].append(answer)
            answer = row['output']
            #if answer not in answers:
            answer_id = next((i+1 for i, a in enumerate(answers) if a['output'] == answer), None)
            #if next((a for a in answers if a['output'] == answer), None) is None:
            if answer_id is None:
                if check_loaded:
                    #except_ids = [
                    #    'ichikara-instruction-004-002-0004629-001',
                    #    'ichikara-instruction-004-002-0006134-001',
                    #    'ichikara-instruction-004-002-0006137-001',
                    #    'ichikara-instruction-004-002-0006138-001',
                    #    'ichikara-instruction-004-002-0006148-001',
                    #]
                    #if len(answers) == 1 and row['ID'] in except_ids:
                    if len(answers) == 1:
                        # 何かの手違いで回答文が変わってしまっている
                        # 修正後の回答文のみを出力するため回答文一覧をリセットする
                        answers = []
                        stat['num_modified_answers'] += 1
                    else:
                        logger.debug('line: %s', i)
                        logger.debug('id: %s', row['ID'])
                        logger.debug('question: %s', question)
                        logger.debug('answers: %s', answers)
                        logger.debug('answer_new: %s', answer)
                        #if len(answers) == 1:
                        #    #diff = StringEdit(answers[0], answer).generate_text()
                        #    #logger.debug('diff: %s', diff)
                        #    #logger.debug('diff:')
                        #    #print(diff)
                        #    answer0 = answers[0]
                        #    for j in range(0, len(answer0)):
                        #        logger.debug(f'{j}: {answer0[j]}, {answer[j]}')
                        #        if answer0[j] != answer[j]:
                        #            logger.debug(f'j: {j}')
                        #            logger.debug(f'answer0[j]: {answer0[j]}')
                        #            logger.debug(f'answer[j]: {answer[j]}')
                        #            break
                        id_fields = row['ID'].split('-')
                        answer_id = int(id_fields[-1])
                        answer_prev = answers[answer_id-1]
                        logger.debug('answer_prev: %s', answer_prev)
                        #for j in range(0, len(answer0)):
                        for j in range(0, len(answer_prev)):
                            #logger.debug(f'{j}: {answer0[j]}, {answer[j]}')
                            logger.debug(f'{j}: {answer_prev[j]}, {answer[j]}')
                            #if answer0[j] != answer[j]:
                            if answer_prev[j] != answer[j]:
                                logger.debug(f'j: {j}')
                                #logger.debug(f'answer0[j]: {answer0[j]}')
                                #logger.debug(f'answer[j]: {answer[j]}')
                                logger.debug(f'answer_old[j]: {answer_prev[j]}')
                                logger.debug(f'answer_new[j]: {answer[j]}')
                                break
                        raise ValueError(f'line:{i}, id:${row["ID"]}, Answer not loaded: {answer[:100]}')
                #answers.append(answer)
                answers.append(row)
                answer_id = len(answers)
                if len(answers) > stat['max_answers']:
                    stat['max_answers'] = len(answers)
                #if len(answers) > stat['max_loaded_answers']:
                #    stat['max_loaded_answers'] = len(answers)
                #if len(answers) > stat['max_target_answers']:
                #    stat['max_target_answers'] = len(answers)
            else:
                if check_loaded:
                    old_row = answers[answer_id-1]
                    #for key in FILL_TARGETS:
                    #    val = row['meta'][key]
                    #    #logger.debug(f'line:{i}, id:${row["ID"]}, key:{key}, val:{val}')
                    #    if not val:
                    #        logger.debug(f'line:{i}, id:${row["ID"]}, key:{key}')
                    #        row['meta'][key] = row_prev['meta'][key]
                    #    if isinstance(val, list):
                    #        for v in val:
                    #            if not v:
                    #                logger.debug(f'row: {row}')
                    #                logger.debug(f'line:{i}, id:${row["ID"]}, key:{key}, val:{v}')
                    #                #row['meta'][key].append(v)
                    #                break
                    for key in old_row['meta'].keys():
                        old_value = old_row['meta'][key]
                        new_value = row['meta'][key]
                        #if old_value or old_value == 0 or old_value == False:
                        #if key in [
                        #    'alert-type',
                        #    'time-dependency',
                        #]:
                        #    continue
                        if old_value or old_value in [0, False, []]:
                        #if old_value and not new_value:
                        #if old_value != new_value:
                            #if new_value == []:
                            if new_value != old_value and new_value in [None]:
                            #if new_value != old_value and new_value in [None, []]:
                            #if new_value != old_value and new_value in [False, None, []]:
                                if key not in [
                                    #'alert-type',
                                    #'time-dependency',
                                    'output-reference'
                                    #'domain',
                                    #'output-type',
                                    #'perspective',
                                    #'source-to-answer',
                                    #'task',
                                ]:
                                    logger.debug(f'line:{i}, id:${row["ID"]}, key:{key}')
                                    logger.debug(f'old_value: {old_value}')
                                    logger.debug(f'new_value: {new_value}')
                                #logger.debug(f'before: {row["meta"][key]}')
                                row['meta'][key] = old_value
                                #logger.debug(f'after: {row["meta"][key]}')
                    answers[answer_id-1] = row
                else:
                    raise ValueError(f'Duplicate answer: {answer[:100]}')
            #answer_id = len(answers)
            #answer_id = next((i+1 for i, a in enumerate(answers) if a['output'] == answer), None)
            if answer_id not in target_answers:
                target_answers[answer_id] = row
                if len(target_answers) > stat['max_target_answers']:
                    stat['max_target_answers'] = len(target_answers)
            str_question_id = f'{question_id:0{question_index_length}d}'
            str_answer_id = f'{answer_id:0{answer_index_length}d}'
            if prefix:
                if prefix.endswith('-'):
                    row['ID'] = f'{prefix}{str_question_id}-{str_answer_id}'
                else:
                    row['ID'] = f'{prefix}-{str_question_id}-{str_answer_id}'
            else:
                row['ID'] = f'{str_question_id}-{str_answer_id}'
            if 'file' in row:
                del row['file']
            rows.append(row)
    #if output_path.endswith('.json'):
    #    with open(output_path, 'w', encoding='utf-8') as f:
    #        logger.debug(f'Exporting: {output_path}')
    #        json.dump(rows, f, indent=indent, ensure_ascii=False)
    #elif output_path.endswith('.jsonl'):
    #    with open(output_path, 'w', encoding='utf-8') as f:
    #        logger.debug(f'Exporting: {output_path}')
    #        for row in rows:
    #            f.write(json.dumps(row, ensure_ascii=False) + '\n')
    #else:
    #    raise ValueError(f'Invalid path: {json.dumps(output_path)}')
    #str_stat_json = json.dumps(stat, indent=indent, ensure_ascii=False)
    #if output_stat_path:
    #    if output_stat_path.endswith('.json'):
    #        with open(output_stat_path, 'w', encoding='utf-8') as f:
    #            logger.debug(f'Exporting stat: {output_stat_path}')
    #            f.write(str_stat_json)
    #    else:
    #        raise ValueError(f'Invalid path: {output_stat_path}')
    #logger.debug(f'Stat:\n{str_stat_json}')
    return rows

def output_json(
    rows,
    output_path: str,
    indent=2,
    sort = False,
):
    if sort:
        rows.sort(key=lambda x: x['ID'])
    if output_path.endswith('.json'):
        with open(output_path, 'w', encoding='utf-8') as f:
            logger.debug(f'Exporting: {output_path}')
            json.dump(rows, f, indent=indent, ensure_ascii=False)
    elif output_path.endswith('.jsonl'):
        with open(output_path, 'w', encoding='utf-8') as f:
            logger.debug(f'Exporting: {output_path}')
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + '\n')
    else:
        raise ValueError(f'Invalid path: {json.dumps(output_path)}')
    
def output_stat_json(
    stat,
    output_stat_path: str,
    indent=2,
):
    str_stat_json = json.dumps(stat, indent=indent, ensure_ascii=False)
    if output_stat_path:
        if output_stat_path.endswith('.json'):
            with open(output_stat_path, 'w', encoding='utf-8') as f:
                logger.debug(f'Exporting stat: {output_stat_path}')
                f.write(str_stat_json)
        else:
            raise ValueError(f'Invalid path: {output_stat_path}')
        
def merge_single(
    merge_single_path: str,
    indent:int,
):
    single_rows = []
    meta_stat = {}
    #for q_id, answers in map_question_id_to_answers.items():
    q_ids = sorted(map_question_id_to_answers.keys())
    #for q_id, answers in map_question_id_to_answers.items():
    for q_id in q_ids:
        answers = map_question_id_to_answers[q_id]
        if len(answers) != 1:
            continue
        answer = answers[0]
        single_rows.append(answer)
        for field in answer['meta'].keys():
            if field == 'output-reference':
                continue
            value = answer['meta'][field]
            #if not value:
            #    logger.debug(f'answer: {answer}')
            #    logger.debug(f'Empty value: {field}')
            field_stat = meta_stat.setdefault(field, {})
            if isinstance(value, list):
                for v in value:
                    #if not v:
                    #    logger.debug(f'answer: {answer}')
                    #    logger.debug(f'Empty value: {field}')
                    field_stat[v] = field_stat.get(v, 0) + 1
            else:
                field_stat[value] = field_stat.get(value, 0) + 1
    stat['num_single_questions'] = len(single_rows)
    stat['single_meta_stat'] = meta_stat
    single_rows.sort(key=lambda x: x['ID'])
    with open(merge_single_path, 'w', encoding='utf-8') as f:
        if merge_single_path.endswith('.jsonl'):
            for row in single_rows:
                f.write(json.dumps(row, ensure_ascii=False) + '\n')
        else:
            f.write(json.dumps(single_rows, ensure_ascii=False, indent=indent))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Merge JSON files')
    #parser.add_argument(
    #    'json_paths', metavar='json_path', type=str, nargs='+',
    #    help='Paths to JSON files to merge'
    #)
    parser.add_argument(
        '--new-paths', type=str, nargs='+',
        help='Paths to JSON files to merge'
    )
    parser.add_argument(
        '--prefix', type=str, default='',
        help='Prefix to add to IDs in the merged JSON')
    parser.add_argument(
        '--previous-paths', type=str, nargs='+',
        help='Paths to previous JSON files to load'
    )
    parser.add_argument(
        '--fixed-paths', type=str, nargs='+',
        help='Paths to fixed JSON files to load'
    )
    parser.add_argument(
        '--output', type=str, required=True,
        help='Output path for the merged JSON'
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
        '--output-stat-json',
        help='Path to export JSON file of statistics',
    )
    parser.add_argument(
        '--merge-single-path', '-S',
        help='Path to merge single QA JSON file',
    )
    args = parser.parse_args()
    logger.debug('args: %s', args)
    if args.previous_paths:
        for previous_path in args.previous_paths:
            load_previous_json(previous_path)
    rows = []
    if args.fixed_paths:
        rows += merge_json_files(
            args.fixed_paths,
            args.prefix,
            args.question_index_length,
            args.answer_index_length,
            check_loaded=True,
        )
    if args.new_paths:
        rows += merge_json_files(
            #args.json_paths,
            args.new_paths,
            args.prefix,
            args.question_index_length,
            args.answer_index_length,
            allow_duplicate_question=False,
            check_loaded=False,
        )
    output_json(rows, args.output, args.indent, sort=True)
    if args.merge_single_path:
        merge_single(args.merge_single_path, args.indent)
    output_stat_json(stat, args.output_stat_json, args.indent)
    str_stat_json = json.dumps(stat, indent=args.indent, ensure_ascii=False)
    logger.debug(f'Stat:\n{str_stat_json}')
