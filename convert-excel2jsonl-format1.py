#!/usr/bin/env python3

#import csv
import json
import os.path
import unicodedata

import pandas as pd

from logzero import logger
from tqdm.auto import tqdm

EXCLUDE_FLAGS = [
    2, 3, 9,
    '2', '3' , '9', 'R',
]

def normalize(text):
    if type(text) == str:
        text = text.strip()
        return unicodedata.normalize('NFKC', text)
    return text

#map_id_pair_to_row = dict()
map_question_id_to_answers = {}
map_question_to_id = {}
stat = {
    'num_questions': 0,
    'max_num_answers': 0,
}
#num_questions = 0
#max_num_answers = 0

def convert_excel2jsonl(
    input_path,
    #jsonl_file,
    id_prefix,
    question_index_length,
    answer_index_length,
):
    #global num_questions
    #global max_num_answers
    df = pd.read_excel(input_path)
    rows = []
    jsonl_file = os.path.splitext(input_path)[0] + '.jsonl'
    #logger.debug('# df: %s', df)
    with open(jsonl_file, 'w', encoding='utf-8') as jsonlfile:
        for i, row in tqdm(
            df.iterrows(),
            total=len(df),
            desc=f'Converting {input_path} to {jsonl_file}'
        ):
            #if i == 0:
            #    logger.debug('row: %s', row)
            #    logger.debug('row.keys(): %s', row.keys())
            #if i >= 1:
            #    break
            #if 'ランダムに並べ替え' in row:
            #    q_id = row['ランダムに並べ替え']
            #elif '1.4ランダムに並べ替え' in row:
            #    q_id = row['1.4ランダムに並べ替え']
            #elif 'ランダムに並べ替え2.6' in row:
            #    q_id = row['ランダムに並べ替え2.6']
            #else:
            #    logger.error('row:\n%s', row)
            #    raise ValueError('No Question ID')
            #a_id = 1
            try:
                q = row['書き換えた質問文']
                q = normalize(q)
                if '作成した回答' in row:
                    a = row['作成した回答']
                elif '回答' in row:
                    a = row['回答']
                else:
                    raise ValueError('No 回答 or 作成した回答')
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
                #a_id = len(answers) + 1
                #a_id = next(
                #    filter(
                #        lambda t: t[1]['output'] == a,
                #        enumerate(answers),
                #    ),
                #    None
                #)
                a_id = next(
                    (t[0] for t in enumerate(answers) if t[1]['output'] == a),
                    None,
                )
                if a_id is None:
                    a_id = len(answers) + 1
                    #answers.append(dict(
                    #    output = a,
                    #))
                    max_num_answers = stat['max_num_answers']
                    if a_id > max_num_answers:
                        stat['max_num_answers'] = a_id
                else:
                    #raise ValueError(f'Duplicated Answer: {a}')
                    #logger.error('flg_q: "%s"', flg_q)
                    logger.error('row: %s', row)
                    raise ValueError(f'Duplicated Answer: {answers[a_id]}')
                if '操作' in row:
                    task = row['操作']
                elif '操作\nー\n要約、分類、創作、オープン質問、クローズド質問' in row:
                    task = row['操作\nー\n要約、分類、創作、オープン質問、クローズド質問']
                else:
                    raise ValueError('No 操作')
                if '主観' in row:
                    prospective = row['主観']
                elif '唯一解／客観／主観\nー\n唯一解：唯一の回答\n客観：一般的な回答\n主観：個人の意見' in row:
                    prospective = row['唯一解／客観／主観\nー\n唯一解：唯一の回答\n客観：一般的な回答\n主観：個人の意見']
                else:
                    raise ValueError('No 主観')
                if '時間依存' in row:
                    time_dependency = row['時間依存']
                elif '時間依存\nー\n時間によって結果が異なる質問' in row:
                    time_dependency = row['時間依存\nー\n時間によって結果が異なる質問']
                else:
                    raise ValueError('No 時間依存')
                if '分野' in row:
                    domain = row['分野']
                elif '分野\nー\n数学、歴史、グルメ（学校の科目、学問、新聞分類程度）' in row:
                    domain = row['分野\nー\n数学、歴史、グルメ（学校の科目、学問、新聞分類程度）']
                else:
                    raise ValueError('No 分野')
                #source_to_answer = row['対象']
                if '対象' in row:
                    source_to_answer = row['対象']
                elif '能力' in row:
                    source_to_answer = row['能力']
                elif '対象\nー\n問われている対象知識、数学、創作' in row:
                    source_to_answer = row['対象\nー\n問われている対象知識、数学、創作']
                else:
                    raise ValueError('No 対象 or 能力')
                if '回答タイプ' in row:
                    output_type = row['回答タイプ']
                elif '回答タイプ\nー\n本質的質問の形式的タイプ。単語、数字、リスト、文章' in row:
                    output_type = row['回答タイプ\nー\n本質的質問の形式的タイプ。単語、数字、リスト、文章']
                else:
                    raise ValueError('No 回答タイプ')
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
            except Exception as e:
                logger.error('row:\n%s', row)
                raise e
            #id = 'answercarefully-instruction-000-001-0000001-001'
            #id = f'answercarefully-instruction-000-001-0000001-{q_id:03d}'
            d = dict(
                ID=None,
                #ID=q_id,
                q_id=q_id,
                a_id=a_id,
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
            #if (q_id, a_id) in map_id_pair_to_row:
            #    logger.error('duplicated: %s', map_id_pair_to_row[(q_id, a_id)])
            #    raise ValueError(f'Duplicate ID: {(q_id, a_id)}')
            #map_id_pair_to_row[(q_id, a_id)] = d
            answers.append(d)
        min_question_index_length = len(str(stat['num_questions']))
        min_answer_index_length = len(str(stat['max_num_answers']))
        if question_index_length < min_question_index_length:
            raise ValueError(f'question_index_length: {question_index_length}')
        if answer_index_length < min_answer_index_length:
            raise ValueError(f'answer_index_length: {answer_index_length}')
        for i, row in enumerate(
            tqdm(rows),
        ):
            q_id = row.pop('q_id')
            a_id = row.pop('a_id')
            str_question_index = str(q_id).zfill(question_index_length)
            str_answer_index = str(a_id).zfill(answer_index_length)
            row['ID'] = f'{id_prefix}-{str_question_index}-{str_answer_index}'
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
    parser.add_argument(
        '--question-index-length', '-Q',
        type=int, default=7, help='Question Index Length'
    )
    parser.add_argument(
        '--answer-index-length', '-A',
        type=int, default=3, help='Answer Index Length'
    )
    args = parser.parse_args()
    logger.debug('args: %s', args)
    for input_file in args.input_files:
        convert_excel2jsonl(
            input_file,
            #args.jsonl_file,
            id_prefix=args.id_prefix,
            question_index_length=args.question_index_length,
            answer_index_length=args.answer_index_length,
        )
