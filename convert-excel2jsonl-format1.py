#!/usr/bin/env python3

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

def normalize(text):
    if isinstance(text, str):
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
                meta = {}
                for key, values in META_MAP.items():
                    found = False
                    for value in values:
                        if value in row:
                            meta[key] = row[value]
                            found = True
                            break
                    if not found:
                        raise ValueError(f'No {key}')
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
            )
            rows.append(d)
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

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('input_files', nargs='+', help='Input Excel files')
    parser.add_argument('--id-prefix', '-P', required=True, help='ID prefix')
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
            id_prefix=args.id_prefix,
            question_index_length=args.question_index_length,
            answer_index_length=args.answer_index_length,
        )