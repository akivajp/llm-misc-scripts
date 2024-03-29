#!/usr/bin/env python3

import csv
import json

from logzero import logger
from tqdm.auto import tqdm

ENCODING_OPTIONS = [
    'utf-8',
    'utf-8-sig',
    'cp932',
]

EXCLUDE_COLUMNS = [
]

def convert_tsv2jsonl(
    input_file,
    jsonl_file,
    id_prefix,
    index_length,
    encoding = 'utf-8',
    tsv = False,
):
    #with open(tsv_file, 'r') as tsvfile:
    if tsv:
        delimiter = '\t'
    else:
        delimiter = ','
    with open(input_file, 'r', encoding=encoding) as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter=delimiter)
        rows = []
        with open(jsonl_file, 'w', encoding='utf-8') as jsonlfile:
            for i, row in enumerate(
                tqdm(reader),
            ):
                #if i == 0:
                #    logger.debug('row: %s', row)
                #    logger.debug('row.keys(): %s', row.keys())
                #if i >= 1:
                #    break
                q_id = row['QuestionID']
                #a_id = row['AnswerID']
                q = row['質問']
                a = row['回答']
                risk_area = row['risk-area']
                harm_type = row['harm-type']
                specific_harm = row['specific-harm']
                if not q or not a:
                    continue
                for exclude_column in EXCLUDE_COLUMNS:
                    if exclude_column in row:
                        del row[exclude_column]
                #id = 'answercarefully-instruction-000-001-0000001-001'
                #id = f'answercarefully-instruction-000-001-0000001-{q_id:03d}'
                d = dict(
                    ID=q_id,
                    text=q,
                    output=a,
                    meta = {
                        'risk-area': risk_area,
                        'harm-type': harm_type,
                        'specific-harm': specific_harm,
                    },
                )
                #logger.debug('d: %s', d)
                #jsonlfile.write(json.dumps(d, ensure_ascii=False) + '\n')
                rows.append(d)
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

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='Input CSV/TSV file')
    parser.add_argument('jsonl_file', help='Output JSONL file')
    parser.add_argument('--encoding', '-E', default='utf-8', help='Encoding of the TSV file')
    parser.add_argument('--tsv', '-T', action='store_true', help='Input TSV file')
    parser.add_argument('--id-prefix', '-P', required=True, help='ID prefix')
    parser.add_argument('--index-length', '-L', type=int, required=True, help='Index Length')
    args = parser.parse_args()
    convert_tsv2jsonl(
        args.input_file,
        args.jsonl_file,
        encoding=args.encoding,
        tsv=args.tsv,
        id_prefix=args.id_prefix,
        index_length=args.index_length,
    )
