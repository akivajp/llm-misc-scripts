#!/usr/bin/env python3

import json

import pandas as pd
from logzero import logger

def set_value(d, j, key, delimiter=None):
    if j is None:
        return
    if key not in j:
        return
    value = j[key]
    if value is None:
        d[key] = None
    if isinstance(value, list):
        if delimiter is not None:
            d[key] = delimiter.join(value)
    else:
        d[key] = value

def export_to_table(
    input_json_paths: list[str],
    export_path,
):
    convert = None
    if export_path.endswith('.xlsx'):
        convert = lambda df: df.to_excel(export_path, index=False)
    elif export_path.endswith('.csv'):
        convert = lambda df: df.to_csv(export_path, index=False, encoding='utf-8-sig')
    else:
        raise ValueError(f'Invalid export_path: {export_path}')
    all_rows = []
    for input_json_path in input_json_paths:
        if input_json_path.endswith('.jsonl'):
            with open(input_json_path, 'r', encoding='utf-8') as f:
                rows = [json.loads(line) for line in f]
        elif input_json_path.endswith('.json'):
            with open(input_json_path, 'r', encoding='utf-8') as f:
                rows = json.load(f)
        else:
            raise ValueError(f'Invalid input_json_path: {input_json_path}')
        for row in rows:
            #meta = row['meta']
            meta = row.get('meta')
            flat_row = {}
            #flat_row = {
            #    'ID': row['ID'],
            #    'text-producer': meta.get('text-producer'),
            #    'output-producer': meta.get('output-producer'),
            #    'text': row['text'],
            #    'output': row['output'],
            #    'task': str.join(';', meta['task']),
            #    'perspective': str.join(';', meta['perspective']),
            #    'time-dependency': meta['time-dependency'],
            #    'domain': str.join(';', meta['domain']),
            #    'source-to-answer': str.join(';', meta['source-to-answer']),
            #    'output-type': str.join(';', meta['output-type']),
            #    'alert-type': str.join(';', meta['alert-type']),
            #    #'output-reference': str.join('\n', meta['output-reference']),
            #    'output-reference': output_reference,
            #}
            set_value(flat_row, row,  'ID')
            set_value(flat_row, row,  'question-ID')
            set_value(flat_row, meta, 'text-producer')
            set_value(flat_row, meta, 'output-producer')
            set_value(flat_row, row,  'text')
            set_value(flat_row, row,  'output')
            set_value(flat_row, meta, 'task', delimiter=';')
            set_value(flat_row, meta, 'perspective', delimiter=';')
            set_value(flat_row, meta, 'time-dependency')
            set_value(flat_row, meta, 'domain', delimiter=';')
            set_value(flat_row, meta, 'source-to-answer', delimiter=';')
            set_value(flat_row, meta, 'output-type', delimiter=';')
            set_value(flat_row, meta, 'alert-type', delimiter=';')
            set_value(flat_row, meta, 'output-reference', delimiter='\n')
            set_value(flat_row, meta, 'point-allocation', delimiter=';')
            all_rows.append(flat_row)
    df = pd.DataFrame(all_rows)
    logger.debug('df: %s', df)
    convert(df)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('input_files', nargs='+', help='Input JSON (JSON Lines) files')
    parser.add_argument(
        '--output', '-O',
        type=str,
        help='Output CSV/Excel file',
    )
    args = parser.parse_args()
    logger.debug('args: %s', args)
    export_to_table(
        args.input_files,
        args.output,
    )
