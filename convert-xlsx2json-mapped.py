#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os

import pandas as pd

from logzero import logger
from tqdm.auto import tqdm

def convert_xlsx2json_mapped(
    #input_xlsx_path: str,
    input_xlsx_paths: list[str],
    #output_json_path: str,
    json_lines: bool,
    str_mapping: str,
):
    #with open(mapping_json_path, 'r', encoding='utf-8') as f:
    #    mapping = json.load(f)
    mapping = []
    for i, field in enumerate(str_mapping.split(',')):
        #key, value = field.split(':')
        #mapping.append((key, value))
        if ':' in field:
            #key, value = field.split(':')
            source, target = field.split(':')
        else:
            #key = i
            #value = field
            source = i
            target = field
        #mapping.append((key, value))
        mapping.append((source, target))
    logger.debug('mapping: %s', mapping)
    for input_xlsx_path in input_xlsx_paths:
        #df = pd.read_excel(input_xlsx_path)
        df = pd.read_excel(input_xlsx_path, header=None)
        #logger.debug('df: %s', df)
        #logger.debug('df.keys: %s', df.keys())
        data = []
        for i, row in tqdm(
            df.iterrows(),
            total=len(df),
            desc=f'Processing rows from XLSX: {input_xlsx_path}',
        ):
            new_row = {}
            #for key, value in mapping.items():
            #for key, value in mapping:
            #    new_row[key] = row[value]
            for source, target in mapping:
                new_row[target] = row[source]
            data.append(new_row)
        base_output_json_path = os.path.splitext(input_xlsx_path)[0]
        if json_lines:
            output_json_path = f'{base_output_json_path}.jsonl'
            with open(output_json_path, 'w', encoding='utf-8') as f:
                for row in data:
                    f.write(json.dumps(row, ensure_ascii=False))
                    f.write('\n')
        else:
            output_json_path = f'{base_output_json_path}.json'
            with open(output_json_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    #parser.add_argument('input_xlsx_path', type=str)
    parser.add_argument(
        'input_xlsx_paths',
        nargs='+',
        metavar='input_xlsx_path',
        help='Input XLSX files',
    )
    parser.add_argument(
        '--json-lines', '--lines', '-L',
        action='store_true',
        help='Ouput as JSON Lines',
    )
    #parser.add_argument('output_json_path', type=str)
    parser.add_argument(
        '--mapping', '-M',
        dest='mapping',
        required=True,
        help='Comma separated mapping: source1:target1,source2:target2,... or target1,target2,... (example: ID,text,output))',
    )
    args = parser.parse_args()
    logger.debug('args: %s', args)

    convert_xlsx2json_mapped(
        args.input_xlsx_paths,
        #args.output_json_path,
        args.json_lines,
        args.mapping
    )