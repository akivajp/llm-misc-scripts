#!/usr/bin/env python3

import argparse
import json

import pandas as pd

from logzero import logger

def convert_json_to_splitted_table(
    input_json_paths: list[str],
    output_prefix: str,
    split_lines: int,
):
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
        all_rows.extend(rows)
    logger.debug('# of all_rows: %d', len(all_rows))
    for i in range(0, len(all_rows), split_lines):
        #if i > 0:
        #    break
        #if i > split_lines:
        #    break
        rows = all_rows[i:i+split_lines]
        str_len = len(str(len(all_rows)))
        #start = i + 1
        start = f'{i+1:0{str_len}}'
        #end = i + len(rows)
        end = f'{i+len(rows):0{str_len}}'
        df = pd.DataFrame(rows)
        #logger.debug('df: %s', pd.DataFrame(rows))
        # NOTE: 標準の openpyxl では IllegalCharacterError が発生してしまう
        #df.to_excel(output_path, index=False)
        #df.to_excel(output_path, index=False, engine='xlsxwriter')
        # NOTE: xlsxwriter でのエラー対策
        #writer = pd.ExcelWriter(
        #    output_path,
        #    engine='xlsxwriter',
        #    #options={'strings_to_urls': False},
        #    engine_kwargs={
        #        'options': {
        #            'strings_to_urls': False,
        #            'strings_to_formulas': False,
        #        }
        #    },
        #)
        #df.to_excel(writer, index=False)
        #writer.close()
        # NOTE: 上記でも正常なExcelファイルとして開けない原因不明の問題があるためCSVで出力
        output_path = f'{output_prefix}_{start}-{end}.csv'
        logger.debug('output_path: %s', output_path)
        df.to_csv(output_path, index=False, encoding='utf-8-sig')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert JSON to splitted table.')
    parser.add_argument(
        '--input-json-paths', '-I',
        type=str, nargs='+',
        required=True,
        help='Input JSON paths.'
    )
    parser.add_argument(
        '--output-prefix', '-O',
        type=str,
        required=True,
        help='Output prefix'
    )
    parser.add_argument(
        '--split-lines', '-L',
        type=int,
        default=10000,
        help='Split lines'
    )
    args = parser.parse_args()
    #logger.debug('args: %s', args)
    convert_json_to_splitted_table(
        input_json_paths=args.input_json_paths,
        output_prefix=args.output_prefix,
        split_lines=args.split_lines,
    )
