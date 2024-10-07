#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import sys

import unicodedata

from tqdm.auto import tqdm

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_json_path', type=str)
    parser.add_argument('output_json_path', type=str)
    args = parser.parse_args()

    with open(args.input_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    count = 0
    new_rows = []
    for i, row in enumerate(tqdm(data)):
        pass
        #print(row)
        ref = row['meta']['output-reference']
        if ref:
            #print(ref)
            new_ref = []
            for str_ref_elem in ref:
                zenkaku = False
                for c in str_ref_elem:
                    if unicodedata.east_asian_width(c) in [
                        'F', # 全角英数など
                        'H', # 半角カナなど
                        'W', # 全角漢字・かななど
                        'A', # ギリシャ文字など
                        'N', # アラビア文字など
                        #'Na', # 除外: 半角英数など
                    ]:
                        zenkaku = True
                        break
                if zenkaku:
                    count += 1
                    #print(str_ref_elem)
                    #new_ref.append('NG: ' + str_ref_elem)
                else:
                    new_ref.append(str_ref_elem)
                    #new_ref.append('OK: ' + str_ref_elem)
            if new_ref:
                row['meta']['output-reference'] = new_ref
            else:
                row['meta']['output-reference'] = None
        #if i > 100:
        #    break
        #if count > 100:
        #    break
        new_rows.append(row)
    with open(args.output_json_path, 'w', encoding='utf-8') as f:
        json.dump(new_rows, f, ensure_ascii=False, indent=2)
    
    sys.exit(0)
