#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import sys

DEFAULT_AXIS_FIELD = 'ID'

def diff_dicts(dict1, dict2, prefix='', depth=0):
    keys = set(dict1.keys()) | set(dict2.keys())
    indent = '  ' * (depth + 1)
    for key in keys:
        key_prefix = f'{prefix}{key}' if prefix else key
        if key not in dict1:
            print(f'{indent}{key_prefix} is not in dict1')
            #sys.exit(1)
        if key not in dict2:
            print(f'{indent}{key_prefix} is not in dict2')
            #sys.exit(1)
        #if dict1[key] != dict2[key]:
        #    print(f'{key_prefix} is different')
        #    print(f'{indent}dict1: {dict1[key]}')
        #    print(f'{indent}dict2: {dict2[key]}')
        #    #sys.exit(1)
        #if isinstance(dict1[key], dict):
        #    diff_dicts(dict1[key], dict2[key], key_prefix, depth+1)
        if isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
            diff_dicts(dict1[key], dict2[key], f'{key_prefix}.', depth+1)
        elif dict1[key] != dict2[key]:
            #print(f'{key_prefix} is different')
            print(f'{key_prefix} is changed')
            #print(f'{indent}dict1: {dict1[key]}')
            #print(f'{indent}dict2: {dict2[key]}')
            print(f'{indent}old: {dict1[key]}')
            print(f'{indent}new: {dict2[key]}')
            #sys.exit(1)

def main(
    input_json_path1: str,
    input_json_path2: str,
    axis_field: str,
):
    json1 = json.load(open(input_json_path1, 'r', encoding='utf-8'))
    json2 = json.load(open(input_json_path2, 'r', encoding='utf-8'))
    json1_dict = {row[axis_field]: row for row in json1}
    json2_dict = {row[axis_field]: row for row in json2}
    #keys = set(json1_dict.keys()) | set(json2_dict.keys())
    keys = set(json1_dict.keys()) | set(json2_dict.keys())
    for key in keys:
        if key not in json1_dict:
            print(f'Key {key} is not in {input_json_path1}')
            #sys.exit(1)
        if key not in json2_dict:
            print(f'Key {key} is not in {input_json_path2}')
            #sys.exit(1)
        #diff_dicts(json1_dict[key], json2_dict[key])
        diff_dicts(json1_dict[key], json2_dict[key], prefix=f'{key}.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_json_path1', help='Input JSON file 1')
    parser.add_argument('input_json_path2', help='Input JSON file 2')
    parser.add_argument(
        '--axis-field', '-A',
        type=str,
        default=DEFAULT_AXIS_FIELD,
        help='Axis field',
    )
    args = parser.parse_args()
    main(
        args.input_json_path1,
        args.input_json_path2,
        args.axis_field,
    )
