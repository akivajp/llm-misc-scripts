#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json

def load_json_array(
    path,
):
    '''
    既存のJSONファイルから配列データを取得
    '''
    if path.endswith('.jsonl'):
        with open(path, 'r', encoding='utf-8') as f:
            rows = [json.loads(line) for line in f]
    elif path.endswith('.json'):
        with open(path, 'r', encoding='utf-8') as f:
            rows = json.load(f)
    else:
        raise ValueError(f'Invalid path: {path}')
    return rows
