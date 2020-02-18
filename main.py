# -*- coding: utf-8 -*-
from typing import Any, List, Dict, Optional, Union
from typedjson import DecodingError
import argparse
import typedjson
import json
import yaml
import os
import pandas as pd
from logging import getLogger, FileHandler, Formatter, INFO, WARN

# ログ
logger = getLogger('LoggingTest')
logger.setLevel(INFO)
handler = FileHandler(filename='test.log', encoding='utf-8', mode='w')
logger.addHandler(handler)

def read_json(path: str) -> Dict[str, Any]:
    # json読み込み
    if not os.path.exists(path):
        print("error.")
    with open(path, 'r', encoding='UTF-8') as f:
        return json.load(f)

def read_csv(path) -> pd.DataFrame:
    # csv読み込み
    if not os.path.exists(path):
        print("error.")
        return
    return pd.read_csv(path, sep=',')

def parse(json_: Dict[str, Any]) -> None:
    # test取得
    tests = []
    for key in json_.keys():
        if key.startswith("test:"):
            tests.append(json_[key])

    # csv読み込み
    path = json_.get("path")
    table = read_csv(path)

    for test in tests:
        e_table = table
        exclude  = test.get("exclude")
        if exclude is not None:
            for e in exclude:
                e_table = e_table.query(f"~({e})")
                logger.info(e)
                logger.info(e_table)

        q_table = reverse(table, e_table)
        query = test.get("query")
        if query is not None:
            for q in query:
                e_table = e_table.query(q)
                logger.info(q)
                logger.info(e_table)

    # res = reverse(q_table, e_table)
    res = e_table
    logger.info("res")
    logger.info(res)

def reverse(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    return df1[~df1.isin(df2.values.reshape(-1)).all(1)]

if __name__ == "__main__":
    # with open(path, 'r', encoding='UTF-8') as f:
    #     print(yaml.open(f))
    # return
    path: str = "test.json"
    json_: Dict[str, Any] = read_json(path)
    parse(json_)