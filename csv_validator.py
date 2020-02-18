# -*- coding: utf-8 -*-
from typing import Any, List, Dict, Optional, Union
from typedjson import DecodingError
import argparse
import typedjson
import json
import os
import pandas as pd
# from .type_json import ValidatesJson, ValidateJson, GroupByJson
# from py.type_json import CountJson, RelationJson
from logger import Logger, Error, ColumnError

class CsvValidator:
    """csvデータ検証 [Pandas 基本操作](https://qiita.com/ysdyt/items/9ccca82fc5b504e7913a)"""

    def __init__(self) -> None:
        self.errors: List[str] = []
        self.dir: str = ''
        self.csv_cache: Dict[str, pd.DataFrame] = { }

    # def assert_column(self, csv_columns: List[str], column: str) -> None:
    #     """カラムチェック raise: ColumnError"""
    #     if not column in csv_columns:
    #         raise ColumnError(f"\'{column}\' csvに存在しないカラムです csv: {self.csv_columns}")

    def read_table(self, dir: str, path: str) -> pd.DataFrame:
        """csvテーブル読み込み キャッシュする raise Exception"""
        path = os.path.join(dir, path)
        if path in self.csv_cache:
            return self.csv_cache[path]
        try:
            if not os.path.exists(path):
                Logger.add(f"{path}が存在しません", error=Error.path)
                raise Exception
            self.csv_cache[path] = pd.read_csv(path, sep=',')
            return self.csv_cache[path]

        except pd.errors.ParserError as e:
            Logger.add(e, error=Error.parse)
            raise Exception

    def parse(self, dir: str, root: Any) -> None:
        """jsonパース: csvの親パス, jsonノード"""
        key_list: List[str] = root.keys()
        path = root.get("path")
        if path is None:
            print("error")

        test_list = []
        for key in key_list:
            if key.startswith("test:"):
                test_list.append(root[key])

        table: pd.DataFrame = self.read_table(dir, path)
        for test in test_list:
            query_df = table
            where_df = table
            where = test.get("where")
            if where is not None:
                where_df = where_df.query(where)
            query = test.get("query")
            if query is not None:
                for q in query:
                    query_df = query_df.query(q)

            res = self.diff_minus(where_df, query_df)
            print(res)
        
    def diff_minus(self, df1, df2):
        return df1[~df1.isin(df2.values.reshape(-1)).all(1)]

    # def parse_validate(self, table: pd.DataFrame, validate_json: Any) -> None:
    #     """validateパース: csvテーブル, jsonノード"""
    #     if validate_json.query is not None:
    #         self.parse_query(table, validate_json.query)

    #     if validate_json.groupby is not None:
    #         self.parse_groupby(table, validate_json.groupby)

    #     if validate_json.count is not None:
    #         self.parse_count(table, validate_json.count)
        
    #     if validate_json.relation is not None:
    #         self.parse_relation(table, validate_json.relation)

    # def parse_query(self, table: pd.DataFrame, query: str) -> None:
    #     """クエリ実行: csvテーブル, クエリ文"""
    #     try:
    #         res: pd.DataFrame = table.query(f"~({query})")
    #         # res: pd.DataFrame = table.query(query)
    #         if not res.empty:
    #             Logger.add(res)
    #     except Exception as e:
    #         Logger.add(f"'{query}'", error=Error.query)

    # def parse_groupby(self, table: pd.DataFrame, groupby_json: Any) -> None:
    #     """グループ化: csvテーブル, jsonノード, グループ化するカラム"""
    #     self.assert_column(table.columns.values, groupby_json.key)
    #     for _, group_table in table.groupby(groupby_json.key):
    #         if groupby_json.duplicate is not None:
    #             self.assert_column(table.columns.values, groupby_json.duplicate)
    #             self.parse_duplicate(group_table, groupby_json.duplicate)

    #         if groupby_json.query is not None:
    #             self.parse_query(group_table, groupby_json.query)

    # def parse_duplicate(self, table: pd.DataFrame, key: str) -> None:
    #     """重複チェック: csvテーブル, 重複チェックするカラム"""
    #     res: pd.DataFrame = table[table[key].duplicated()]
    #     if not res.empty:
    #         Logger.add(res)

    # def parse_count(self, table: pd.DataFrame, count_json: Any) -> None:
    #     """同一の値をカウント: csvテーブル, jsonノード"""
    #     self.assert_column(table.columns.values, count_json.key)
    #     if (count_json.type == "min"):
    #         (_, count) = min(table[count_json.key].value_counts().iteritems())
    #     elif (count_json.type == "max"):
    #         (_, count) = max(table[count_json.key].value_counts().iteritems())
    #     else:
    #         Logger.add("type {count_json.type}存在しないタイプです。 type: [max, min]", error=Error.string)
    #         return

    #     eval_str = f"{count_json.eval}".format(count)
    #     try:
    #         res = eval(eval_str)
    #         if res:
    #             Logger.add(f"{eval_str}")
    #     except Exception as e:
    #         Logger.add("eval 失敗", error=Error.parse)

    # def parse_relation(self, table: pd.DataFrame, relation_json: Any) -> None:
    #     self.assert_column(table.columns.values, relation_json.from_key)
    #     relation_table = self.read_table(relation_json.to_path)
    #     self.assert_column(relation_table.columns.values, relation_json.to_key)

    #     from_values: List[str] = table[relation_json.from_key].values
    #     to_values: List[str] = relation_table[relation_json.to_key].values
    #     for from_ in from_values:
    #         if not any([from_ == to for to in to_values]):
    #             s = f"{relation_json.to_path}の{relation_json.to_key}に{from_}がありません"
    #             Logger.add(s)

    def run(self, dir: str, path: str) -> None:
        """検証実行: csv用親ディレクトリ, jsonパス"""
        if not os.path.exists(dir):
            Logger.exit(f"{dir} 作業ディレクトリが存在しません", error=Error.path)

        if not os.path.exists(path):
            Logger.exit(f"{path} パスが存在しません", error=Error.path)

        try:
            with open(path, 'r', encoding='UTF-8') as f:
                print(f"### {path} ###")
                node = json.load(f)
        except json.JSONDecodeError as e:
            Logger.exit("json decode error.", error=Error.parse)
            return
        except Exception as e:
            Logger.exit(e, error=Error.unknown)
            return
        self.parse(dir, node)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="csvをjsonの検証データで検証する")
    parser.add_argument('-c', '--csv-dir', type=str, default='./', help='親ディレクトリ')
    parser.add_argument('-j', '--json', type=str, default='./validate.json', help='検証用json')
    args = parser.parse_args()

    validator = CsvValidator()
    validator.run(args.csv_dir, args.json)
    # df = pd.DataFrame({
    #     'id': [1, 2, 3, 4, 5, 6],
    #     'name': ['荒岩 一味', '荒岩 虹子', '荒岩 まこと', '荒岩 みゆき', '田中 一', '梅田 よしお'],
    #     'age': [44, 44, 21, 13, 36, 31],
    #     'sex': ['M', 'F', 'M', 'F', 'M', 'M'],
    # })
    # print(df.query("~(id == 1)"))
