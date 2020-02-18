# -*- coding: utf-8 -*-
from enum import Enum
from typing import Any, List, Dict, Optional, Union

class ColumnError(Exception):
    """csvに存在しないカラムを指定"""
    pass

class Error(Enum):
    parse = "[parse error]"
    path = "[path error]"
    query = "[query error]"
    string = "[string error]"
    column = "[column error]"
    unknown = "[unknown error]"

class Logger:
    __logs: List[str] = []

    @staticmethod
    def add(message: Union[str, Exception], error : Optional[Error] = None) -> None:
        """ログ追加"""
        if error is None:
            Logger.__logs.append(f"{message}")
        else:
            Logger.__logs.append(f"{error.value} {message}")

    @staticmethod
    def exit(message: Union[str, Exception], error: Error) -> None:
        Logger.add(message, error=error)
        for log in Logger.__logs:
            print(log)
        exit(1)
    
    @staticmethod
    def output(title: Optional[str] = None) -> None:
        """出力 エラーがない場合、titleに ✓ がつく"""
        if Logger.__logs:
            if title is not None:
                print(f"☓  {title}")
            for error in Logger.__logs:
                print(error)
            Logger.__logs = []
        else:
            print(f"✓  {title}")