from __future__ import annotations

from enum import Enum
from typing import TypeVar, cast

EnumClassT = TypeVar("EnumClassT", bound=type[Enum])

try:
    from enum_tools.documentation import document_enum as _document_enum
except ImportError:

    def document_enum(enum_class: EnumClassT) -> EnumClassT:
        return enum_class

else:

    def document_enum(enum_class: EnumClassT) -> EnumClassT:
        return cast(EnumClassT, _document_enum(enum_class))
