# -*- coding: utf-8 -*-
import time
from dataclasses import dataclass
from collections.abc import Callable
from llm_core.assistants import LLaMACPPAssistant


NOT_FOUND = "No record found."
FAILED = "Failed to write record."
SUCCESS = "Record updated successfully"


@dataclass
class BaseSelector:
    key: str
    value: str


@dataclass
class BaseRecord:
    key: str
    value: str


@dataclass
class BaseDataSource:
    name: str
    read: Callable[[list[BaseSelector]], str]
    write: Callable[[list[BaseSelector], BaseRecord], str]


@dataclass
class BaseDataReadOperation:
    datasource: str
    selectors: list[BaseSelector]

    def __post_init__(self):
        self.selectors = [BaseSelector(**s) for s in self.selectors]


@dataclass
class BaseDataWriteOperation:
    datasource: str
    record: BaseRecord


class DemoRegistry:
    def __init__(self):
        self.data = []
        self.name = "demo-source"

    def read(self, selectors):
        try:
            record = next(
                filter(
                    lambda r: all(
                        [
                            r[selector.key] == selector.value
                            for selector in selectors
                        ]
                    ),
                    self.data,
                )
            )
        except StopIteration:
            return NOT_FOUND

        return record

    def write(self, selectors, record):
        if not selectors:
            self.data.append({record.key: record.value})
            return SUCCESS

        source_record = self.read(selectors)
        if source_record == NOT_FOUND:
            return FAILED

        source_record.update({record.key: record.value})
        return SUCCESS


registry_main = DemoRegistry()
registry_backup = DemoRegistry()
main_source = BaseDataSource("main", registry_main.read, registry_main.write)
backup_source = BaseDataSource(
    "backup", registry_backup.read, registry_backup.write
)

sources = {
    "registry_main": registry_main,
    "registry_backup": registry_backup,
}


@dataclass
class APIReadAssistant:
    system_prompt = "You are a helpful assistant."
    prompt = """
    # Available data sources

    {sources}

    # Informations

    You can retrieve data from data sources by providing a data read operation
    with the following schema:
    - name: data source name
    - selectors: a list of key, value pairs that will be used to find the
      relevant record

    # Query

    Translate the following query into an data read operation:

    {query}
    """

    operation: BaseDataReadOperation

    @classmethod
    def ask(cls, query):
        with LLaMACPPAssistant(cls) as assistant:
            response = assistant.process(
                query=query, sources=",".join(sources.keys())
            )
            print(response)
            return response

    @classmethod
    def retrieve(cls, query):
        response = cls.ask(query)
        op = response.operation
        return sources[op.datasource].read(op.selectors)


@dataclass
class APIWriteAssistant:
    system_prompt = "You are a helpful assistant."
    prompt = """
    # Available databases

    {sources}

    # Creating a new record

    You can create data by providing a data write
    operation with the following schema:

    - name: data source name
    - selectors: Should be an empty array when creating data.
    - record: a key, value pair to update the data

    # Updating an existing record

    You can update data by providing a data write
    operation with the following schema:

    - name: data source name
    - selectors: It should be set with key, value pairs to identify the
      record to update
    - record: a key, value pair to update the data

    # Query

    Translate the following query into serie of operations in a logical order (read and write):

    {query}
    """

    read_operation: BaseDataReadOperation
    write_operation: BaseDataWriteOperation

    @classmethod
    def ask(cls, query):
        with LLaMACPPAssistant(cls) as assistant:
            response = assistant.process(
                query=query, sources=",".join(sources.keys())
            )
            print(response)
            return response

    @classmethod
    def store(cls, query):
        response = cls.ask(query)
        read_operation = response.read_operation
        write_operation = response.write_operation
        return sources[write_operation.datasource].write(
            read_operation.selectors, write_operation.record
        )


# We need a way to inject tools + schema and then perform calls
# > generate a dataclass object from a signature ?
# > what do we do with the answer => re-inject in a secondary call
# > like a dynamic pipeline ?

# abstraction of a database => not useful when we can map services instead
# abstract a service layer ?
