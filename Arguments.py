import os
import re
from abc import ABC, abstractmethod
from argparse import ArgumentParser as ArgParse, Action, ArgumentError
from typing import List

from attrs import define, field, converters

import yaml


@define(kw_only=True)
class Arguments:
    project_id: str
    customer_name: str
    #customers have read access, Customer's have write, and extra customers have none
    standard_datasets: List[str] = field(converter=converters.default_if_none(default=["ONX360", "ONXData", "ONXFocus", "ONXSpotlight", "Region_Files"])) 
    customer_datasets: List[str]
    extra_datasets: List[str] = field(converter=converters.default_if_none(default=["Opensignal", "Logs"]))
    project_quota: int = field(converter=converters.default_if_none(default=40 * 1024 ** 2))
    user_quota: int = field(converter=converters.default_if_none(default=10 * 1024 ** 2))

    def __attrs_post_init__(self):
        if self.customer_datasets is None:
            self.customer_datasets: List[str] = [self.customer_name]


class RegexAction(Action, ABC):
    def __call__(self, parser, namespace, values, option_string=None):
        # https://cloud.google.com/resource-manager/reference/rest/v3/projects
        # View `projectId` field for description of a valid id
        match = re.match(self.get_regex(), values)
        if match is None:
            raise ValueError(self.get_message())
        setattr(namespace, self.dest, values)

    @abstractmethod
    def get_regex(self):
        pass

    @abstractmethod
    def get_message(self):
        pass


class ProjectAction(RegexAction):
    def get_regex(self):
        return r"^[a-z][a-z0-9-]{5,29}(?<!-)$"

    def get_message(self):
        return "Project ID must be 6 to 30 lowercase letters, digits, or hyphens. It must start with a letter. Trailing hyphens are prohibited."


class CustomerNameAction(RegexAction):
    def get_regex(self):
        return r"^[a-zA-Z0-9][a-zA-Z0-9_]{0,1023}$"

    def get_message(self):
        return "Customer Name must be 1 to 1024 letters, digits, or underscores. It must start with a letter."


class ConfigFileAction(Action):
    def __call__(self, parser, namespace, values, option_string=None):
        try:
            with open(os.path.expanduser(values), 'r') as handle:
                for k, v in yaml.safe_load(handle).items():
                    setattr(namespace, k, v)
        except Exception as e:
            raise ValueError(e)


class ArgumentParser:
    def __init__(self):
        self.parser = ArgParse(description="Create a new Google Cloud Console setup for a customer")

        self.parser.add_argument("--project-id", "-p",
                                 dest="project_id",
                                 help="The name of the project.",
                                 action=ProjectAction,
                                 required=False)

        self.parser.add_argument("--customer-name", "-c",
                                 dest="customer_name",
                                 help="The name of the customer.",
                                 action=CustomerNameAction,
                                 required=False)

        self.parser.add_argument("--standard-datasets", "--sd",
                                 dest="standard_datasets",
                                 help="Standard datasets that customer has read access to",
                                 action="append",
                                 required=False),

        self.parser.add_argument("--customer-datasets", "--cd",
                                 dest="customer_datasets",
                                 help="Customers datasets that customer has read/write access to",
                                 action="append",
                                 required=False),

        self.parser.add_argument("--extra-datasets", "--ed",
                                 dest="extra_datasets",
                                 help="Extra datasets that customer has no access to",
                                 action="append",
                                 required=False),

        self.parser.add_argument("--project-quota", "--pq",
                                 dest="project_quota",
                                 help="Project Daily Query Quota in MB",
                                 required=False),

        self.parser.add_argument("--user-quota", "--uq",
                                 dest="user_quota",
                                 help="Project Daily User Query Quota in MB",
                                 required=False),

        self.parser.add_argument("--config",
                                 action=ConfigFileAction
                                 )

    def parse(self, argv) -> Arguments:
        args = self.parser.parse_args(argv)
        return Arguments(
            project_id=args.project_id,
            customer_name=args.customer_name,
            standard_datasets=args.standard_datasets,
            customer_datasets=args.customer_datasets,
            extra_datasets=args.extra_datasets,
            project_quota=args.project_quota,
            user_quota=args.user_quota,
        )

    def help(self):
        self.parser.print_help()
