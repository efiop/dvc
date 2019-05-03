from __future__ import unicode_literals

import os
import argparse
import logging

from dvc.command.base import CmdBase, append_doc_link
from dvc.config import Config
from dvc.exceptions import DvcException


logger = logging.getLogger(__name__)


class CmdConfig(CmdBase):
    def __init__(self, args):
        self.args = args

    def run_cmd(self):
        return self.run()

    def run(self):
        section, opt = self.args.name.lower().strip().split(".", 1)

        kwargs = [
            "validate": False,
            "system": self.args.system,
            "glob": self.args.glob,
            "local": self.args.local,
            "merge": False,
        ]
        if self.args.unset:
            config = Config(**kwargs)
            config.unset(section, opt)
        elif self.args.value is None:
            kwargs["merge"] = True
            config = Config(**kwargs)
            logger.info(config.show(section, opt))
        else:
            config = Config(**kwargs)
            config.set(section, opt, self.args.value)


parent_config_parser = argparse.ArgumentParser(add_help=False)
parent_config_parser.add_argument(
    "--global",
    dest="glob",
    action="store_true",
    default=False,
    help="Use global config.",
)
parent_config_parser.add_argument(
    "--system", action="store_true", default=False, help="Use system config."
)
parent_config_parser.add_argument(
    "--local", action="store_true", default=False, help="Use local config."
)


def add_parser(subparsers, parent_parser):
    CONFIG_HELP = "Get or set config options."

    config_parser = subparsers.add_parser(
        "config",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(CONFIG_HELP, "config"),
        help=CONFIG_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    config_parser.add_argument(
        "-u",
        "--unset",
        default=False,
        action="store_true",
        help="Unset option.",
    )
    config_parser.add_argument("name", help="Option name.")
    config_parser.add_argument(
        "value", nargs="?", default=None, help="Option value."
    )
    config_parser.set_defaults(func=CmdConfig)
