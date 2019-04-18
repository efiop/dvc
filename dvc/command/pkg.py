from __future__ import unicode_literals

import argparse
import logging

from dvc.exceptions import DvcException
from dvc.command.base import CmdBase, fix_subparsers, append_doc_link


logger = logging.getLogger(__name__)


class CmdPkgAdd(CmdBase):
    def run(self):
        try:
            self.repo.pkg.add(self.args.url, name=self.args.name)
            return 0
        except DvcException:
            logger.exception(
                "failed to add package '{}'".format(self.args.url)
            )
            return 1


class CmdPkgInstall(CmdBase):
    def run(self):
        try:
            self.repo.pkg.install()
            return 0
        except DvcException:
            logger.exception("failed to install packages")
            return 1


class CmdPkgRemove(CmdBase):
    def run(self):
        try:
            self.repo.pkg.remove(self.args.name)
            return 0
        except DvcException:
            logger.exception(
                "failed to remove package '{}'".format(self.args.name)
            )
            return 1


class CmdPkgList(CmdBase):
    def run(self):
        try:
            self.repo.pkg.list()
            return 0
        except DvcException:
            logger.exception("failed to list packages")
            return 1


def add_parser(subparsers, parent_parser):
    from dvc.command.config import parent_config_parser

    PKG_HELP = "Manage DVC packages."
    pkg_parser = subparsers.add_parser(
        "pkg",
        parents=[parent_parser],
        description=append_doc_link(PKG_HELP, "pkg"),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )

    pkg_subparsers = pkg_parser.add_subparsers(
        dest="cmd", help="Use dvc pkg CMD --help for command-specific help."
    )

    fix_subparsers(pkg_subparsers)

    PKG_ADD_HELP = "Add package."
    pkg_add_parser = pkg_subparsers.add_parser(
        "add",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(PKG_ADD_HELP, "pkg-add"),
        help=PKG_ADD_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    pkg_add_parser.add_argument("url", help="Package URL.")
    pkg_add_parser.add_argument(
        "-n", "--name", help="Name to use for this package."
    )
    pkg_add_parser.set_defaults(func=CmdPkgAdd)

    PKG_INSTALL_HELP = "Install packages."
    pkg_install_parser = pkg_subparsers.add_parser(
        "install",
        parents=[parent_parser],
        description=append_doc_link(PKG_INSTALL_HELP, "pkg-install"),
        help=PKG_INSTALL_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    pkg_install_parser.set_defaults(func=CmdPkgInstall)

    PKG_REMOVE_HELP = "Remove package."
    pkg_remove_parser = pkg_subparsers.add_parser(
        "remove",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(PKG_REMOVE_HELP, "pkg-remove"),
        help=PKG_REMOVE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    pkg_remove_parser.add_argument("name", help="Package name.")
    pkg_remove_parser.set_defaults(func=CmdPkgRemove)

    PKG_LIST_HELP = "List packages."
    pkg_list_parser = pkg_subparsers.add_parser(
        "list",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(PKG_LIST_HELP, "pkg-list"),
        help=PKG_LIST_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    pkg_list_parser.set_defaults(func=CmdPkgList)
