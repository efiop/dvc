"""DVC config objects."""

from __future__ import unicode_literals

from dvc.utils.compat import str, open, urlparse

import os
import errno
import configobj
import logging

from schema import Schema, Optional, And, Use, Regex
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


class ConfigError(DvcException):
    """DVC config exception.

    Args:
        msg (str): error message.
        ex (Exception): optional exception that has caused this error.
    """

    def __init__(self, msg, ex=None):
        super(ConfigError, self).__init__(
            "config file error: {}".format(msg), ex
        )


def supported_cache_type(types):
    """Checks if link type config option has a valid value.

    Args:
        types (list/string): type(s) of links that dvc should try out.
    """
    if isinstance(types, str):
        types = [typ.strip() for typ in types.split(",")]
    for typ in types:
        if typ not in ["reflink", "hardlink", "symlink", "copy"]:
            return False
    return True


def supported_loglevel(level):
    """Checks if log level config option has a valid value.

    Args:
        level (str): log level name.
    """
    return level in ["info", "debug", "warning", "error"]


def supported_cloud(cloud):
    """Checks if obsoleted cloud option has a valid value.

    Args:
        cloud (str): cloud type name.
    """
    return cloud in ["aws", "gcp", "local", ""]


def is_bool(val):
    """Checks that value is a boolean.

    Args:
        val (str): string value verify.

    Returns:
        bool: True if value stands for boolean, False otherwise.
    """
    return val.lower() in ["true", "false"]


def to_bool(val):
    """Converts value to boolean.

    Args:
        val (str): string to convert to boolean.

    Returns:
        bool: True if value.lower() == 'true', False otherwise.
    """
    return val.lower() == "true"


def is_whole(val):
    """Checks that value is a whole integer.

    Args:
        val (str): number string to verify.

    Returns:
        bool: True if val is a whole number, False otherwise.
    """
    return int(val) >= 0


def is_percent(val):
    """Checks that value is a percent.

    Args:
        val (str): number string to verify.

    Returns:
        bool: True if 0<=value<=100, False otherwise.
    """
    return int(val) >= 0 and int(val) <= 100


class Config(object):  # pylint: disable=too-many-instance-attributes
    """Class that manages configuration files for a dvc repo.

    Args:
        dvc_dir (str): optional path to `.dvc` directory, that is used to
            access repo-specific configs like .dvc/config and
            .dvc/config.local.
        validate (bool): optional flag to tell dvc if it should validate the
            config or just load it as is. 'True' by default.


    Raises:
        ConfigError: thrown when config has an invalid format.
    """

    APPNAME = "dvc"
    APPAUTHOR = "iterative"

    # NOTE: used internally in RemoteLOCAL to know config
    # location, that url should resolved relative to.
    PRIVATE_CWD = "_cwd"

    DVC_DIR = ".dvc"

    CONFIG = "config"
    CONFIG_LOCAL = "config.local"

    SECTION_CORE = "core"
    SECTION_CORE_LOGLEVEL = "loglevel"
    SECTION_CORE_LOGLEVEL_SCHEMA = And(Use(str.lower), supported_loglevel)
    SECTION_CORE_REMOTE = "remote"
    SECTION_CORE_INTERACTIVE_SCHEMA = And(str, is_bool, Use(to_bool))
    SECTION_CORE_INTERACTIVE = "interactive"
    SECTION_CORE_ANALYTICS = "analytics"
    SECTION_CORE_ANALYTICS_SCHEMA = And(str, is_bool, Use(to_bool))

    SECTION_CACHE = "cache"
    SECTION_CACHE_DIR = "dir"
    SECTION_CACHE_TYPE = "type"
    SECTION_CACHE_TYPE_SCHEMA = supported_cache_type
    SECTION_CACHE_PROTECTED = "protected"
    SECTION_CACHE_LOCAL = "local"
    SECTION_CACHE_S3 = "s3"
    SECTION_CACHE_GS = "gs"
    SECTION_CACHE_SSH = "ssh"
    SECTION_CACHE_HDFS = "hdfs"
    SECTION_CACHE_AZURE = "azure"
    SECTION_CACHE_SCHEMA = {
        Optional(SECTION_CACHE_LOCAL): str,
        Optional(SECTION_CACHE_S3): str,
        Optional(SECTION_CACHE_GS): str,
        Optional(SECTION_CACHE_HDFS): str,
        Optional(SECTION_CACHE_SSH): str,
        Optional(SECTION_CACHE_AZURE): str,
        Optional(SECTION_CACHE_DIR): str,
        Optional(SECTION_CACHE_TYPE, default=None): SECTION_CACHE_TYPE_SCHEMA,
        Optional(SECTION_CACHE_PROTECTED, default=False): And(
            str, is_bool, Use(to_bool)
        ),
        Optional(PRIVATE_CWD): str,
    }

    # backward compatibility
    SECTION_CORE_CLOUD = "cloud"
    SECTION_CORE_CLOUD_SCHEMA = And(Use(str.lower), supported_cloud)
    SECTION_CORE_STORAGEPATH = "storagepath"

    SECTION_CORE_SCHEMA = {
        Optional(SECTION_CORE_LOGLEVEL, default="info"): And(
            str, Use(str.lower), SECTION_CORE_LOGLEVEL_SCHEMA
        ),
        Optional(SECTION_CORE_REMOTE, default=""): And(str, Use(str.lower)),
        Optional(
            SECTION_CORE_INTERACTIVE, default=False
        ): SECTION_CORE_INTERACTIVE_SCHEMA,
        Optional(
            SECTION_CORE_ANALYTICS, default=True
        ): SECTION_CORE_ANALYTICS_SCHEMA,
        # backward compatibility
        Optional(SECTION_CORE_CLOUD, default=""): SECTION_CORE_CLOUD_SCHEMA,
        Optional(SECTION_CORE_STORAGEPATH, default=""): str,
    }

    # backward compatibility
    SECTION_AWS = "aws"
    SECTION_AWS_STORAGEPATH = "storagepath"
    SECTION_AWS_CREDENTIALPATH = "credentialpath"
    SECTION_AWS_ENDPOINT_URL = "endpointurl"
    SECTION_AWS_LIST_OBJECTS = "listobjects"
    SECTION_AWS_REGION = "region"
    SECTION_AWS_PROFILE = "profile"
    SECTION_AWS_USE_SSL = "use_ssl"
    SECTION_AWS_SCHEMA = {
        SECTION_AWS_STORAGEPATH: str,
        Optional(SECTION_AWS_REGION): str,
        Optional(SECTION_AWS_PROFILE): str,
        Optional(SECTION_AWS_CREDENTIALPATH): str,
        Optional(SECTION_AWS_ENDPOINT_URL): str,
        Optional(SECTION_AWS_LIST_OBJECTS, default=False): And(
            str, is_bool, Use(to_bool)
        ),
        Optional(SECTION_AWS_USE_SSL, default=True): And(
            str, is_bool, Use(to_bool)
        ),
    }

    # backward compatibility
    SECTION_GCP = "gcp"
    SECTION_GCP_STORAGEPATH = SECTION_AWS_STORAGEPATH
    SECTION_GCP_CREDENTIALPATH = SECTION_AWS_CREDENTIALPATH
    SECTION_GCP_PROJECTNAME = "projectname"
    SECTION_GCP_SCHEMA = {
        SECTION_GCP_STORAGEPATH: str,
        Optional(SECTION_GCP_PROJECTNAME): str,
    }

    # backward compatibility
    SECTION_LOCAL = "local"
    SECTION_LOCAL_STORAGEPATH = SECTION_AWS_STORAGEPATH
    SECTION_LOCAL_SCHEMA = {SECTION_LOCAL_STORAGEPATH: str}

    SECTION_AZURE_CONNECTION_STRING = "connection_string"

    SECTION_REMOTE_REGEX = r'^\s*remote\s*"(?P<name>.*)"\s*$'
    SECTION_REMOTE_FMT = 'remote "{}"'
    SECTION_REMOTE_URL = "url"
    SECTION_REMOTE_USER = "user"
    SECTION_REMOTE_PORT = "port"
    SECTION_REMOTE_KEY_FILE = "keyfile"
    SECTION_REMOTE_TIMEOUT = "timeout"
    SECTION_REMOTE_PASSWORD = "password"
    SECTION_REMOTE_ASK_PASSWORD = "ask_password"
    SECTION_REMOTE_SCHEMA = {
        SECTION_REMOTE_URL: str,
        Optional(SECTION_AWS_REGION): str,
        Optional(SECTION_AWS_PROFILE): str,
        Optional(SECTION_AWS_CREDENTIALPATH): str,
        Optional(SECTION_AWS_ENDPOINT_URL): str,
        Optional(SECTION_AWS_LIST_OBJECTS, default=False): And(
            str, is_bool, Use(to_bool)
        ),
        Optional(SECTION_AWS_USE_SSL, default=True): And(
            str, is_bool, Use(to_bool)
        ),
        Optional(SECTION_GCP_PROJECTNAME): str,
        Optional(SECTION_CACHE_TYPE): SECTION_CACHE_TYPE_SCHEMA,
        Optional(SECTION_CACHE_PROTECTED, default=False): And(
            str, is_bool, Use(to_bool)
        ),
        Optional(SECTION_REMOTE_USER): str,
        Optional(SECTION_REMOTE_PORT): Use(int),
        Optional(SECTION_REMOTE_KEY_FILE): str,
        Optional(SECTION_REMOTE_TIMEOUT): Use(int),
        Optional(SECTION_REMOTE_PASSWORD): str,
        Optional(SECTION_REMOTE_ASK_PASSWORD): And(str, is_bool, Use(to_bool)),
        Optional(SECTION_AZURE_CONNECTION_STRING): str,
        Optional(PRIVATE_CWD): str,
    }

    SECTION_PKG_FMT = 'pkg "{}"'
    SECTION_PKG_URL = SECTION_REMOTE_URL
    SECTION_PKG_SCHEMA = {SECTION_PKG_URL: str}

    SECTION_STATE = "state"
    SECTION_STATE_ROW_LIMIT = "row_limit"
    SECTION_STATE_ROW_CLEANUP_QUOTA = "row_cleanup_quota"
    SECTION_STATE_SCHEMA = {
        Optional(SECTION_STATE_ROW_LIMIT): And(Use(int), is_whole),
        Optional(SECTION_STATE_ROW_CLEANUP_QUOTA): And(Use(int), is_percent),
    }

    SCHEMA = {
        Optional(SECTION_CORE, default={}): SECTION_CORE_SCHEMA,
        Optional(Regex(SECTION_REMOTE_REGEX)): SECTION_REMOTE_SCHEMA,
        Optional(SECTION_CACHE, default={}): SECTION_CACHE_SCHEMA,
        Optional(SECTION_STATE, default={}): SECTION_STATE_SCHEMA,
        # backward compatibility
        Optional(SECTION_AWS, default={}): SECTION_AWS_SCHEMA,
        Optional(SECTION_GCP, default={}): SECTION_GCP_SCHEMA,
        Optional(SECTION_LOCAL, default={}): SECTION_LOCAL_SCHEMA,
    }

    def __init__(
        self,
        root_dir=None,
        validate=True,
        system=False,
        glob=False,
        local=False,
        merge=True,
    ):
        try:
            self.root_dir = os.path.abspath(
                os.path.realpath(self.find_root(root_dir))
            )
            self.dvc_dir = os.path.join(self.root_dir, self.DVC_DIR)
        except NotDvcRepoError:
            self.root_dir = None
            self.dvc_dir = None

        system_file = os.path.join(self.get_system_dir(), self.CONFIG)
        global_file = os.path.join(self.get_global_dir(), self.CONFIG)

        if self.dvc_dir is not None:
            repo_file = os.path.join(self.dvc_dir, self.CONFIG)
            local_file = os.path.join(self.dvc_dir, self.CONFIG_LOCAL)
        else:
            repo_file = None
            local_file = None

        system = self._load_config(system_file)
        glob = self._load_config(global_file)
        repo = self._load_config(repo_file)
        local = self._load_config(local_file)

        configs = [local, repo, glob, system]

        if system:
            self.config = self._system_config
        elif glob:
            self.config = self._global_config
        elif local:
            self.config = self._local_config
        elif merge:
            self.config = configobj.ConfigObj()
            for config in configs:
                self.config.merge(config)
        else:
            self.config = self._repo_config

        # NOTE: schema doesn't support ConfigObj.Section validation, so we
        # need to convert our config to dict before passing it to
        config_dict = dict(config)
        if validate:
            config_dict = Schema(self.SCHEMA).validate(config_dict)
            self.config.merge(config_dict)

        self._resolve_paths(self.config, self.config_file)

    @classmethod
    def find_root(cls, root=None):
        if root is None:
            root = os.getcwd()
        else:
            root = os.path.abspath(os.path.realpath(root))

        while True:
            dvc_dir = os.path.join(root, cls.DVC_DIR)
            if os.path.isdir(dvc_dir):
                return root
            if os.path.ismount(root):
                break
            root = os.path.dirname(root)
        raise NotDvcRepoError(root)

    @classmethod
    def find_dvc_dir(cls, root=None):
        root_dir = cls.find_root(root)
        return os.path.join(root_dir, cls.DVC_DIR)

    @staticmethod
    def get_global_config_dir():
        """Returns global config location. E.g. ~/.config/dvc/config.

        Returns:
            str: path to the global config directory.
        """
        from appdirs import user_config_dir

        return user_config_dir(
            appname=Config.APPNAME, appauthor=Config.APPAUTHOR
        )

    @staticmethod
    def get_system_config_dir():
        """Returns system config location. E.g. /etc/dvc.conf.

        Returns:
            str: path to the system config directory.
        """
        from appdirs import site_config_dir

        return site_config_dir(
            appname=Config.APPNAME, appauthor=Config.APPAUTHOR
        )

    @staticmethod
    def init(dvc_dir):
        """Initializes dvc config.

        Args:
            dvc_dir (str): path to .dvc directory.

        Returns:
            dvc.config.Config: config object.
        """
        config_file = os.path.join(dvc_dir, Config.CONFIG)
        open(config_file, "w+").close()
        return Config(dvc_dir)

    def _load_config(self, path):
        config = configobj.ConfigObj(path)
        config = self._lower(config)
        self._resolve_paths(config, path)
        return config

    @staticmethod
    def _resolve_path(path, config_file):
        assert os.path.isabs(config_file)
        config_dir = os.path.dirname(config_file)
        return os.path.abspath(os.path.join(config_dir, path))

    def _resolve_cache_path(self, config, fname):
        cache = config.get(self.SECTION_CACHE)
        if cache is None:
            return

        cache_dir = cache.get(self.SECTION_CACHE_DIR)
        if cache_dir is None:
            return

        cache[self.PRIVATE_CWD] = os.path.dirname(fname)

    def _resolve_paths(self, config, fname):
        if fname is None:
            return

        self._resolve_cache_path(config, fname)
        for section in config.values():
            if self.SECTION_REMOTE_URL not in section.keys():
                continue

            section[self.PRIVATE_CWD] = os.path.dirname(fname)

    @staticmethod
    def _get_key(conf, name, add=False):
        for k in conf.keys():
            if k.lower() == name.lower():
                return k

        if add:
            conf[name] = {}
            return name

        return None

    def get_remote_settings(self, name):
        import posixpath

        """
        Args:
            name (str): The name of the remote that we want to retrieve

        Returns:
            dict: The content beneath the given remote name.

        Example:
            >>> config = {'remote "server"': {'url': 'ssh://localhost/'}}
            >>> get_remote_settings("server")
            {'url': 'ssh://localhost/'}
        """
        settings = self.config[self.SECTION_REMOTE_FMT.format(name)]
        parsed = urlparse(settings["url"])

        # Support for cross referenced remotes.
        # This will merge the settings, giving priority to the outer reference.
        # For example, having:
        #
        #       dvc remote add server ssh://localhost
        #       dvc remote modify server user root
        #       dvc remote modify server ask_password true
        #
        #       dvc remote add images remote://server/tmp/pictures
        #       dvc remote modify images user alice
        #       dvc remote modify images ask_password false
        #       dvc remote modify images password asdf1234
        #
        # Results on a config dictionary like:
        #
        #       {
        #           "url": "ssh://localhost/tmp/pictures",
        #           "user": "alice",
        #           "password": "asdf1234",
        #           "ask_password": False,
        #       }
        #
        if parsed.scheme == "remote":
            reference = self.get_remote_settings(parsed.netloc)
            url = posixpath.join(reference["url"], parsed.path.lstrip("/"))
            merged = reference.copy()
            merged.update(settings)
            merged["url"] = url
            return merged

        return settings

    def remote_add(self, url, name, default, force):
        from dvc.remote import _get, RemoteLOCAL

        remote = _get({self.SECTION_REMOTE_URL: url})
        if remote == RemoteLOCAL and not url.startswith("remote://"):
            if not os.path.isabs(url):
                url = os.path.relpath(
                    url, os.path.dirname(self.config.filename)
                )

        section = self.SECTION_REMOTE_FMT.format(name)
        if (section in self.config.keys()) and not force:
            raise ConfigError(
                "Remote with name {} already exists. "
                "Use -f (--force) to overwrite remote "
                "with new value".format(self.args.name)
            )

        self.set(section, self.SECTION_REMOTE_URL, url)

        if default:
            logger.info("Setting '{}' as a default remote.".format(name))
            self.set(self.SECTION_CORE, self.SECTION_CORE_REMOTE, name)

    def _remove_default(self, config, name):
        core = self.config.get(self.SECTION_CORE, None)
        if core is None:
            return 0

        default = core.get(self.SECTION_CORE_REMOTE, None)
        if default is None:
            return 0

        if default == name:
            self.unset(self.SECTION_CORE, self.SECTION_CORE_REMOTE)

    def remote_remove(self, name):
        self.unset(SECTION_REMOTE_FMT.format(name))

        for config in [self.local, self.repo, self.glob, self.system]:
            self._remove_default(config, name)
            if config == self.config:
                break

    def remote_modify(self, name, option, value):
        self.set(SECTION_REMOTE_FMT.format(name), option, value)

    def get_pkg_settings(self, name):
        return self.config[self.SECTION_PKG_FMT.format(name)]

    def pkg_add(self, url, name):
        self.set(SECTION_PKG_FMT.format(name), SECTION_PKG_URL, url)

    def pkg_remove(self, name):
        self.unset(SECTION_PKG_FMT.format(name))

    def pkg_modify(self, name, option, value):
        self.set(SECTION_PKG_FMT.format(name), option, value)

    def unset(self, section, opt=None):
        """Unsets specified option and/or section in the config.

        Args:
            section (str): section name.
            opt (str): optional option name.
        """
        if section not in self.config.keys():
            raise ConfigError("section '{}' doesn't exist".format(section))

        if opt is None:
            del self.config[section]
            self.config.save()
            return

        if opt not in self.config[section].keys():
            raise ConfigError(
                "option '{}.{}' doesn't exist".format(section, opt)
            )
        del self.config[section][opt]

        if not self.config[section]:
            del self.config[section]

        self.config.save()

    def set(self, section, opt, value):
        """Sets specified option in the config.

        Args:
            section (str): section name.
            opt (str): option name.
            value: value to set option to.
        """
        if section not in self.config.keys():
            self.config[section] = {}

        config[section][opt] = value
        config.save()

    def show(self, section, opt):
        """Prints option value from the config.

        Args:
            section (str): section name.
            opt (str): option name.
        """
        if section not in self.config.keys():
            raise ConfigError("section '{}' doesn't exist".format(section))

        if opt not in self.config[section].keys():
            raise ConfigError(
                "option '{}.{}' doesn't exist".format(section, opt)
            )

        return self.config[section][opt]

    @staticmethod
    def _lower(config):
        for s_key in config.keys():
            s_value = config.pop(s_key)
            for key in s_value.keys():
                value = s_value.pop(key)
                s_value[key.lower()] = value
            config[s_key.lower()] = s_value
        return config
