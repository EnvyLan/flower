from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import atexit
import signal
import logging

from pprint import pformat

from tornado.options import options
from tornado.options import parse_command_line, parse_config_file
from tornado.log import enable_pretty_logging
from celery.bin.base import Command

from . import __version__
from .app import Flower
from .urls import settings
from .utils import abs_path, prepend_url
from .options import DEFAULT_CONFIG_FILE, default_options

try:
    from logging import NullHandler
except ImportError:
    from .utils.backports import NullHandler


logger = logging.getLogger(__name__)


class FlowerCommand(Command):
    ENV_VAR_PREFIX = 'FLOWER_'
    
    multi_celery_app = False
    
    def run_from_argv(self, prog_name, argv=None, **_kwargs):
        self.apply_env_options()
        self.apply_options(prog_name, argv)

        self.extract_settings()
        self.setup_logging()
        self.app.loader.import_default_modules()
        flower = Flower(capp=self.app, options=options, **settings)
        # flower程序退出时的注册函数
        atexit.register(flower.stop)

        def sigterm_handler(signal, frame):
            logger.info('SIGTERM detected, shutting down')
            sys.exit(0)
        signal.signal(signal.SIGTERM, sigterm_handler)

        self.print_banner('ssl_options' in settings)

        try:
            flower.start()
        except (KeyboardInterrupt, SystemExit):
            pass

    def handle_argv(self, prog_name, argv=None):
        return self.run_from_argv(prog_name, argv)

    def apply_env_options(self):
        "apply options passed through environment variables"
        env_options = filter(self.is_flower_envvar, os.environ)
        for env_var_name in env_options:
            name = env_var_name.replace(self.ENV_VAR_PREFIX, '', 1).lower()
            value = os.environ[env_var_name]
            try:
                option = options._options[name]
            except:
                option = options._options[name.replace('_', '-')]
            if option.multiple:
                value = [option.type(i) for i in value.split(',')]
            else:
                value = option.type(value)
            setattr(options, name, value)

    def apply_options(self, prog_name, argv):
        "apply options passed through the configuration file"
        argv = list(filter(self.is_flower_option, argv))
        # parse the command line to get --conf option
        parse_command_line([prog_name] + argv)
        try:
            parse_config_file(os.path.abspath(options.conf), final=False)
            parse_command_line([prog_name] + argv)
        except IOError:
            if os.path.basename(options.conf) != DEFAULT_CONFIG_FILE:
                raise

    def setup_logging(self):
        logger.debug(options.logging)
        if options.debug and options.logging == 'info':
            options.logging = 'debug'
            enable_pretty_logging()
        else:
            logging.getLogger("tornado.access").addHandler(NullHandler())
            logging.getLogger("tornado.access").propagate = False

    def extract_settings(self):
        settings['debug'] = options.debug

        if options.cookie_secret:
            settings['cookie_secret'] = options.cookie_secret

        if options.url_prefix:
            for name in ['login_url', 'static_url_prefix']:
                settings[name] = prepend_url(settings[name], options.url_prefix)

        if options.auth:
            settings['oauth'] = {
                'key': options.oauth2_key or os.environ.get('FLOWER_OAUTH2_KEY'),
                'secret': options.oauth2_secret or os.environ.get('FLOWER_OAUTH2_SECRET'),
                'redirect_uri': options.oauth2_redirect_uri or os.environ.get('FLOWER_AUTH2_REDIRECT_URI'),
            }

        if options.certfile and options.keyfile:
            settings['ssl_options'] = dict(certfile=abs_path(options.certfile),
                                           keyfile=abs_path(options.keyfile))
            if options.ca_certs:
                settings['ssl_options']['ca_certs'] = abs_path(options.ca_certs)

    def early_version(self, argv):
        if '--version' in argv:
            print(__version__, file=self.stdout)
            super(FlowerCommand, self).early_version(argv)

    @staticmethod
    def is_flower_option(arg):
        name, _, value = arg.lstrip('-').partition("=")
        name = name.replace('-', '_')
        return hasattr(options, name)

    def is_flower_envvar(self, name):
        return name.startswith(self.ENV_VAR_PREFIX) and\
               name[len(self.ENV_VAR_PREFIX):].lower() in default_options

    def print_banner(self, ssl):
        if not options.unix_socket:
            logger.info(
                "Visit me at http%s://%s:%s", 's' if ssl else '',
                options.address or 'localhost', options.port
            )
        else:
            logger.info("Visit me via unix socket file: %s" % options.unix_socket)

        logger.info('Broker: %s', self.app.connection().as_uri())
        logger.info(
            'Registered tasks: \n%s',
            pformat(sorted(self.app.tasks.keys()))
        )
        logger.debug('Settings: %s', pformat(settings))


class MultiFlowerCommand(FlowerCommand):
    
    multi_celery_app = True
    
    _celery_apps = []
    
    # 测试数据
    broker_urls = ['redis://127.0.0.1:6379/8', 'redis://127.0.0.1:6379/3']
    apps = [('envylan', 'C:\\Users\\Envylan\\PycharmProjects\\DjangoTest'), ('envylan2', 'C:\\Users\\Envylan\\PycharmProjects\\DjangoTest')]
    
    def __init__(self, *args, **kwargs):
        super(MultiFlowerCommand, self).__init__(get_app=self._get_app, *args, **kwargs)

    def run_from_argv(self, prog_name, argv=None, **_kwargs):
        self.apply_env_options()
        self.apply_options(prog_name, argv)
    
        self.extract_settings()
        self.setup_logging()
        
        for _app in self._celery_apps:
            # 这里如果不同的app使用不同的loader就会有问题，但一般不会有这样的情况
            print(_app._conf.broker_url)
            _loader = _app.loader
            temp = _loader.import_default_modules()
        flower = Flower(capp=self._celery_apps[0], options=options, **settings)
        # flower2 = Flower(capp=self._celery_apps[1], options=options, **settings)
        # flower程序退出时的注册函数
        atexit.register(flower.stop)
    
        def sigterm_handler(signal, frame):
            logger.info('SIGTERM detected, shutting down')
            sys.exit(0)
    
        signal.signal(signal.SIGTERM, sigterm_handler)
    
        self.print_banner('ssl_options' in settings)
    
        try:
            flower.start()
        except (KeyboardInterrupt, SystemExit):
            pass
    
    def execute_from_commandline(self, argv=None):
        """Execute application from command-line.
        Arguments:
            argv (List[str]): The list of command-line arguments.
                Defaults to ``sys.argv``.
        重新 flower命令初始化方法，这里要setup 多个 celery app，
        根据配置文件来 多次调用 setup_app_from_commandline
        """
        if argv is None:
            argv = list(sys.argv)
        # Should we load any special concurrency environment?
        self.maybe_patch_concurrency(argv)
        self.on_concurrency_setup()
    
        # Dump version and exit if '--version' arg set.
        self.early_version(argv)
        argv = self.setup_app_from_commandline(argv)
        self.prog_name = os.path.basename(argv[0])
        return self.handle_argv(self.prog_name, argv[1:])

    # 这个是初始化celery app的函数
    def setup_app_from_commandline(self, argv):
        preload_options = self.parse_preload_options(argv)
        quiet = preload_options.get('quiet')
        if quiet is not None:
            self.quiet = quiet
        try:
            self.no_color = preload_options['no_color']
        except KeyError:
            pass
        workdir = preload_options.get('workdir')
        if workdir:
            os.chdir(workdir)
        app = (preload_options.get('app') or
               os.environ.get('CELERY_APP') or
               self.app)
        preload_loader = preload_options.get('loader')
        if preload_loader:
            # Default app takes loader from this env (Issue #1066).
            os.environ['CELERY_LOADER'] = preload_loader
        loader = (preload_loader,
                  os.environ.get('CELERY_LOADER') or
                  'default')
        broker = preload_options.get('broker', None)
        if broker:
            pass
            #os.environ['CELERY_BROKER_URL'] = broker
        config = preload_options.get('config')
        if config:
            os.environ['CELERY_CONFIG_MODULE'] = config
        if self.respects_app_option:
            # app 是通过命令行 -A  proj 传入的字符串， find_app会通过 pythonpath寻找proj的module并初始化
            # 这里还有另外一种初始化celery app的方式，通过传入的broker url也能解析到celery的配置
            # 所以我这里判断，其实flower并没有通过引入proj的东西，只是通过broker url来获取相关配置信息
            if app:
                for _a in self.apps:
                    os.chdir(_a[1])
                    self._celery_apps.append(self.find_app(_a[0]))
                self.app = self._celery_apps[0]
            elif self.app is None:
                self.app = self.get_app(loader=loader)
            if self.enable_config_from_cmdline:
                argv = self.process_cmdline_config(argv)
        else:
            from celery import Celery
            self.app = Celery(fixups=[])
    
        self._handle_user_preload_options(argv)
    
        return argv
    
    # 先从 celery._state._get_current_app  copy过来
    def _get_app(self, *args, **kwargs):
        from celery.app.base import Celery
        return Celery(
            'default', fixups=[], set_as_current=False,
            loader=os.environ.get('CELERY_LOADER') or 'default',
        )