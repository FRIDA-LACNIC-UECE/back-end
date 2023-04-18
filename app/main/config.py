import os

# uncomment the line below for mysql database url from environment variable
# mysql_local_base = os.environ['DATABASE_URL']

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "my_precious_secret_key")
    DEBUG = False
    JWT_EXP = 720
    ACTIVATION_EXP_SECONDS = 86400

    # Remove additional message on 404 responses
    RESTX_ERROR_404_HELP = False

    # Swagger
    RESTX_MASK_SWAGGER = False

    # Database Frida Informations
    TYPE_DATABASE = "mysql"
    USER_DATABASE = "root"
    PASSWORD_DATABASE = "trustno1"
    HOST = "localhost"
    PORT = 3306

    # Pagination
    CONTENT_PER_PAGE = [10, 20, 30, 50, 100]
    DEFAULT_CONTENT_PER_PAGE = CONTENT_PER_PAGE[0]


class DevelopmentConfig(Config):
    # uncomment the line below to use mysql
    # SQLALCHEMY_DATABASE_URI = mysql_local_base
    mysql_local_base = "mysql://root:trustno1@localhost/frida"
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = mysql_local_base
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    ENV = "development"
    HOST = "localhost"

    # uncomment the line below to see SQLALCHEMY queries
    # SQLALCHEMY_ECHO = True


class StagingConfig(Config):
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = "mysql://root:@db/frida"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    ENV = "staging"
    HOST = "0.0.0.0"


class TestingConfig(Config):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = "sqlite:///" + os.path.join(
        basedir, "flask_boilerplate_test.db"
    )
    PRESERVE_CONTEXT_ON_EXCEPTION = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    ENV = "testing"


class ProductionConfig(Config):
    DEBUG = False
    # uncomment the line below to use postgres
    # SQLALCHEMY_DATABASE_URI = postgres_local_base
    ENV = "production"


config_by_name = dict(
    dev=DevelopmentConfig,
    test=TestingConfig,
    prod=ProductionConfig,
    staging=StagingConfig,
)

_env_name = os.environ.get("ENV_NAME")
_env_name = _env_name if _env_name is not None else "dev"
app_config = config_by_name[_env_name]
