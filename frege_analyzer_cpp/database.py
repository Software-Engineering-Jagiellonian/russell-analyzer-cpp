import time

from sqlalchemy import TIMESTAMP, Boolean, exc
from sqlalchemy import create_engine, Column, Integer, String, Sequence, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from frege_analyzer_cpp import config
from frege_analyzer_cpp.database_connection_parameters import DatabaseConnectionParameters
from frege_analyzer_cpp.logger import logger

Base = declarative_base()


class RepositoriesTable(Base):
    __tablename__ = "repositories"
    repo_id = Column(String, primary_key=True)
    git_url = Column(String)
    repo_url = Column(String)
    crawl_time = Column(TIMESTAMP(timezone=True))
    download_time = Column(TIMESTAMP(timezone=True))
    commit_hash = Column(String)


class RepositoryLanguageTable(Base):
    __tablename__ = "repository_language"
    id = Column(Integer, Sequence('repository_language_id_seq'), primary_key=True)
    repository_id = Column(String, ForeignKey('repositories.repo_id'))
    language_id = Column(Integer)
    present = Column(Boolean)
    analyzed = Column(Boolean)


class RepositoryLanguageFileTable(Base):
    __tablename__ = "repository_language_file"
    id = Column(Integer, Sequence('repository_language_file_id_seq'), primary_key=True)
    repository_language_id = Column(String, ForeignKey('repository_language.id'))
    file_path = Column(String)


class Languages(Base):
    __tablename__ = "languages"
    id = Column(Integer, Sequence('language_id_seq'), primary_key=True)
    name = Column(String)


class CppFile(Base):
    __tablename__ = 'cppfile'
    id = Column(Integer, Sequence('cppfile_id_seq'), primary_key=True)
    file_id = Column(Integer, ForeignKey('repository_language_file.id'))
    lines_of_code = Column(Integer)
    token_count = Column(Integer)
    average_lines_of_code = Column(Integer)
    average_token_count = Column(Integer)
    average_cyclomatic_complexity = Column(Integer)
    average_parameter_count = Column(Integer)
    average_nesting_depth = Column(Integer)
    max_nesting_depth = Column(Integer)


class Database:
    def __init__(self, database_parameters: DatabaseConnectionParameters):
        self.database_parameters = database_parameters
        self.engine = create_engine(f'postgresql://{self.database_parameters.username}:'
                                    f'{self.database_parameters.password}@{self.database_parameters.host}:'
                                    f'{self.database_parameters.port}/{self.database_parameters.database}')
        self.connection = None

    def connect(self):
        while True:
            try:
                logger.info('Connecting to a database')
                self.connection = self.engine.connect()
                self.Session = sessionmaker(bind=self.connection)

                CppFile.__table__.create(bind=self.connection, checkfirst=True)

                session = self.Session()
                session.commit()
                break
            except exc.DBAPIError as exception:
                logger.error(f'Database connection error: {exception}, '
                             f'sleeping for {config.DATABASE_CONNECTION_DELAY} seconds')
                time.sleep(config.DATABASE_CONNECTION_DELAY)
                if not exception.connection_invalidated:
                    raise exception
        logger.info('Connected to a database')

    def save_results(self, repo_id, results):
        session = self.Session()
        for file_id, stats in results.items():
            python_file = CppFile(file_id=file_id, **stats.as_dict())
            session.add(python_file)

        repository_language = session.query(RepositoryLanguageTable) \
            .filter(RepositoryLanguageTable.repository_id == repo_id).first()
        repository_language.analyzed = True

        session.flush()
        session.commit()

    def get_file_paths(self, repo_id):
        session = self.Session()
        file_paths = session.query(RepositoryLanguageFileTable.id, RepositoryLanguageFileTable.file_path) \
            .join(RepositoryLanguageTable) \
            .filter(RepositoryLanguageTable.repository_id == repo_id) \
            .filter(RepositoryLanguageTable.present.is_(True)) \
            .filter(RepositoryLanguageTable.analyzed.is_(False)) \
            .all()
        session.commit()
        return file_paths
