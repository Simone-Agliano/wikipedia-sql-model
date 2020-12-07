import re
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class Redirect(Base):
    __tablename__ = 'redirect'
    redFrom = Column('red_from', Integer, nullable=False, primary_key=True)
    redTitle = Column('red_title', String(5), nullable=False)
    version = Column('version', Integer, nullable=True,
                     primary_key=True)  # double foreign keys because I need to add the same red_from for two different versions
    pages = relationship('Page', cascade="all, delete", passive_deletes=True)

    def __repr__(self):
        return "<Redirect(red_from='%s', red_title='%s', version='%s')>" % (
            self.redFrom, self.redTitle, self.version)


class Page(Base):
    __tablename__ = 'page'
    pageId = Column('page_id', Integer, primary_key=True)
    redId = Column('red_id', Integer, ForeignKey(Redirect.redFrom), nullable=True)
    pageTitle = Column('page_title', String(5), nullable=False)
    redTitle = Column('red_title', String(5), nullable=False)
    version = Column('version', Integer, nullable=True, primary_key=True)

    def __repr__(self):
        return "<Page(page_id='%s', red_id='%s', page_title='%s', redTitle='%s',version='%sì)>" % (
            self.pageId, self.redId, self.pageTitle, self.redTitle, self.version)


def process_status_sql(filename, session):
    with open(filename, 'r') as f:
        max_version = session.query(func.max(Page.version)).scalar()
        session.commit()
        for line in f:
            if line.startswith('INSERT INTO `page` VALUES'):
                entries = line[26:].split('),(')
                for entry in entries:
                    fields = entry.strip('(').strip(')').split(',')
                    if fields[4] == '1' and (len(fields[2]) <= 5) and fields[1] == '0':  # the page is redirect with namespaces =0
                        if max_version is not None:
                            print("max version not none")
                            current_page = session.query(Page).filter_by(redId=fields[0], version=max_version).first()
                            if current_page is None:  # there is no page in the table,maybe is a new one
                                our_root = session.query(Redirect).filter_by(redFrom=fields[0],
                                                                             version=max_version + 1).first()  # with the last version, since we update firstly the redirected it should be +1
                                if our_root is not None:
                                    p = Page(pageId=int(fields[0]), redId=our_root.redFrom, redTitle=our_root.redTitle,
                                             pageTitle=fields[2], version=our_root.version)
                                    session.add(p)
                            else:  # there is already a page, I need to update its version

                                our_root = session.query(Redirect).filter_by(redFrom=fields[0],
                                                                             version=current_page.version + 1).first()  # with the last version, since we update firstly the redirected it should be +1
                                if our_root is not None:
                                    p = Page(pageId=int(fields[0]), redId=our_root.redFrom, redTitle=our_root.redTitle,
                                             pageTitle=fields[2], version=our_root.version)
                                    session.add(p)

                        else:  # no max version then there is no value, it's empty
                            our_root = session.query(Redirect).filter_by(redFrom=fields[0],
                                                                         version=0).first()  # with the version 0
                            if our_root is not None:
                                p = Page(pageId=int(fields[0]), redId=our_root.redFrom, redTitle=our_root.redTitle,
                                         pageTitle=fields[2], version=our_root.version)
                                session.add(p)
                session.commit()
                session.flush()
        f.close()


def process_redirected_sql(filename, session):
    with open(filename, 'r') as f:
        max_version = session.query(func.max(Redirect.version)).scalar()
        session.commit()

        for line in f:
            if line.startswith('INSERT INTO `redirect` VALUES'):
                entries = line[26:].split('),(')
                for entry in entries:
                    fields = entry.strip('(').strip(')').split(',')
                    if re.search(r"[a-z]", fields[0]) is None and re.search(r"[A-Z]", fields[0]) is None and len(
                            fields[2]) <= 5 and fields[1] == '0':  # the page is redirect with namespaces =0
                        if max_version is not None:
                            current_redirected_page = session.query(Redirect).filter_by(redFrom=fields[0],
                                                                                        version=max_version).first()  # checking if the redirected records is already in place
                            if not current_redirected_page:
                                r = Redirect(redFrom=int(fields[0]), redTitle=fields[2], version=0)
                                session.add(r)
                            else:
                                r = Redirect(redFrom=int(fields[0]), redTitle=fields[2], version=max_version + 1)
                                session.add(r)
                        else:  # is the title of the root page changed? Every change or no change is recorded
                            r = Redirect(redFrom=int(fields[0]), redTitle=fields[2], version=0)
                            session.add(r)
            session.commit()
            session.flush()
        f.close()


def create_myengine(arguments):
    engine = create_engine(
        "mysql+pymysql://" + arguments.user + ":" + arguments.password + "@localhost/" + args.dbname)
    Session = sessionmaker(bind=engine)
    session = Session()
    if not engine.dialect.has_table(engine, 'page') and not engine.dialect.has_table(engine,
                                                                                     'redirect'):  # If table don't exist, Create.
        Base.metadata.create_all(engine)
    return engine, session


def number_of_redirected_pages(session):
    max_version = session.query(func.max(Page.version)).scalar()
    session.commit()
    current_redirects = session.query(Page).filter(Page.version == max_version).count()
    session.commit()
    return current_redirects


def number_of_root_pages(session):
    max_version = session.query(func.max(Redirect.version)).scalar()
    session.commit()
    current_redirects = session.query(Redirect).filter(Redirect.version == max_version).count()
    session.commit()
    return current_redirects


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Reading database model')
    parser.add_argument('--pageSql', metavar='pageSql', required=True,
                        help='path to sql model  as input')
    parser.add_argument('--redirectedSql', metavar='redirectedSql', required=False,
                        help='path to sql model as output')
    parser.add_argument('--user', metavar='user', required=True,
                        help='DB user')
    parser.add_argument('--password', metavar='password', required=True,
                        help='DB password')
    parser.add_argument('--dbname', metavar='dbname', required=True,
                        help='DB name')
    args = parser.parse_args()
    # create engine
    engine, session = create_myengine(args)
    # process redirected SQL dump file
    process_redirected_sql(args.redirectedSql, session)
    # process page SQL dump file
    process_status_sql(args.pageSql, session)
    # retrieve number of redirected pages
    print("number of redirects last dump" + str(number_of_redirected_pages(session)))
    # retrieve number of root pages
    print("number of root pages last dump" + str(number_of_root_pages(session)))
    session.close()
