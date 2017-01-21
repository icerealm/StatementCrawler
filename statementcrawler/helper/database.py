from statementcrawler.helper.config import get_configuration
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


def get_credential():
    config = get_configuration()
    dbuser = config.get("database", "user")
    dbpassword = config.get("database", "password")
    auth_provider = PlainTextAuthProvider(username=dbuser, password=dbpassword)
    return auth_provider

db_session = None
cluster = None

def init_db_session():
    global cluster, db_session

    config = get_configuration()
    node_ids = [config.get("database", "url")]
    db_port = int(config.get("database", "port"))
    cluster = Cluster(contact_points=node_ids, port=db_port)
    db_session = cluster.connect(config.get("database", "keyspace"))

def get_db_session():
    return db_session

def shutdown_all_db_session():
    cluster.shutdown()
