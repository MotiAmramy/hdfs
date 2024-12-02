from hdfs import InsecureClient

from app.settings.client import HDFS_NAMENODE_URL, HDFS_USER


def create_client():
    return InsecureClient(HDFS_NAMENODE_URL, user=HDFS_USER)


hdfs_client = create_client()