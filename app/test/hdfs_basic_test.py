import pytest

from app.aggregation.map_reduce import map_reduce
from app.db.database import hdfs_client


DATA_DIR = '/data'

@pytest.fixture(scope="module")
def init_db():
    hdfs_client.delete(hdfs_path=DATA_DIR, recursive=True)
    yield


@pytest.fixture(scope="module")
def create_root_file(init_db):
    hdfs_client.makedirs(DATA_DIR)
    yield



def test_map_reduce(create_root_file):
    with open("../data/u.data", "r") as file:
        data = [line for line in file.readlines()][:20]
        print("\n    ".join(map_reduce(data)))
#
# def test_upload(create_root_file):
#     hdfs_upload_file(f'{DATA_DIR}/hello.txt','../data/hello.txt')
#
#
# def test_download(create_root_file):
#     hdfs_download_file(f'{DATA_DIR}/hello.txt','../data/hellooo.txt')
#





# def test_delete_file(init_db):
#     hdfs_delete_file('/mty')
#

# def test_hdfs_list(init_db):
#     res = hdfs_list('/')
#     print(res)
# def test_create_dir(init_db):
#     folder_name = "mty"
#     hdfs_makedir(f"/{folder_name}")
#     assert folder_name in hdfs_list("/")