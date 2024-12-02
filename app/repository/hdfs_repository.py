from app.db.database import hdfs_client
import toolz as t





def hdfs_list(path: str):
    return hdfs_client.list(path)

def hdfs_makedir(folder_name: str):
    hdfs_client.makedirs(folder_name)

@t.curry
def hdfs_upload_file(hdfs_path: str, local_hdfs: str):
    hdfs_client.upload(hdfs_path, local_hdfs)

@t.curry
def hdfs_download_file(hdfs_path: str, local_path: str):
    hdfs_client.download(hdfs_path, local_path)

def hdfs_delete_file(hdfs_path: str):
    hdfs_client.delete(hdfs_path,recursive=True)



@t.curry
def write_path(output_path: str, output_content: str):
    with hdfs_client.write(output_path) as writer:
        writer.write(output_content.encode('utf-8'))